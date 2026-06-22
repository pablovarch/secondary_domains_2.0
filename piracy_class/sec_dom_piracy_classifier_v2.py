import asyncio
import logging
import os
import re
from datetime import datetime
from dill import settings

from psycopg2 import pool
from pydantic import BaseModel
from openai import AsyncOpenAI, RateLimitError, APIConnectionError, APIStatusError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from settings import DB_CONNECTION
from dotenv import load_dotenv
from typing import Any, Literal
from settings import openia_apikey

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
logger.info('Loading script')
OPENAI_APIKEY = openia_apikey

# Configuration constants
MAX_HTML_CHARS = 120000  # ~5k tokens approx
MAX_CONCURRENT_REQUESTS = 5  # Semaphore limit for API calls
DB_POOL_MIN_CONN = 1
DB_POOL_MAX_CONN = 10

MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-5.4-2026-03-05")
REASONING_EFFORT = os.getenv("OPENAI_REASONING_EFFORT", "none")
DOMAIN_PROCESS_LIMIT = int(os.getenv("DOMAIN_PROCESS_LIMIT", "1000"))

SECONDARY_DOMAIN_TABLE = os.getenv("SECONDARY_DOMAIN_TABLE", "secondary_domains")
SECONDARY_DOMAIN_HTML_TABLE = os.getenv("SECONDARY_DOMAIN_HTML_TABLE", "secondary_domains_html")
DOMAIN_ID_COLUMN = os.getenv("DOMAIN_ID_COLUMN", "sec_domain_id")
MEDIA_TYPE_COLUMN = os.getenv("MEDIA_TYPE_COLUMN", "sec_domain_media_type_id")
ENFORCEMENT_LABEL_COLUMN = os.getenv("ENFORCEMENT_LABEL_COLUMN", "sec_domain_piracy_class_v2_id")

SEARCH_KEYWORDS_TABLE = os.getenv("SEARCH_KEYWORDS_TABLE", os.getenv("PIRACY_KEYWORDS_TABLE", "search_keywords"))
SEARCH_KEYWORDS_DEFAULT_COLUMN = "keyword" if os.getenv("PIRACY_KEYWORDS_TABLE") and not os.getenv(
    "SEARCH_KEYWORDS_TABLE") else "search_keyword"
SEARCH_KEYWORDS_COLUMN = os.getenv("SEARCH_KEYWORDS_COLUMN",
                                   os.getenv("PIRACY_KEYWORDS_COLUMN", SEARCH_KEYWORDS_DEFAULT_COLUMN))
SEARCH_KEYWORDS_BRAND_COLUMN = os.getenv("SEARCH_KEYWORDS_BRAND_COLUMN",
                                         os.getenv("PIRACY_KEYWORDS_BRAND_COLUMN", "brand"))

DOMAIN_SSL_TABLE = os.getenv("DOMAIN_SSL_TABLE", "domain_ssl_data")
DOMAIN_SSL_REQUESTED_DOMAIN_COLUMN = os.getenv("DOMAIN_SSL_REQUESTED_DOMAIN_COLUMN", "requested_domain")
SSL_SCORE_MODE = os.getenv("SSL_SCORE_MODE", "trust")

# Database connection pool (initialized lazily)
db_pool: pool.ThreadedConnectionPool | None = None


class EnforcementResponse(BaseModel):
    label_id: Literal[0, 1, 13, 12, 15, 16, 17, 18, 19]


ENFORCEMENT_CLASSIFICATION_PROMPT = '''You are a web domain enforcement classifier (second stage in a pipeline).

Stage 1 has already classified the domain into a MEDIA TYPE (e.g. Film & TV, Anime, Games, Software, Publishing, Music, Adult, Sports, News, Content Host, Gambling, Social Media, Other, etc.).

Your task in Stage 2 is to analyze:
- The MEDIA TYPE
- The raw HTML content of the site
- Whether the site is associated with a known piracy brand
- The SSL score (a numeric trust/safety signal)
and assign EXACTLY ONE enforcement label ID from the allowed set.

You MUST respond using the EnforcementResponse schema, setting the integer field `label_id` to ONE of these IDs:

0  = Exclude
1  = Enforce
13  = Content Host
12 = Illegal Pornography
15 = Social Media
16 = Stream Ripper
17 = Piracy Apps
18 = Forum Only
19 = IPTV Piracy

No other IDs are allowed. Do NOT include explanations or extra fields. Only set `label_id` correctly.

-------------------------
INPUT FIELDS (CONTEXT)
-------------------------

You will receive a user message with:
- media_type: the media type name (e.g. "Film & TV", "Anime", "Games", "Software", "Publishing", "Music", "Adult", "Sports", "News", "Content Host", "Gambling", "Other", etc.)
- html: the raw HTML of the site (may be truncated)
- piracy_brand_known: a boolean indicating whether this domain is associated with a known piracy brand (True/False)
- ssl_score: a numeric score where higher values indicate a more trustworthy / legitimate configuration, and lower values indicate more suspicious configuration. This score is only a weak hint: HTML evidence and the definitions below are more important.
- privacy_policy_detected: a boolean crawler signal indicating the site likely has a Privacy Policy page (True/False)
- terms_of_use_detected: a boolean crawler signal indicating the site likely has Terms/Terms of Use/Terms & Conditions (True/False)
- blocked_snapshot_detected: a boolean crawler signal indicating the captured HTML appears to be a bot-check, CAPTCHA, Cloudflare challenge, access-denied, forbidden, DDoS-protection, or other blocked/interstitial snapshot (True/False)

You must primarily rely on:
1) The HTML content and site behavior.
2) The media_type from Stage 1.
3) The piracy_brand_known flag (strong signal of piracy when True).
Use ssl_score only as a secondary signal in ambiguous cases, never as the sole reason for an enforcement decision.
Use privacy_policy_detected and terms_of_use_detected only as WEAK legitimacy hints: pirates can copy or fake these pages. Do NOT choose Exclude solely because these are True.
Use blocked_snapshot_detected as an incomplete-evidence signal, NOT as a legitimacy signal. Cloudflare, CAPTCHA, access denied, forbidden, and DDoS-protection pages are common on pirate sites. Do NOT choose Exclude solely because blocked_snapshot_detected is True. If the domain has a known piracy brand, high-risk media type, or residual piracy evidence in the snapshot, still classify according to the piracy evidence.

IMPORTANT: The HTML can be in ANY language (Spanish, Chinese, Korean, Japanese, Portuguese, Russian, Arabic, Turkish, etc.). Do NOT bias toward Exclude just because the site is not in English. Non-English pirate sites are extremely common (e.g. Spanish "ver online", Chinese 在线观看, Korean 보기, Japanese 無料視聴).

-------------------------
STRONG PIRACY SIGNALS (override generic legitimacy hints)
-------------------------

Treat ANY of the following as strong evidence of infringement. If one or more of these appear on a Film & TV / Anime / Manga / Sports / Adult / Games / Software / Music / Publishing site, DO NOT classify as Exclude just because the site looks polished, has a Privacy Policy, a footer with legal links, or social media icons.

1) FAKE SAFE-HARBOR / "BUY THE ORIGINAL" DISCLAIMER in footer / legal copy, such as:
   - "This site does not store any files on its servers"
   - "We do not host any videos/files"
   - "All content is provided by non-affiliated third parties"
   - "Este sitio no almacena ningún video / ningún archivo en sus servidores"
   - "Esse site não hospeda nenhum vídeo em seu servidor"
   - "Todo o conteúdo é disponibilizado por terceiros não afiliados"
   - "No alojamos ningún contenido, solo compartimos enlaces"
   - "本站不存储任何视频" / "저희는 파일을 저장하지 않습니다" and equivalents.
   - "This site does not store any files on our server, we only linked to the media which is hosted on 3rd party services" / "We only provide links / embeds to media hosted on other websites" / "All media is hosted by non-affiliated third parties" — the "we only link/embed" framing is the universal excuse of streaming aggregators.
   - "All the comics on this website are only previews of the original comics" / "For the original version, please buy the comic" / "Please support the official release" / "Buy the original from the official publisher" — these "buy-the-original" disclaimers are the de-facto fingerprint of scanlation / manga-piracy aggregators.
   - "漫画を無料で読む" (read manga for free) combined with no licensing — typical Japanese raw-manga piracy framing.
   These disclaimers are the classic pirate attempt at safe harbor; when combined with direct episode/chapter listings, reader UIs, or large catalogs of copyrighted titles they are a RED FLAG for ENFORCE, not evidence of legitimacy.

2) FILM & TV / ANIME STREAMING AGGREGATORS — TWO page types both qualify as ENFORCE:

   2a) CATALOG / HOME / GENRE / COUNTRY / TOP-IMDb / UPDATES pages:
   - Navigation mega-menu combining dozens of Genres + dozens of Countries + "Movies / TV-Series / Anime / Top IMDb / Updates / Trending / Latest". Official platforms (Netflix/Disney+/HBO/Crunchyroll/Prime Video) do NOT expose a filter-by-country-of-origin or a 35+ genre grid on their home page; this layout is a fingerprint of pirate aggregators.
   - Grid of posters with titles of known copyrighted works (HBO/Netflix/Disney/Marvel/Warner originals, popular movies/series) each with "HD/HDRip/CAM/TS/WEB-DL/BluRay" badge and "watch free/online" phrasing.
   - Franchises arranged by seasons/sagas/arcs/chapters/episodes with "Ver/Watch/Assistir/播放/보기/Смотреть онлайн" links and NO sign of being an official rights holder.

   2b) INDIVIDUAL WATCH / PLAY pages (this alone is sufficient, no need to actually see a video playing):
   - URL pattern /watch/<slug>, /film/<slug>, /movie/<slug>, /serie(s)/<slug>, /ver/<slug>, /episode/<slug>, /play/<slug>, /stream/<slug>.
   - <title> or meta description containing: "Watch Online FREE", "Watch <title> online free", "<title> Full Movie Online", "free streaming", "no registration required", "download to watch offline", "assistir online grátis", "ver online gratis", "在线观看", "무료보기", "無料視聴".
   - Player DOM signature: a player container (`#player`, `.player-main`, `.jw-player`, `.plyr`, iframe from unknown CDN like *.xyz / *.top / *.site / *.pro) together with a SERVER SELECTOR (`#movie-server`, `.server-list`, labels like "Server 1/2/3", "Vidstream/VidPlay/StreamSB/StreamWish/MixDrop/DoodStream/StreamTape/FileMoon/UpCloud") and an EPISODE LIST (`#movie-episodes`, `.eps-list`, `.seasons-list`).
   - Control bar layout with "Focus / Light / Auto Play / Auto Next / Prev / Next / Bookmark / Report" buttons — this exact layout is a shared fingerprint of pirate streaming CMS clones (FMovies/9Anime/HiAnime/GoGoAnime/SFlix/AZMovies/ZoroX/2Embed/Aniwatch templates).
   - "Report" modal with fixed checkboxes: "Video wrong", "Audio not synced", "Subtitle not synced", "Subtitle wrong", "Broken link", "Wrong episode" — the same template recycled across hundreds of pirate aggregators.
   - Metadata side panel listing `Productions: <HBO / Netflix / Disney / Warner Bros / Amazon / Paramount / A24 / Sony / Universal>` on a domain that is clearly NOT one of those rights holders → ENFORCE (the site is borrowing legitimacy signals, not operating the service).
   - "You may also like" / "Related" / "Similar" sliders filled with posters of other popular copyrighted titles from different rights holders — aggregator pattern, not an official single-franchise site.
   - Cross-links in footer or body to OTHER known pirate aggregators (e.g. hianime, fmovies, sflix, aniwatch, gogoanime, 9anime, primewire, soap2day, bflix, lookmovie, yesmovies, putlocker and their numerous clones) with or without utm_source attribution.

   Any of (2a) or (2b) → ENFORCE. A single WATCH page with the DOM signature in (2b) is sufficient evidence even without seeing the catalog.

3) MANGA / MANHWA / MANHUA piracy sites — TWO distinct page types both qualify as ENFORCE:

   3a) READER pages:
   - Server selector ("Server 1", "Server 2"), chapter dropdown, Prev/Next navigation.
   - Inline panels rendered on the page for free reading.
   - Title headers like "<Title> - Chapter N" with a gallery of panels below, or "Chapter: N" + "Start Reading" CTA.

   3b) CATALOG / INDEX / GENRE / RANKING pages (very common landing pages — DO NOT require seeing a reader UI):
   - Grid of manga/manhwa/manhua cover thumbnails with title, latest chapter number (e.g. "# 95", "Chapter 148") and view counter ("448,302 万", view eye icon).
   - "UP" / "NEW" / "HOT" badges on covers.
   - Navigation with: ホーム/Home, トレンド/Trending, ランキング/Ranking, 人気/Popular, ジャンル/Genre, Ecchi, アクション, ファンタジー, コメディ, Manhwa, Manhwa Hot, Webtoons, Smut, Mature, Adult, SM/BDSM, Loli, etc.
   - URL paths like /manga/<slug>/, /genre/<x>/, /ranking/, /trending/, /chapter-<n>/.
   - Titles ending in "Raw", "Raw Free", "Free", "無料", "제로" — strong manga-piracy marker.
   - WordPress theme "mangareader" / "madara" / "themesia mangareader" (visible in CSS/JS paths like /wp-content/themes/mangareader/) is the de facto pirate manga theme.

   3c) KNOWN MANGA-PIRACY BRAND TOKENS (any of these in domain, <title>, meta description, JSON-LD or footer is a strong piracy signal):
   manga1000, manga1001, mangaraw, mangaraw.ac, rawkuma, rawqq, klmanga, 13dl, mangakakalot, manganato, mangapark, mangahere, mangafox, kissmanga, mangabuddy, mangaowl, kingofshojo, asurascans (and clones), flamescans, reaperscans (clones), nitroscans, lhtranslation, niadd, 漫画 raw, 漫画タウン, 漫画bank, 漫画村.

   Any of (3a), (3b), or (3c) → ENFORCE. The presence of catalog/index alone (3b) is sufficient evidence even WITHOUT seeing a reader UI in the snapshot.

4) LIVE SPORTS LISTS with phrases like:
   - "en Vivo", "Live Now", "直播", "Partidos de hoy", "Watch live" next to league names (LaLiga, Premier League, Champions, NBA, NFL, Copa de Francia, Eredivisie, etc.)
   - Schedules of matches with direct stream/play links.
   → IPTV Piracy (19) if IPTV branding, else ENFORCE (1). A legitimate news site only shows scores/results, not embedded live streams or "ver en vivo" links.

5) INTRUSIVE MONETIZATION typical of piracy (combined with other piracy signals, this is confirmatory):
   - Forced popup/modal on load with "Close" buttons that open third-party redirect URLs.
   - Adult banner ads on a non-adult (e.g. anime, manga, sports) site.
   - Multiple redirect / affiliate domains, "smart link" shorteners, traffic monetizer scripts.
   - Sticky floating video players or iframes not related to the stated topic.

6) OFFICIAL-LOOKING BRANDING is NOT by itself proof of legitimacy. Only trust it when:
   - The domain matches or is clearly operated by a known rights holder / official licensee, AND
   - There is no direct free access to full copyrighted episodes/chapters/matches.
   Examples of genuinely legitimate platforms: Crunchyroll, Netflix, Disney+, MangaPlaza (NTT Solmare), Shueisha's MANGA Plus, ViX, DAZN, Paramount+, Apple TV+, Spotify, Steam, Epic Games, Amazon, Kindle, Google Books, Audible.

If piracy_brand_known is True AND any of signals 1-5 are present → strongly prefer ENFORCE (or a more specific piracy label), never Exclude.

-------------------------
LABEL DEFINITIONS
-------------------------

You must choose ONE of the following labels (IDs):

ID 0 - Exclude
ID 1 - Enforce
ID 13 - Content Host
ID 12 - Illegal Pornography
ID 15 - Social Media
ID 16 - Stream Ripper
ID 17 - Piracy Apps
ID 18 - Forum Only
ID 19 - IPTV Piracy

Below are detailed definitions and rules.


### ID 1 - Enforce (Enforceable Domain)

Use ID 1 when the domain is an ENFORCEABLE pirate site:

General definition:
- Offers well-known commercial content (movies, series, anime, games, software, music, Publishing, sports, etc.) WITHOUT a license.
AND
- Content is published by the site owner
OR
- The site publishes user content that is unmoderated such that infringing content is easily discovered (e.g. unmoderated forums where illegal content is clearly visible and promoted).

Enforceable special cases:
- Sites that pretend to be safe or compliant but clearly publish infringing content (e.g. token or fake DMCA contact info in the footer, or a fake “report” link that does nothing).
- Sites where infringing content is the main value for users, even if they claim safe harbor.

Per media type guidance (when not covered by more specific labels like IPTV Piracy, Piracy Apps, Stream Ripper, Illegal Pornography):

Film & TV / Anime / Manga:
- ENFORCE if the site allows the user to WATCH or DOWNLOAD full movies, series, anime episodes, or similar content without license.
- Concrete ENFORCE patterns (any of these is sufficient):
  * Navigation menus grouping a franchise by seasons/sagas/arcs/chapters with episode-level pages (e.g. "Dragon Ball Z > Saga Saiyajin > Capítulo N", "Naruto Shippuden > Capítulo N").
  * Title "Ver <franchise> online" / "Watch <title> online free" / "<title> 在线观看" / "<title> 무료보기" in <title> or <h1>.
  * Post/article pages that are thin wrappers around an embedded video player or external streaming iframe for a full episode / full movie.
  * Manga / manhwa / manhua reader UI: chapter/episode dropdown + "Server 1/Server 2" + Prev/Next + inline image panels. This alone is ENFORCE even if the landing page looks like a blog.
  * A footer "safe harbor" disclaimer claiming the site does not host files, combined with clear streaming/download UX (see STRONG PIRACY SIGNALS #1). Do NOT treat the disclaimer as legitimacy.
  * Catalog/home pages that display rows of popular anime/manga/movie covers with titles of known copyrighted works, and clicking them leads to free full playback/reading. WordPress or custom themes branded as "<name>.com – watch/read online" are a very common pattern.
- EXCLUDE (ID 0) ONLY if the site is clearly a review/news/wiki/trailer site with NO full episodes, NO full chapters, NO live streams, and NO direct download links — typical examples: MyAnimeList, IMDb, Rotten Tomatoes, AniList, Letterboxd, Wikipedia articles, Fandom wikis, official publisher blogs.
- Do NOT mark as Exclude based on the mere presence of Privacy Policy, Terms, Facebook/Twitter icons, comment widgets, or a "DMCA" link in the footer — pirate streaming sites routinely include all of these.

Games:
- ENFORCE if the site offers pirated game downloads or hacks/cheats that give unfair advantages (aimbots, cracks, keygens, loaders, etc.).
- EXCLUDE (ID 0) if it is only reviews, news, guides or simple mods that do not clearly facilitate piracy.

Software:
- ENFORCE if the site offers commercial (paid) software for free, or cracked versions, beyond any legitimate trial period.
- EXCLUDE (ID 0) if it is only reviews, product information, tutorials, or links to official stores.

Publishing:
- ENFORCE if the site offers commercial Publishing (non-free titles) for free download without authorization.
- EXCLUDE (ID 0) if it offers free/public-domain Publishing, or clearly authorized/legitimate downloads.

Music:
- ENFORCE if the site offers direct downloads of copyrighted music content (songs, full albums) without authorization.
- EXCLUDE (ID 0) if it only provides reviews, news, or embeds from YouTube/Spotify without a clear download function (e.g. only iframes).

Sports:
- ENFORCE if the site illegally streams live sports matches or premium sports content without license.
- Concrete ENFORCE patterns:
  * Schedule/table of today's matches ("Partidos de hoy", "Hoy TV", "Live Matches", "直播") with team-vs-team links labeled "en Vivo" / "Live" / "Watch".
  * Brands or clones like rojadirecta, pirlotv, futbollibre, libertv, 6stream, buffstreams, crackstreams, etc. — typical free streaming of LaLiga, Premier League, Champions, NBA, NFL, UFC, F1.
  * Embedded players or iframes pointing to unknown CDNs for premium leagues without a licensing logo (DAZN, ESPN, Sky, beIN, etc.) being the operator.
- If the site sells/advertises IPTV subscriptions, M3U lists, channel bouquets, "10k channels", TV box panels → IPTV Piracy (ID 19).
- EXCLUDE (ID 0) if it is only sports news, scores, commentary, fantasy, statistics, or transfer rumors with no embedded live streams and no "watch live" links.

Adult:
- ENFORCE if it is an adult content site that primarily publishes infringing commercial content (but not involving minors or animals).
- Use ILLEGAL PORNOGRAPHY (ID 12) if minors or bestiality are involved (see below).

News:
- News media type should generally be EXCLUDE (ID 0). Only mark ENFORCE if the site is clearly using “news” as a façade but the main value is direct infringing content (HTML must show this clearly).

Content Host media type:
- If it is a generic file hosting/search platform, and it acts as a passive tech platform with takedown processes and no obvious search/indexing of infringing content, use Content Host (ID 13) or Exclude (ID 0).
- If it clearly promotes and exposes infringing content prominently, use ENFORCE (ID 1), or a more specific label like Stream Ripper / Piracy Apps / IPTV Piracy, if applicable.

Gambling:
- Gambling and betting sites are normally EXCLUDE (ID 0) unless they are clearly used as fronts for piracy (rare).


### ID 0 - Exclude

Use ID 0 when the site should NOT be enforced for copyright infringement:

- There is no infringing content, OR
- The site is clearly protected under safe harbor as a passive tech platform with proper notice-and-takedown and does not obviously promote piracy.

Legitimacy / lawful commerce signals (strong evidence for EXCLUDE when there is no clear piracy evidence):
- Clear CONTACT information: business email, phone number, contact form, physical address, customer support, help center.
- Clear legal/compliance pages in navigation or footer:
  - Terms / Terms of Service / Terms & Conditions / T&C
  - Privacy Policy.
  - Cookie Policy.
  - Refund / Returns policy, billing, subscription management, cancellation.
  - Company/about pages (About, Who we are, Our team) with consistent branding.
- Clear links to official social media profiles (e.g. Facebook/Instagram/Twitter/X/YouTube/TikTok/LinkedIn) that look like real brand accounts.
- Clear indication of legitimate commerce/operations: pricing pages, checkout tied to a known payment processor, app store links, official store links, publisher/rights-holder messaging.

Important caveats:
- Pirates can copy footers/legal pages. These signals are strong only when they are consistent with the overall site and there is no explicit piracy functionality.
- Do NOT choose Exclude solely because you see a "DMCA" link or a generic takedown email if the site otherwise clearly distributes infringing content.
- If piracy_brand_known is True, do NOT choose Exclude based only on legitimacy signals; require strong evidence the site is legitimate and not offering piracy.

Examples:
- News sites (when genuinely focused on journalism/information).
- Film, game, music, or book review sites that do NOT provide full infringing content or direct pirate links.
- Gaming communities with no game downloads or cracks.
- Book review sites with no book downloads.
- Online Service Providers with safe harbor and clear notice-and-takedown processes.
- Social media or UGC platforms with content moderation and reporting tools (but see Social Media label below).
- Commercial ISPs and hosting services.
- Commercial file or stream hosting with clearly non-infringing usage and no obvious pirate search/index.
- Search engines (Google, Bing, DuckDuckGo, Naver, Yahoo, etc.).

If the site is clearly a bona fide Social Media / UGC platform (Facebook, Twitter/X, TikTok, Instagram, etc.), use the SOCIAL MEDIA label (ID 15), not generic Exclude.


### ID 18 - Forum Only

Use ID 18 only for forums that meet these criteria:

- Forum is private or requires registration, and you CANNOT verify that it has infringing content affecting customers.
OR
- Infringing content is NOT promoted or encouraged in the main areas:
  - Not in the home page,
  - Not in the site name/byline,
  - Not in social media posts by the operators.
AND
- The forum is the core function of the site, and the purpose is not to contribute infringing content (e.g., a generic tech forum, hobby forum, etc.).
- The landing page offers forum groups, sign-up, etc. for a non-piracy purpose.
- The forum is not a side feature next to clearly infringing content.

If a forum openly promotes illegal downloads or infringing content, it is ENFORCE (ID 1), NOT Forum Only.


### ID 19 - IPTV Piracy

Use ID 19 for illegal IPTV sites:

- They offer unauthorized access to premium TV channels, movies, or sports events.
- Typically at significantly lower prices than legitimate providers, or for free.
- Often advertise channel lists, sports packages, movies “all in one”, hacked IPTV panels, etc.
- No clear licensing or affiliation with official providers.

If the main product is IPTV piracy, use ID 19 rather than generic ENFORCE (ID 1).


### ID 17 - Piracy Apps

Use ID 17 when the site offers dedicated PIRACY APPS for download:

- Software/apps specifically designed to access pirated movies, series, IPTV, or other media.
- Often require installation on PC, Android, iOS, TV boxes, etc.
- Example: apps similar to magictv that clearly facilitate piracy.
- Distinct from IPTV Piracy (which is usually web-based IPTV service) and from generic software.

Only use ID 17 when the main product is a piracy app. Otherwise, use IPTV Piracy, Stream Ripper, or ENFORCE as appropriate.


### ID 16 - Stream Ripper

Use ID 16 for illegal stream-ripping sites or apps:

- Enable users to download copyrighted audio or video from streaming platforms without authorization.
- Target ad-supported or subscription streaming services (YouTube, Spotify, etc.).
- Must clearly name the targeted service(s).
- Must advertise that they convert, download, or save streaming content for offline use.
- Purpose is to consume the unmodified media content (not editing or transforming it).
- Not just an editor, converter, or cover art downloader; it is clearly focused on grabbing protected streams.

If the site is mainly a stream-ripping tool, use ID 16 rather than generic ENFORCE (ID 1).


### ID 12 - Illegal Pornography

Use ID 12 for ILLEGAL pornography involving minors or animals:

- If the main page presents, promotes, or clearly focuses on pornographic content involving minors or bestiality, assign ID 12.
- This overrides other labels: if illegal pornography is present, use ID 12 even if other infringing content exists.
- Adult content that does NOT involve minors/animals and is infringing should be ENFORCE (ID 1) with Adult media type, not ID 12.


### ID 15 - Social Media

Use ID 15 for mainstream SOCIAL MEDIA or UGC platforms:

- Core function is user-generated content and social networking.
- Examples: Facebook, Instagram, TikTok, Twitter/X, Snapchat, large UGC video platforms with moderation.
- The platform provides moderation tools, reporting, and safe harbor behavior.
- The landing page and branding indicate a general social network or UGC service, not a pirate brand.

Even if some users may upload infringing content, large social platforms with moderation are treated as Social Media (ID 15) rather than Enforce.

Do NOT use Social Media (ID 15) for small pirate streaming sites that just have comments; those should be ENFORCE (ID 1) or other piracy labels.


### ID 13 - Content Host

Use ID 13 when the site is primarily a CONTENT HOST or storage provider:

- Offers file storage or streaming as a service.
- May require login or account.
- Marketing emphasizes hosting, cloud storage, or generic file sharing, not specific infringing content.
- Does NOT prominently promote or index infringing content on the public pages.

Examples:
- Generic cloud storage platforms.
- File hosting where users can upload content, with some form of moderation or abuse reporting.

If the host clearly promotes infringing content (e.g. “Download latest movies here” with direct links), it is ENFORCE (ID 1) or a more specific piracy label.


-------------------------
PRIORITY & CONFLICT RULES
-------------------------

When deciding, follow this priority order:

1) If the site clearly contains ILLEGAL pornography involving minors or animals -> label_id = 12 (Illegal Pornography).
2) Else, if it is clearly IPTV piracy -> label_id = 19.
3) Else, if it is clearly a STREAM RIPPING site/app -> label_id = 16.
4) Else, if it is clearly a PIRACY APP distribution site -> label_id = 17.
5) Else, if it is clearly a mainstream SOCIAL MEDIA / UGC platform -> label_id = 15.
6) Else, if it is a pure FORUM meeting the "Forum Only" criteria (private/uncertain, non-piracy purpose) -> label_id = 18.
7) Else, if it is a CONTENT HOST/storage service -> label_id = 13 (unless obviously promoting piracy → then ENFORCE).
8) Else, decide between ENFORCE (ID 1) and EXCLUDE (ID 0) based on:
   - Presence of clearly infringing commercial content vs. only reviews/news.
   - Media type (Film & TV, Anime, Games, Software, Publishing, Music, Sports, Adult, etc.).
   - Piracy brand indicator: if piracy_brand_known is True, strongly favor ENFORCE or a specific piracy label.
   - SSL score: low scores slightly increase suspicion; high scores slightly favor Exclude, but HTML and behavior are more important.

Default bias for HIGH-RISK media types:
- When media_type is Film & TV, Anime, Manga, Sports, or Adult, and the HTML shows ANY of the STRONG PIRACY SIGNALS (fake safe-harbor disclaimer, episode/chapter/saga catalogs of known franchises, manga reader UI, live-match lists, intrusive redirect monetization), the correct answer is ENFORCE (or a specific piracy label), even if the site also has Privacy Policy, Terms, social media icons, a DMCA link, or a polished UI.
- Do NOT require an explicit "Download" button or a visible video player in the snapshot. Navigation structure, titles, headings and catalog listings are sufficient evidence.

Default bias for LOW-RISK media types:
- When media_type is News, Online Courses, Other, or when the site is clearly an official rights-holder / licensee / storefront (Netflix, Disney+, Crunchyroll, MangaPlaza, Steam, Apple TV+, DAZN, Spotify, Amazon, etc.), prefer Exclude unless there is clear infringing functionality.

If evidence is genuinely unclear AND media_type is not in the high-risk set AND none of the strong piracy signals are present, use Exclude (ID 0).


-------------------------
OUTPUT REQUIREMENTS
-------------------------

You MUST respond using the EnforcementResponse schema, with:
- label_id: ONE of {0, 1, 13, 12, 15, 16, 17, 18, 19}

Do NOT include explanations, text, or additional fields. Only set `label_id`.
'''


def invalid_html(text: str | None) -> bool:
    """Check if HTML content is too short to classify."""
    try:
        if not text:
            return True
        # word count < 4
        if len(text.split()) < 4:
            return True
        return False
    except Exception as e:
        logger.error(f"Error on invalid_html method: {e}")
        return True


def safe_identifier(name: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


def normalize_domain(value: str) -> str:
    s = (value or "").strip().lower()
    if not s:
        return ""
    s = re.sub(r"^[a-z][a-z0-9+.-]*://", "", s)
    s = s.split("/")[0]
    s = s.split("?")[0]
    s = s.split("#")[0]
    if ":" in s and not s.startswith("["):
        s = s.split(":")[0]
    s = s.strip(".")
    return s


def extract_semantic_content(html_content: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        'title': '',
        'meta_description': '',
        'meta_keywords': '',
        'headings': [],
    }

    title_match = re.search(r'<title[^>]*>([^<]+)</title>', html_content, re.IGNORECASE)
    if title_match:
        result['title'] = title_match.group(1).strip()

    meta_desc = re.search(
        r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\'>]+)["\']',
        html_content,
        re.IGNORECASE,
    )
    if not meta_desc:
        meta_desc = re.search(
            r'<meta[^>]*content=["\']([^"\'>]+)["\'][^>]*name=["\']description["\']',
            html_content,
            re.IGNORECASE,
        )
    if meta_desc:
        result['meta_description'] = meta_desc.group(1).strip()

    meta_kw = re.search(
        r'<meta[^>]*name=["\']keywords["\'][^>]*content=["\']([^"\'>]+)["\']',
        html_content,
        re.IGNORECASE,
    )
    if meta_kw:
        result['meta_keywords'] = meta_kw.group(1).strip()

    headings = re.findall(r'<h[1-3][^>]*>([^<]+)</h[1-3]>', html_content, re.IGNORECASE)
    result['headings'] = [h.strip() for h in headings[:10]]

    return result


def preprocess_html(html_content: str) -> str | None:
    if not html_content:
        return None

    semantic = extract_semantic_content(html_content)

    prefix_parts: list[str] = []
    if semantic['title']:
        prefix_parts.append(f"TITLE: {semantic['title']}")
    if semantic['meta_description']:
        prefix_parts.append(f"DESC: {semantic['meta_description']}")
    if semantic['meta_keywords']:
        prefix_parts.append(f"KEYWORDS: {semantic['meta_keywords']}")
    if semantic['headings']:
        prefix_parts.append(f"HEADINGS: {' | '.join(semantic['headings'])}")

    prefix = '\n'.join(prefix_parts)

    text = html_content
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'[\U00010000-\U0010ffff]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()

    if prefix:
        full_text = f"{prefix}\n\nCONTENT: {text}"
    else:
        full_text = text

    if len(full_text) > MAX_HTML_CHARS:
        full_text = full_text[:MAX_HTML_CHARS] + "... [TRUNCATED]"

    return full_text if full_text.strip() else None


def extract_html_from_mhtml(snapshot: str) -> str:
    if not snapshot:
        return ""

    s = snapshot

    m = re.search(r"Content-Type:\s*text/html", s, flags=re.IGNORECASE)
    if not m:
        return s

    start = s.find("<!DOCTYPE", m.start())
    if start < 0:
        start = s.find("<html", m.start())
    if start < 0:
        start = m.start()

    tail = s[start:]
    end_candidates: list[int] = []
    for pat in [
        r"\r?\n--[_A-Za-z0-9\-\.]+=",
        r"\r?\nContent-Type:\s*",
        r"\r?\nFrom:\s*",
    ]:
        mm = re.search(pat, tail, flags=re.IGNORECASE)
        if mm:
            end_candidates.append(mm.start())

    end = min(end_candidates) if end_candidates else len(tail)
    return tail[:end]


def remove_large_data_uris(text: str) -> str:
    if not text:
        return ""
    return re.sub(
        r"data:[^\s\"\']{1,200};base64,[A-Za-z0-9+/=\s]{256,}",
        "data:[base64]",
        text,
        flags=re.IGNORECASE,
    )


def should_fast_exclude(prepared_text: str, raw_html: str) -> bool:
    hay = (prepared_text or "").lower()
    raw_full = (raw_html or "")
    raw = raw_full[:200000].lower()

    parking_patterns = [
        r"sedo\s+domain\s+parking",
        r"sedoparking\.com",
        r"this\s+domain\s+has\s+recently\s+been\s+registered",
        r"domain\s+parking",
        r"buy\s+this\s+domain",
        r"domain\s+is\s+for\s+sale",
        r"for\s+sale\s+domain",
        r"parkingcrew",
        r"bodis",
        r"dan\.com",
        r"afternic",
    ]

    for pat in parking_patterns:
        if re.search(pat, hay) or re.search(pat, raw):
            return True

    return False


def detect_blocked_snapshot(prepared_text: str | None, raw_html: str | None) -> bool:
    hay = (prepared_text or "").lower()
    raw_full = (raw_html or "")
    raw = raw_full[:200000].lower()

    blocked_patterns = [
        r"captcha",
        r"verify\s+you\s+are\s+human",
        r"attention\s+required",
        r"checking\s+your\s+browser",
        r"cloudflare",
        r"cf-challenge",
        r"ddos\s+protection",
        r"access\s+denied",
        r"forbidden",
        r"error\s*403",
        r"temporarily\s+unavailable",
        r"unusual\s+traffic",
        r"ray\s+id",
    ]

    for pat in blocked_patterns:
        if re.search(pat, hay) or re.search(pat, raw):
            return True

    return False


def prepare_snapshot_for_llm(raw_snapshot: str) -> str | None:
    if not raw_snapshot:
        return None
    extracted = extract_html_from_mhtml(raw_snapshot)
    extracted = remove_large_data_uris(extracted)
    return preprocess_html(extracted)


def load_piracy_brand_keywords() -> list[str]:
    conn = None
    cursor = None
    keywords: list[str] = []

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        table = safe_identifier(SEARCH_KEYWORDS_TABLE)
        kw_col = safe_identifier(SEARCH_KEYWORDS_COLUMN)
        brand_col = safe_identifier(SEARCH_KEYWORDS_BRAND_COLUMN)
        sql_string = f"""
            SELECT {kw_col}
            FROM {table}
            WHERE {brand_col} = true
              AND LENGTH({kw_col}) >= 6
        """
        cursor.execute(sql_string)
        rows = cursor.fetchall()
        for (kw,) in rows:
            if kw:
                keywords.append(str(kw).strip().lower())

    except Exception as e:
        logger.error(f"Error loading piracy brand keywords: {e}")
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)

    return keywords


def piracy_brand_known_for_domain(domain: str, brand_keywords: list[str]) -> bool:
    domain_lc = (domain or "").lower()
    if not domain_lc:
        return False
    for kw in brand_keywords:
        if kw and kw in domain_lc:
            return True
    return False


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        s = str(value)
        return datetime.fromisoformat(s.replace('Z', '').replace('z', ''))
    except Exception:
        return None


def evaluate_ssl(row: dict[str, Any] | None) -> float:
    if not row:
        return 0.7

    if not row.get('validation_type') and not row.get('issuer_organization'):
        return 0.7

    score = 0.0

    vt = row.get('validation_type')
    if isinstance(vt, str) and vt.lower() == 'domain':
        score += 0.3

    issuer = row.get('issuer_organization')
    issuer_str = str(issuer).lower() if issuer is not None else ''
    if any(x in issuer_str for x in ["let's encrypt", "google"]):
        score += 0.2

    valid_from = _to_datetime(row.get('valid_from'))
    valid_to = _to_datetime(row.get('valid_to'))
    if valid_from and valid_to:
        try:
            if (valid_to - valid_from).days <= 90:
                score += 0.2
        except Exception:
            pass

    pk_type = row.get('public_key_type')
    pk_type_str = str(pk_type) if pk_type is not None else ''
    try:
        pk_bits = int(row.get('public_key_bits', 0) or 0)
    except Exception:
        pk_bits = 0
    if (pk_type_str == 'RSA' and pk_bits < 2048) or (pk_type_str == 'ECDSA' and pk_bits < 256):
        score += 0.2

    policies = str(row.get('certificate_policies', '') or '')
    if '2.23.140.1.2.1' in policies:
        score += 0.2

    dns = str(row.get('dns_names', '') or '')
    if '*' in dns:
        score += 0.1

    return round(score, 2)


def compute_ssl_score(row: dict[str, Any] | None) -> float:
    base = evaluate_ssl(row)
    if SSL_SCORE_MODE.lower() == "trust":
        trust = 1.0 - base
        if trust < 0.0:
            trust = 0.0
        if trust > 1.0:
            trust = 1.0
        return round(trust, 2)
    return base


MEDIA_TYPE_ID_TO_NAME: dict[int, str] = {
    1: "Film & TV",
    3: "Anime",
    4: "Manga",
    2: "Sports",
    6: "Games",
    5: "Publishing",
    14: "Online Courses",
    8: "Music",
    9: "Adult",
    10: "News",
    13: "Content Host",
    7: "Software",
    11: "General",
    12: "Other",
    16: "Publishing",
    17: "invalid",
}


def init_db_pool():
    """Initialize the database connection pool."""
    global db_pool
    if db_pool is None:
        try:
            db_pool = pool.ThreadedConnectionPool(
                DB_POOL_MIN_CONN,
                DB_POOL_MAX_CONN,
                DB_CONNECTION
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Cannot initialize DB pool: {e}")
            raise


def close_db_pool():
    """Close all connections in the pool."""
    global db_pool
    if db_pool:
        db_pool.closeall()
        db_pool = None
        logger.info("Database connection pool closed")


def get_db_connection():
    """Get a connection from the pool."""
    global db_pool
    if db_pool is None:
        init_db_pool()
    return db_pool.getconn()


def release_db_connection(conn):
    """Return a connection to the pool."""
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)


def get_all_domain_secondary_domains() -> list[int]:
    """
    Get all sec_domain_id values from the secondary_domains table.
    Returns a list of secondary domain IDs to process.
    """
    conn = None
    cursor = None
    domain_ids = []

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        table = safe_identifier(SECONDARY_DOMAIN_TABLE)
        domain_id_col = safe_identifier(DOMAIN_ID_COLUMN)
        media_type_col = safe_identifier(MEDIA_TYPE_COLUMN)
        enforcement_col = safe_identifier(ENFORCEMENT_LABEL_COLUMN)

        sql_string = f"""
            SELECT sd.sec_domain_id 
            FROM secondary_domains sd
            where sd.sec_domain_media_type_id  IS NOT null
            and sd.sec_domain_piracy_class_v2_id is null
            and sd.online_status = 'Online'
        """

        cursor.execute(sql_string)
        results = cursor.fetchall()

        if results:
            domain_ids = [row[0] for row in results]
            logger.info(f"Found {len(domain_ids)} domains to process")
        else:
            logger.info("No domains found to process")

    except Exception as e:
        logger.error(f"Error getting discovery domains: {e}")
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)

    return domain_ids


def get_html_signals(sec_domain_id: int) -> tuple[str | None, bool, bool]:
    conn = None
    cursor = None
    html_content: str | None = None
    privacy_policy_detected = False
    terms_of_use_detected = False

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        table = safe_identifier(SECONDARY_DOMAIN_HTML_TABLE)
        sql_string = f"""
            SELECT html_content, privacy_policy, terms_of_use
            FROM {table}
            WHERE sec_domain_id = %s
            ORDER BY sec_domain_html_id DESC
            LIMIT 1
        """

        cursor.execute(sql_string, (sec_domain_id,))
        result = cursor.fetchone()

        if result:
            html_content = result[0]
            privacy_policy_detected = bool(result[1]) if result[1] is not None else False
            terms_of_use_detected = bool(result[2]) if result[2] is not None else False

    except Exception as e:
        logger.error(f"Error getting HTML signals for sec_domain_id {sec_domain_id}: {e}")
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)

    return (html_content, privacy_policy_detected, terms_of_use_detected)


def get_domain_metadata(
        domain_id: int,
        brand_keywords: list[str]
) -> tuple[int | None, str, bool, float]:
    conn = None
    cursor = None
    media_type_id: int | None = None
    domain_name = ""
    domain_root = ""
    piracy_brand_known = False
    ssl_score = 0.0

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        table = safe_identifier(SECONDARY_DOMAIN_TABLE)
        domain_id_col = safe_identifier(DOMAIN_ID_COLUMN)
        media_type_col = safe_identifier(MEDIA_TYPE_COLUMN)
        sql_string = f"""
            SELECT sec_domain, {media_type_col}
            FROM {table}
            WHERE {domain_id_col} = %s
        """
        cursor.execute(sql_string, (domain_id,))
        row = cursor.fetchone()
        if row:
            domain_name = normalize_domain(str(row[0] or ""))
            if row[1] is not None:
                media_type_id = int(row[1])

        piracy_brand_known = piracy_brand_known_for_domain(domain_name, brand_keywords)

        if domain_name:
            try:
                candidates: list[str] = []

                def add_candidate(v: str):
                    nv = normalize_domain(v)
                    if nv and nv not in candidates:
                        candidates.append(nv)
                    if nv.startswith("www."):
                        alt = nv.removeprefix("www.")
                        if alt and alt not in candidates:
                            candidates.append(alt)
                    else:
                        alt = f"www.{nv}"
                        if alt not in candidates:
                            candidates.append(alt)

                add_candidate(domain_name)
                if domain_root:
                    add_candidate(domain_root)

                ssl_table = safe_identifier(DOMAIN_SSL_TABLE)
                req_col = safe_identifier(DOMAIN_SSL_REQUESTED_DOMAIN_COLUMN)
                sql_string = f"""
                    SELECT validation_type,
                           issuer_organization,
                           valid_from,
                           valid_to,
                           public_key_type,
                           public_key_bits,
                           certificate_policies,
                           dns_names
                    FROM {ssl_table}
                    WHERE {req_col} = ANY(%s)
                    ORDER BY valid_from DESC NULLS LAST
                    LIMIT 1
                """
                cursor.execute(sql_string, (candidates,))
                ssl_row = cursor.fetchone()
                ssl_dict: dict[str, Any] | None = None
                if ssl_row:
                    ssl_dict = {
                        "validation_type": ssl_row[0],
                        "issuer_organization": ssl_row[1],
                        "valid_from": ssl_row[2],
                        "valid_to": ssl_row[3],
                        "public_key_type": ssl_row[4],
                        "public_key_bits": ssl_row[5],
                        "certificate_policies": ssl_row[6],
                        "dns_names": ssl_row[7],
                    }
                ssl_score = compute_ssl_score(ssl_dict)
            except Exception as e:
                logger.warning(
                    f"Could not compute ssl_score for domain_id {domain_id}: {e}. "
                    "Defaulting to 0.0"
                )

    except Exception as e:
        logger.error(f"Error getting metadata for domain_id {domain_id}: {e}")
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)

    return (media_type_id, domain_name, piracy_brand_known, ssl_score)


def update_enforcement_label(domain_id: int, label_id: int) -> bool:
    """
    Update enforcement label in secondary_domains for a specific sec_domain_id.
    Returns True if successful, False otherwise.
    """
    conn = None
    cursor = None
    success = False

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        table = safe_identifier(SECONDARY_DOMAIN_TABLE)
        domain_id_col = safe_identifier(DOMAIN_ID_COLUMN)
        enforcement_col = safe_identifier(ENFORCEMENT_LABEL_COLUMN)
        sql_string = f"""
            UPDATE {table}
            SET {enforcement_col} = %s
            WHERE {domain_id_col} = %s
        """

        cursor.execute(sql_string, (label_id, domain_id))
        conn.commit()
        success = True

    except Exception as e:
        logger.error(f"Error updating enforcement label for domain_id {domain_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)

    return success


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((RateLimitError, APIConnectionError)),
    before_sleep=lambda retry_state: logger.warning(
        f"Classification retry {retry_state.attempt_number} after error: {retry_state.outcome.exception()}"
    )
)
async def classify_enforcement(
        client: AsyncOpenAI,
        media_type_name: str,
        raw_html: str,
        prepared_text: str | None,
        piracy_brand_known: bool,
        ssl_score: float,
        privacy_policy_detected: bool,
        terms_of_use_detected: bool,
        blocked_snapshot_detected: bool,
        domain_id: int
) -> int:
    """
    Classify domain into an enforcement label using OpenAI.

    Returns a single label ID (int). Fallback: 0 (Exclude).
    """
    model = MODEL_NAME

    try:

        prepared = (prepared_text or prepare_snapshot_for_llm(raw_html) or "")

        if invalid_html(prepared):
            logger.info(
                f"domain_id {domain_id} has insufficient content after preprocessing, defaulting to Exclude (0)"
            )
            return 0

        if should_fast_exclude(prepared, raw_html):
            logger.info(
                f"domain_id {domain_id} matched fast-exclude patterns, defaulting to Exclude (0)"
            )
            return 0

        user_content = (
            "You are given the following site information:\n\n"
            f"media_type: \"{media_type_name}\"\n"
            f"piracy_brand_known: {piracy_brand_known}\n"
            f"ssl_score: {ssl_score}\n"
            f"privacy_policy_detected: {privacy_policy_detected}\n"
            f"terms_of_use_detected: {terms_of_use_detected}\n"
            f"blocked_snapshot_detected: {blocked_snapshot_detected}\n\n"
            "Below is extracted and cleaned page text from the snapshot (possibly truncated):\n\n"
            "```text\n"
            f"{prepared}\n"
            "```\n\n"
            "Use the system instructions to set the label_id in the EnforcementResponse schema."
        )

        messages = [
            {
                "role": "system",
                "content": ENFORCEMENT_CLASSIFICATION_PROMPT,
            },
            {
                "role": "user",
                "content": user_content,
            },
        ]

        common_kwargs = {
            "model": model,
            "messages": messages,
            "response_format": EnforcementResponse,
            "temperature": 0,
            "max_completion_tokens": 128,
            "store": False,
        }

        if str(REASONING_EFFORT).lower() == "none":
            completion = await client.beta.chat.completions.parse(**common_kwargs)
        else:
            try:
                completion = await client.beta.chat.completions.parse(
                    **common_kwargs,
                    reasoning={"effort": REASONING_EFFORT},
                )
            except TypeError as e:
                if "unexpected keyword argument" in str(e) and "reasoning" in str(e):
                    try:
                        completion = await client.beta.chat.completions.parse(
                            **common_kwargs,
                            reasoning_effort=REASONING_EFFORT,
                        )
                    except TypeError as e2:
                        if "unexpected keyword argument" in str(e2) and "reasoning_effort" in str(e2):
                            completion = await client.beta.chat.completions.parse(**common_kwargs)
                        else:
                            raise
                else:
                    raise

        result = completion.choices[0].message.parsed

        if not result:
            logger.warning(
                f"domain_id {domain_id} got empty parsed result, defaulting to Exclude (0)"
            )
            return 0

        label_id = int(result.label_id)

        if label_id in {0, 1, 13, 12, 15, 16, 17, 18, 19}:
            logger.info(
                f"domain_id {domain_id} classified as label_id: {label_id}"
            )
            return label_id

        logger.warning(
            f"domain_id {domain_id} got invalid label_id: "
            f"{label_id}, defaulting to Exclude (0)"
        )
        return 0

    except APIStatusError as e:
        logger.error(
            f"OpenAI API error classifying enforcement for domain_id {domain_id}: "
            f"{e.status_code} - {e.message}"
        )
        raise
    except Exception as e:
        logger.error(
            f"Error classifying enforcement for domain_id {domain_id}: {e}. "
            "Defaulting to Exclude (0)"
        )
        return 0


async def process_domain(
        client: AsyncOpenAI,
        domain_id: int,
        semaphore: asyncio.Semaphore,
        brand_keywords: list[str]
) -> tuple[int, str]:
    """
    Process a single domain: get inputs, classify enforcement, and update DB.
    Returns tuple (sec_domain_id, status) where status is 'processed', 'skipped', or 'error'.
    """
    async with semaphore:
        try:
            media_type_id, domain_name, piracy_brand_known, ssl_score = get_domain_metadata(
                domain_id=domain_id,
                brand_keywords=brand_keywords,
            )

            if media_type_id is None:
                logger.warning(
                    f"No media_type_id found for domain_id {domain_id}, skipping"
                )
                return (domain_id, 'skipped')

            media_type_name = MEDIA_TYPE_ID_TO_NAME.get(media_type_id, "Other")

            # Step 1: Get HTML content + crawler signals
            html_content, privacy_policy_detected, terms_of_use_detected = get_html_signals(domain_id)

            if not html_content:
                logger.warning(f"No HTML found for domain_id {domain_id}, skipping")
                return (domain_id, 'skipped')

            prepared_text = prepare_snapshot_for_llm(html_content)

            if invalid_html(prepared_text):
                logger.info(
                    f"Invalid content after preprocessing for domain_id {domain_id}, setting enforcement label to 0 (Exclude)"
                )
                update_enforcement_label(domain_id, 0)
                return (domain_id, 'processed')

            if prepared_text is not None and should_fast_exclude(prepared_text, html_content):
                logger.info(
                    f"Fast-exclude match for domain_id {domain_id}, setting enforcement label to 0 (Exclude)"
                )
                update_enforcement_label(domain_id, 0)
                return (domain_id, 'processed')

            blocked_snapshot_detected = detect_blocked_snapshot(prepared_text, html_content)
            if blocked_snapshot_detected:
                logger.info(
                    f"Blocked/interstitial snapshot detected for domain_id {domain_id}; sending to LLM for evaluation"
                )

            # Step 2: Classify with OpenAI
            label_id = await classify_enforcement(
                client=client,
                media_type_name=media_type_name,
                raw_html=html_content,
                prepared_text=prepared_text,
                piracy_brand_known=piracy_brand_known,
                ssl_score=ssl_score,
                privacy_policy_detected=privacy_policy_detected,
                terms_of_use_detected=terms_of_use_detected,
                blocked_snapshot_detected=blocked_snapshot_detected,
                domain_id=domain_id,
            )

            # Step 3: Update database
            success = update_enforcement_label(domain_id, label_id)

            if success:
                logger.info(
                    f"Successfully updated domain_id {domain_id} with enforcement label {label_id}"
                )
                return (domain_id, 'processed')
            else:
                return (domain_id, 'error')

        except Exception as e:
            logger.error(f"Error processing domain_id {domain_id}: {e}")
            return (domain_id, 'error')


async def main():
    """Main async entry point with concurrent processing."""
    logger.info("Starting enforcement classification process")

    # Initialize OpenAI client
    api_key = OPENAI_APIKEY or os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        raise ValueError("Missing OpenAI API key. Set OPENAI_API_KEY env var or fill OPENAI_APIKEY.")
    client = AsyncOpenAI(api_key=api_key)

    # Initialize DB pool
    init_db_pool()

    brand_keywords = load_piracy_brand_keywords()

    try:
        # Get all domain IDs to process
        domain_ids = get_all_domain_secondary_domains()

        if not domain_ids:
            logger.info("No domains to process. Exiting.")
            return

        logger.info(
            f"Processing {len(domain_ids)} domains with {MAX_CONCURRENT_REQUESTS} concurrent requests"
        )

        # Create semaphore for rate limiting
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        # Create tasks for all domains
        tasks = [
            process_domain(client, domain_id, semaphore, brand_keywords)
            for domain_id in domain_ids
        ]

        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count results
        processed_count = 0
        skipped_count = 0
        error_count = 0

        for result in results:
            if isinstance(result, Exception):
                error_count += 1
            elif isinstance(result, tuple):
                _, status = result
                if status == 'processed':
                    processed_count += 1
                elif status == 'skipped':
                    skipped_count += 1
                else:
                    error_count += 1

        # Summary
        logger.info("=" * 60)
        logger.info("EXECUTION SUMMARY")
        logger.info(f"Total domains: {len(domain_ids)}")
        logger.info(f"Processed: {processed_count}")
        logger.info(f"Skipped (no HTML): {skipped_count}")
        logger.info(f"Errors: {error_count}")
        logger.info("=" * 60)
        logger.info("Ending execution")

    finally:
        # Always close the DB pool
        close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
