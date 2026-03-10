import asyncio
import logging
import os
import re
import psycopg2
from psycopg2 import pool
from pydantic import BaseModel
from openai import AsyncOpenAI, RateLimitError, APIConnectionError, APIStatusError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import settings
from settings import DB_CONNECTION
from dotenv import load_dotenv
from typing import Literal



# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



load_dotenv()
logger.info('Loading script')
OPENAI_APIKEY = settings.openia_apikey

# Valid media type IDs
VALID_MEDIA_TYPES = {1, 3, 4, 2, 6, 5, 14, 8, 9, 10, 13, 7, 12, 17}

# Configuration constants
MAX_HTML_CHARS = 80000  # ~5k tokens approx
MAX_CONCURRENT_REQUESTS = 5  # Semaphore limit for API calls
DB_POOL_MIN_CONN = 1
DB_POOL_MAX_CONN = 10

# Database connection pool (initialized lazily)
db_pool: pool.ThreadedConnectionPool | None = None



class MediaTypeResponse(BaseModel):
    media_type: Literal[1, 3, 4, 2, 6, 5, 14, 8, 9, 10, 13, 7, 12, 17]



MEDIA_TYPE_CLASSIFICATION_PROMPT = '''You are a media type classifier.

You will receive the HTML content of a website. Your task is to analyze this HTML and classify the website into exactly ONE media type ID.

The HTML may be in ANY language. Use all available signals (titles, headings, meta tags, menus, visible text, links, image alt texts, structured data, etc.) to infer the MAIN purpose of the site.

You MUST respond using the MediaTypeResponse schema, setting the integer field `media_type` to one of the allowed IDs listed below.

If multiple categories seem to apply, choose the one that best represents the PRIMARY focus of the site, following the rules and priorities below.
If the content is present but ambiguous, use "Other" (ID: 12).
If the content is blocked or missing (parking / captcha / access denied / region blocked), use "invalid" (ID: 17).

Valid media_type IDs:
1, 3, 4, 2, 6, 5, 14, 8, 9, 10, 13, 7, 12, 17

Your response must be compatible with the MediaTypeResponse schema: an object with a single field:
- media_type: one of the integer IDs above.

Do NOT add explanations or extra fields. Only set `media_type` correctly.


## Classification Rules


### ID: 1 - Film & TV
Classify as Film & TV if the site is mainly about watching or accessing movies, series or TV content, including:
- Streaming services like Netflix, Disney Plus, Apple TV, HBO Max, etc.
- Online movie sites (e.g. Cuevana, PelisHD, Pelispedia)
- Torrent sites that EXCLUSIVELY offer movies and series
- Online TV sites or IPTV-like services (e.g. Pluto.tv)

Priority rule:
- If the site offers movies, series AND anime mixed together as watchable/downloadable content, prefer Film & TV (ID: 1) instead of Anime (ID: 3).
- If the site offers movies, series, anime AND manga mixed together, prefer Film & TV (ID: 1).

Do NOT use Film & TV for:
- Social media sites
- Celebrity gossip blogs
- General download sites with many file types
- Anime-only sites (use Anime, ID: 3)
- Blogs or info sites without streaming or downloads


### ID: 3 - Anime
Classify as Anime if the site is mainly about:
- Online anime streaming
- Sites that offer BOTH anime AND manga content together
- Stores EXCLUSIVELY selling anime/manga related items (clothes, toys, figures, magazines, etc.)
- Torrent or download sites offering anime content (with or without manga)

Priority rule:
- If the site offers BOTH anime AND manga content, prefer Anime (ID: 3) instead of Manga (ID: 4).

Do NOT use Anime for:
- General cartoon sites mixed with movies/series (prefer Film & TV, ID: 1, if focused on video/TV)
- General download sites with many categories
- Manga-only sites without anime content (use Manga, ID: 4)


### ID: 4 - Manga
Classify as Manga ONLY if the site is EXCLUSIVELY about manga content:
- Online manga reading sites with NO anime content
- Manga download sites with NO anime streaming or downloads
- Sites focused solely on manga chapters, scanlations, or manga news

Priority rules:
- If the site has BOTH manga AND anime content, use Anime (ID: 3) instead.
- If the site has manga, anime AND movies/series, use Film & TV (ID: 1) instead.
- Only use Manga (ID: 4) when the site is 100% dedicated to manga with NO anime presence.

Do NOT use Manga for:
- Sites that also offer anime streaming or downloads (use Anime, ID: 3)
- Sites mixed with movies/series content (use Film & TV, ID: 1)
- General download sites with many categories


### ID: 2 - Sports
Classify as Sports if the site is focused primarily on sports content:
- Sports streaming platforms (e.g. SkySports, ESPN)
- Sites that stream live matches
- Sports betting sites focused on sports (e.g. bet365)
- Sports-only news portals (scores, fixtures, sports news)

Do NOT use Sports for:
- Casino-only sites (use Other, ID: 12)
- General TV streaming services (use Film & TV, ID: 1, if mainly video/TV)
- General news sites that just include a sports section (use News, ID: 10)


### ID: 6 - Games
Classify as Games if the site is mainly dedicated to video games:
- Game download sites
- Cheat codes, game cracks, or trainers sites
- Torrent sites EXCLUSIVELY for game content
- Online gaming platforms and stores (Steam, Epic Games, EA, Battle.net)
- Sites for in-game currency or tokens (e.g. coins, skins marketplaces)

Do NOT use Games for:
- General download sites that also host software, movies, etc. (use Content Host, ID: 13)
- Clothing marketplaces with some game-related merchandise (use Other, ID: 12)
- Betting/casino game sites (use Other, ID: 12, unless clearly sports betting → Sports, ID: 2)


### ID: 5 - Publishing
Classify as Publishing if the site is mainly about:
- Online PDF/book download sites (ebooks, novels, manuals)
- Book marketplaces (physical or digital)
- Audiobook download or streaming sites
- Scientific research/document repositories (papers, journals, academic publications)

Do NOT use Publishing for:
- Manga-only sites (use Manga, ID: 4)
- Sites with manga and anime (use Anime, ID: 3)
- General college/university sites that focus on institutional information (use Other, ID: 12)


### ID: 14 - Online Courses
Classify as Online Courses if the site is mainly about structured learning and online education:
- Platforms that host or sell online courses (e.g. Udemy, Coursera, edX)
- Sites that provide course catalogs, enrollment, and online classes (live or recorded)
- .edu or university sites when they clearly promote or host online courses or virtual learning

If it is a college/university general homepage mainly about the institution (campus info, admissions, generic info) and online courses are not central, use Other (ID: 12).


### ID: 8 - Music
Classify as Music when the site is focused on music as media:
- Online music streaming services (Spotify, Deezer, Apple Music)
- Music download sites (mp3, flac, albums)
- Torrent sites ONLY for music content
- Music-only news sites (artists, albums, concerts)
- Online radio streaming platforms

Do NOT use Music for:
- Sites where you can download music AND other file types such as movies or software (use Content Host, ID: 13)
- Lyrics-only or chords-only sites (use Other, ID: 12)


### ID: 9 - Adult
Classify as Adult if the main content is:
- Pornographic content sites
- Escort services or prostitution-related services
- Hentai-only content sites

If adult content is only a small section but the overall site is clearly about another category, classify by the main focus. Only use Adult (ID: 9) when the site is primarily adult-oriented.


### ID: 10 - News
Classify as News when the primary purpose is informational news coverage:
- General news portals (NYT, BBC, CNN, etc.)
- Newspapers and online magazines
- Official government sites with news, bulletins or official announcements as the main content

Do NOT use News for:
- News sites exclusively covering Film & TV (use Film & TV, ID: 1)
- News sites exclusively covering Anime (use Anime, ID: 3)
- News sites exclusively covering Sports (use Sports, ID: 2)
- News sites exclusively covering Music (use Music, ID: 8)


### ID: 13 - Content Host
Classify as Content Host when the site is a general file hosting or distribution platform:
- General download sites hosting multiple media types (movies, series, games, software, music, Publishing, etc.)
- Torrent or file indexing sites with several distinct categories

Use Content Host (ID: 13) when no single media type clearly dominates and the main role is indexing/hosting diverse files.


### ID: 7 - Software
Classify as Software when the site is focused primarily on software:
- Software product pages, download pages, app stores
- Sites that provide software tools, utilities, apps, or SaaS dashboards

Do NOT use Software for:
- General download sites that also include movies, music, games, etc. (use Content Host, ID: 13)


### ID: 12 - Other
Classify as Other when the site does not fit clearly into any of the above media types and is NOT blocked/empty. Examples:
- Clothing or fashion marketplaces
- E-commerce sites for general products
- Blogs and personal websites (non-media focused)
- Corporate or industrial equipment sites
- General betting or casino sites
- College/university sites focused on institutional information (not mainly online courses)
- Any general-purpose site that is not primarily about Film & TV, Anime, Manga, Sports, Games, Publishing, Online Courses, Music, Adult, News, Content Host or Software


### ID: 17 - invalid
Return invalid (ID: 17) in ANY of these situations:
- The HTML has almost no meaningful text content
- The page clearly indicates access denied, region blocking or similar
- The domain is parked, for sale, "coming soon" or a generic placeholder/parking page
- The page is a captcha or bot protection page without real site content
- You cannot reasonably infer the real purpose of the site because the content is blocked, missing or replaced by generic placeholders

If the content is present but simply unclear or mixed, use Other (ID: 12).
Only use invalid (ID: 17) when the problem is missing/blocked content, not ambiguity.


## Output Requirements

- Follow the MediaTypeResponse schema.
- Set `media_type` to EXACTLY ONE of the following integer IDs:
  1, 3, 4, 2, 6, 5, 14, 8, 9, 10, 13, 7, 12, 17
- Do NOT include explanations or additional fields.
'''


def extract_semantic_content(html_content: str) -> dict:
    """
    Extract semantically important content from HTML.
    Preserves key signals for classification.
    """
    result = {
        'title': '',
        'meta_description': '',
        'meta_keywords': '',
        'headings': [],
        'body_text': ''
    }
    
    # Extract <title>
    title_match = re.search(r'<title[^>]*>([^<]+)</title>', html_content, re.IGNORECASE)
    if title_match:
        result['title'] = title_match.group(1).strip()
    
    # Extract meta description
    meta_desc = re.search(r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\'>]+)["\']', html_content, re.IGNORECASE)
    if not meta_desc:
        meta_desc = re.search(r'<meta[^>]*content=["\']([^"\'>]+)["\'][^>]*name=["\']description["\']', html_content, re.IGNORECASE)
    if meta_desc:
        result['meta_description'] = meta_desc.group(1).strip()
    
    # Extract meta keywords
    meta_kw = re.search(r'<meta[^>]*name=["\']keywords["\'][^>]*content=["\']([^"\'>]+)["\']', html_content, re.IGNORECASE)
    if meta_kw:
        result['meta_keywords'] = meta_kw.group(1).strip()
    
    # Extract headings (h1-h3)
    headings = re.findall(r'<h[1-3][^>]*>([^<]+)</h[1-3]>', html_content, re.IGNORECASE)
    result['headings'] = [h.strip() for h in headings[:10]]  # Limit to 10 headings
    
    return result


def preprocess_html(html_content: str) -> str | None:
    """
    Clean and structure HTML content for classification.
    Extracts semantic signals and truncates to MAX_HTML_CHARS.
    """
    if not html_content:
        return None
    
    # Extract semantic content first
    semantic = extract_semantic_content(html_content)
    
    # Build structured prefix with key signals
    prefix_parts = []
    if semantic['title']:
        prefix_parts.append(f"TITLE: {semantic['title']}")
    if semantic['meta_description']:
        prefix_parts.append(f"DESC: {semantic['meta_description']}")
    if semantic['meta_keywords']:
        prefix_parts.append(f"KEYWORDS: {semantic['meta_keywords']}")
    if semantic['headings']:
        prefix_parts.append(f"HEADINGS: {' | '.join(semantic['headings'])}")
    
    prefix = '\n'.join(prefix_parts)
    
    # Clean body text (remove scripts, styles, tags)
    text = html_content
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'[\U00010000-\U0010ffff]', '', text)  # Remove emojis
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Combine prefix + body text
    if prefix:
        full_text = f"{prefix}\n\nCONTENT: {text}"
    else:
        full_text = text
    
    # Truncate to MAX_HTML_CHARS
    if len(full_text) > MAX_HTML_CHARS:
        full_text = full_text[:MAX_HTML_CHARS] + "... [TRUNCATED]"
    
    return full_text if full_text.strip() else None


def invalid_html(text: str) -> bool:
    """Check if HTML content is too short to classify. Model will determine validity."""
    try:
        if not text:
            return True
        # word count < 4
        if len(text.split(" ")) < 4:
            return True
        return False
    except Exception as e:
        logger.error(f"Error on invalid_html method: {e}")
        return True


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


def get_all_discovery_domains() -> list[int]:
    """
    Get all sec_domain_id from domain_discovery table.
    Returns list of domain IDs to process.
    """
    conn = None
    cursor = None
    domain_ids = []
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        sql_string = """
            SELECT sec_domain_id 
            FROM secondary_domains
            WHERE sec_domain_media_type_id IS NULL
              AND online_status = 'Online'
              and added > '2025-01-01'
            -- LIMIT 1000
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


def get_html(sec_domain_id: int) -> str | None:
    """
    Get html_content from secondary_domains_html for a specific sec_domain_id.
    Returns None if not found.
    """
    conn = None
    cursor = None
    html_content = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        sql_string = """
            SELECT html_content 
            FROM secondary_domains_html 
            WHERE sec_domain_id = %s
        """
        
        cursor.execute(sql_string, (sec_domain_id,))
        result = cursor.fetchone()
        
        if result:
            html_content = result[0]
            
    except Exception as e:
        logger.error(f"Error getting HTML for sec_domain_id {sec_domain_id}: {e}")
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)
    
    return html_content


def update_media_type(sec_domain_id: int, sec_domain_media_type_id: int) -> bool:
    """
    Update sec_domain_media_type_id in secondary_domains for a specific sec_domain_id.
    Returns True if successful, False otherwise.
    """
    conn = None
    cursor = None
    success = False
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        sql_string = """
            UPDATE secondary_domains 
            SET sec_domain_media_type_id = %s 
            WHERE sec_domain_id = %s
        """
        
        cursor.execute(sql_string, (sec_domain_media_type_id, sec_domain_id))
        conn.commit()
        success = True
        
    except Exception as e:
        logger.error(f"Error updating media type for sec_domain_id {sec_domain_id}: {e}")
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
async def classify_media_type(
    client: AsyncOpenAI,
    processed_html: str,
    sec_domain_id: int
) -> int:
    """
    Classify processed HTML content into a media type using OpenAI.

    Returns a single media type ID (int). Fallback: 12 (Other).
    """
    # Puedes usar el alias estable si preferís:
    # model = "gpt-5.1"
    model = "gpt-5.1-2025-11-13"

    try:

        completion = await client.beta.chat.completions.parse(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": MEDIA_TYPE_CLASSIFICATION_PROMPT,
                },
                {
                    "role": "user",
                    "content": f"Classify:\n{processed_html}",
                },
            ],
            # Structured output con Pydantic
            response_format=MediaTypeResponse,
            # Para clasificación: determinismo y bajo coste
            # GPT-5.1 usa reasoning tokens internamente, necesita margen
            temperature=0,
            max_completion_tokens=128,
            store=False,
        )

        result = completion.choices[0].message.parsed

        if not result:
            logger.warning(
                f"sec_domain_id {sec_domain_id} got empty parsed result, defaulting to Other (12)"
            )
            return 12

        media_type_id = int(result.media_type)

        if media_type_id in VALID_MEDIA_TYPES:
            logger.info(
                f"sec_domain_id {sec_domain_id} classified as: {media_type_id}"
            )
            return media_type_id

        # Si el ID no está en la lista de válidos, hacemos fallback
        logger.warning(
            f"sec_domain_id {sec_domain_id} got invalid media_type: "
            f"{media_type_id}, defaulting to Other (12)"
        )
        return 12

    except APIStatusError as e:
        logger.error(
            f"OpenAI API error classifying sec_domain_id {sec_domain_id}: "
            f"{e.status_code} - {e.message}"
        )
        raise
    except Exception as e:
        logger.error(
            f"Error classifying sec_domain_id {sec_domain_id}: {e}. "
            "Defaulting to Other (12)"
        )
        return 12

async def process_domain(
    client: AsyncOpenAI,
    sec_domain_id: int,
    semaphore: asyncio.Semaphore
) -> tuple[int, str]:
    """
    Process a single domain: get HTML, classify, and update DB.
    Returns tuple (sec_domain_id, status) where status is 'processed', 'skipped', or 'error'.
    """
    async with semaphore:
        try:
            # Step 1: Get HTML content
            html_content = get_html(sec_domain_id)
            
            if not html_content:
                logger.warning(f"No HTML found for sec_domain_id {sec_domain_id}, skipping")
                return (sec_domain_id, 'skipped')
            
            # Step 2: Preprocess HTML
            processed_html = preprocess_html(html_content)
            
            # Step 3: Check if valid
            if invalid_html(processed_html):
                logger.info(f"Invalid HTML for sec_domain_id {sec_domain_id}, setting media_type to 17 (invalid)")
                update_media_type(sec_domain_id, 17)
                return (sec_domain_id, 'processed')
            
            # Step 4: Classify with OpenAI
            media_type_id = await classify_media_type(client, processed_html, sec_domain_id)
            
            # Step 5: Update database
            success = update_media_type(sec_domain_id, media_type_id)
            
            if success:
                logger.info(f"Successfully updated sec_domain_id {sec_domain_id} with media_type {media_type_id}")
                return (sec_domain_id, 'processed')
            else:
                return (sec_domain_id, 'error')
                
        except Exception as e:
            logger.error(f"Error processing sec_domain_id {sec_domain_id}: {e}")
            return (sec_domain_id, 'error')


async def main():
    """Main async entry point with concurrent processing."""
    logger.info("Starting media type classification process")
    
    # Initialize OpenAI client
    client = AsyncOpenAI(api_key=OPENAI_APIKEY)
    
    # Initialize DB pool
    init_db_pool()
    
    try:
        # Get all domain IDs to process
        domain_ids = get_all_discovery_domains()
        
        if not domain_ids:
            logger.info("No domains to process. Exiting.")
            return
        
        logger.info(f"Processing {len(domain_ids)} domains with {MAX_CONCURRENT_REQUESTS} concurrent requests")
        
        # Create semaphore for rate limiting
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        
        # Create tasks for all domains
        tasks = [
            process_domain(client, sec_domain_id, semaphore)
            for sec_domain_id in domain_ids
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
