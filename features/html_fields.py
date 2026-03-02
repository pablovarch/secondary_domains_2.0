from dependencies import  log
from settings import db_connect, openia_apikey, DB_CONNECTION
import psycopg2
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
from html import unescape
import re
from openai import OpenAI
import trafilatura


class html_fields:

    # ------------------------------------------------------------------ #
    #  CONSTANTES DE CLASE                                               #
    # ------------------------------------------------------------------ #

    _AFFILIATE_DOMAINS = [
        'amazon.', 'amzn.to', 'clickbank.', 'shareasale.', 'cj.com',
        'awin1.', 'impact.com', 'partnerize.', 'rakuten.', 'linksynergy.',
        'ebay.', 'glnk.io', 'go2cloud.', 'rstyle.me', 'go.redirectingat.com',
    ]
    _AFFILIATE_QUERY_KEYS = [
        'affid', 'affiliate', 'aff', 'ref', 'refid', 'subid', 'tag',
        'utm_medium', 'utm_campaign',
    ]

    _ECOM_CDNS = [
        r'cdn\.shopify\.com', r'wp-content/plugins/woocommerce',
        r'checkout\.shopify\.com', r'cart\.js', r'mage/.*\.js',
        r'opencart', r'bigcommerce', r'squarespace-commerce',
        r'paypal\.com/sdk', r'stripe\.com',
    ]
    _ECOM_FORM_HINTS = [r'cart', r'checkout', r'order', r'payment', r'wp-cart']
    _ECOM_CLASS_HINTS = [r'add[-_]to[-_]cart', r'cart-btn', r'btn-buy', r'product-price']

    _AD_KEYWORDS = [
        r'\bad\b', r'\bads\b', r'advert', r'sponsor', r'dfp', r'gpt',
        r'adslot', r'ad-unit', r'outstream', r'preroll', r'banner',
        r'sticky-ad', r'interstitial', r'pub', r'anuncio', r'publicidad',
        r'annonce', r'werbung',
    ]
    _AD_KEYWORDS_EXCLUDE = re.compile(r'\b(address|adapter|addthis|additional|pub(?:lish|lic|lication))\b', re.I)

    _AD_SRC_PATTERNS = [
        'doubleclick', 'googlesyndication', 'adnxs', 'adservice',
        'adsystem', 'criteo', 'taboola', 'outbrain',
    ]

    _VENDOR_GPT    = re.compile(r'googletag|gpt\.js|doubleclick|securepubads', re.I)
    _VENDOR_HB     = re.compile(
        r'prebid|amazon-adsystem|criteo|rubicon|openx|indexexchange|pubmatic|sovrn|adnxs', re.I
    )
    _VENDOR_ARBITRAGE = re.compile(r'taboola|outbrain|revcontent|mgid|zergnet', re.I)

    _REC_WIDGET_TEXT = re.compile(
        r'recommended|from around the web|you may like|sponsored stories|around the web', re.I
    )

    _PAGINATION_ANCHORS = re.compile(
        r'\b(next|siguiente|slide|gallery|page[\s-]?\d+)\b', re.I
    )
    _PAGINATION_URL = re.compile(r'/page/\d+|[?&]page=|/amp/', re.I)

    _CLICKBAIT = re.compile(
        r"(you won.t believe|shocking|no vas a creer|te sorprender|lo que pasó"
        r"|\d+\s+(things|cosas|razones)|top\s+\d+|number\s+\d+|before.*after"
        r"|antes.*después|mira esto)",
        re.I,
    )

    _ALLOWED_GRAYMARKET_LABELS = {
        'Adult Content',
        'Gambling & Betting',
        'Cryptocurrency Speculation',
        'Supplement / Nutra',
        'undeterminated',
    }
    _ALLOWED_MFA_LABELS = {'mfa', 'unknow'}

    _LLM_MODEL   = 'gpt-5.1-2025-11-13'
    _LLM_TEMP    = 0
    _MFA_THRESHOLD = 65

    def __init__(self):
        self.__logger = log.Log().get_logger(name='ad_count.log')
        self.__openai_client = OpenAI(api_key=openia_apikey)

    def main(self):
        self.__logger.info('Starting html fields script')
        self.__logger.info('getting all secondary_domains')

        list_to_scan = self.get_all_secondary_domains()
        for dom in list_to_scan:
            sec_domain_id = dom['sec_domain_id']
            sec_domain = dom['sec_domain']
            try:
                self.__logger.info(f'------get html from site {dom}')
                html = self.get_html(sec_domain_id)
                if html:
                    result_detect_ecommerce_signals = self.detect_ecommerce_signals(html)
                    result_detect_affiliate_handoffs = self.detect_affiliate_handoffs(html)
                    ad_count = self.count_ad_slots_from_html(html)

                    mfa_features = self.extract_mfa_features(html)
                    mfa_score    = self.compute_mfa_score(mfa_features)
                    graymarket_label = self.process_html_to_graymarket(html, mfa_features, mfa_score)

                    self.__logger.info(
                        f'{sec_domain_id} | mfa_score={mfa_score} | label={graymarket_label}'
                    )
                    print(f'{sec_domain_id} --- score={mfa_score} --- {graymarket_label}')

                    self.__logger.info(f'update secondary domain id = {sec_domain_id}')
                    self.update_secondary_domain(
                        sec_domain_id,
                        ad_count,
                        result_detect_affiliate_handoffs['has_affiliate_handoff'],
                        result_detect_ecommerce_signals['is_ecommerce'],
                        graymarket_label,
                    )
                else:
                    self.__logger.info('site has not html')

            except Exception as e:
                self.__logger.error(f'Error getting htmls fields - {dom} - error {e}')

    def _db_connect(self):
        """Retorna una conexión psycopg2 activa."""
        try:
            conn = psycopg2.connect(DB_CONNECTION)
            return conn
        except Exception as e:
            print(f'::DBConnect:: cant connect to DB Exception: {e}')
            raise

    def get_all_secondary_domains(self):
        sql_string = """
            SELECT DISTINCT sd.sec_domain_id, sd.sec_domain
            FROM secondary_domains sd
            INNER JOIN secondary_domains_html sdh ON sd.sec_domain_id = sdh.sec_domain_id
            WHERE
                sd.graymarket_label IS NULL
                AND sd.online_status = 'Online'
                AND sd.redirect_domain = False;
        """
        list_all_domains = []
        conn = self._db_connect()
        cursor = conn.cursor()
        try:
            cursor.execute(sql_string)
            respuesta = cursor.fetchall()
            conn.commit()
            if respuesta:
                for elem in respuesta:
                    list_all_domains.append({
                        'sec_domain_id': elem[0],
                        'sec_domain': elem[1],
                    })
        except Exception as e:
            self.__logger.error(f':::: Error found trying to get_all_secondary_domains: {e}')
        finally:
            cursor.close()
            conn.close()
        return list_all_domains

    def get_html(self, sec_domain_id):
        sql_string = """SELECT sdh.html_content FROM secondary_domains_html sdh WHERE sdh.sec_domain_id = %s"""
        html = None
        conn = self._db_connect()
        cursor = conn.cursor()
        try:
            cursor.execute(sql_string, (sec_domain_id,))
            respuesta = cursor.fetchone()
            conn.commit()
            if respuesta:
                html = respuesta[0]
        except Exception as e:
            self.__logger.error(f':::: Error found trying to get_html: {e}')
        finally:
            cursor.close()
            conn.close()
        return html

    def _is_affiliate_link(self, url: str) -> bool:
        """True si URL coincide con patrón afiliado."""
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if any(dom for dom in self._AFFILIATE_DOMAINS if dom in host):
            return True
        params = parse_qs(parsed.query)
        for key in params:
            if key.lower() in self._AFFILIATE_QUERY_KEYS:
                return True
        return False

    def detect_affiliate_handoffs(self, html: str) -> dict:
        """Devuelve dict con enlaces de afiliado detectados."""
        min_links: int = 1
        soup = BeautifulSoup(html, "html.parser")
        affiliate_links = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if self._is_affiliate_link(href):
                affiliate_links.append(href)

        return {
            "links": affiliate_links,
            "has_affiliate_handoff": len(affiliate_links) >= min_links,
        }

    def detect_ecommerce_signals(self, html: str) -> dict:
        """Devuelve dict con señales e-commerce encontradas."""
        min_hits: int = 2
        soup = BeautifulSoup(html, "html.parser")
        signals = []

        # 1) JSON-LD Product / Offer
        for script in soup.find_all("script", type="application/ld+json"):
            if re.search(r'"@type"\s*:\s*"(Product|Offer|AggregateOffer)"', script.string or '', re.I):
                signals.append("jsonld_product")
                break

        # 2) Assets de plataformas e-commerce
        for tag in soup.find_all(src=True):
            for pat in self._ECOM_CDNS:
                if re.search(pat, tag["src"], re.I):
                    signals.append("ecom_asset:" + pat)
                    break

        # 3) Formularios de carrito / checkout
        for form in soup.find_all("form", action=True):
            action_lower = form["action"].lower()
            if any(re.search(hint, action_lower) for hint in self._ECOM_FORM_HINTS):
                signals.append("ecom_form")
                break

        # 4) Botones o enlaces con clases/ids típicos
        for elem in soup.find_all(True, {"class": True}):
            classes = " ".join(elem.get("class", [])).lower()
            if any(re.search(cls, classes) for cls in self._ECOM_CLASS_HINTS):
                signals.append("ecom_ui_hint")
                break

        return {
            "signals": signals,
            "is_ecommerce": len(signals) >= min_hits,
        }

    def count_ad_slots_from_html(self, html: str) -> int:
        soup = BeautifulSoup(html, "html.parser")
        ad_tags = soup.find_all(["iframe", "div", "section", "ins"])
        return sum(1 for tag in ad_tags if self.looks_like_ad(tag))

    def looks_like_ad(self, tag) -> bool:
        data_attrs = " ".join(
            str(v) for k, v in tag.attrs.items() if k.startswith("data-")
        )
        attrs_to_check = [
            tag.get("id", ""),
            tag.get("class", ""),
            tag.get("name", ""),
            tag.get("src", ""),
            data_attrs,
        ]
        flat_attrs = " ".join(
            [a if isinstance(a, str) else " ".join(a) for a in attrs_to_check]
        ).lower()

        if any(re.search(pat, flat_attrs) for pat in self._AD_KEYWORDS):
            if self._AD_KEYWORDS_EXCLUDE.search(flat_attrs):
                return False
            return True

        src = tag.get("src", "").lower()
        if any(p in src for p in self._AD_SRC_PATTERNS):
            return True

        return False

    def extract_relevant_text(self, html: str) -> str:
        """
        Extrae texto principal usando trafilatura (main content extraction).
        Fallback a BeautifulSoup si trafilatura no retorna contenido.
        Retorna: title + description + main_text separados por líneas en blanco.
        """
        soup = BeautifulSoup(html, "html.parser")

        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        meta = soup.find("meta", attrs={"name": "description"}) or \
               soup.find("meta", attrs={"property": "og:description"})
        description = meta.get("content", "").strip() if meta else ""

        main_text = trafilatura.extract(html, include_comments=False, include_tables=False) or ""

        if not main_text:
            for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
                tag.decompose()
            raw = soup.get_text(separator=" ")
            main_text = unescape(re.sub(r"\s+", " ", raw)).strip()

        parts = [p for p in (title, description, main_text) if p]
        return "\n\n".join(parts)

    def extract_main_text(self, html: str) -> str:
        """
        Retorna solo el texto principal (sin título/meta) usando trafilatura.
        Usado internamente para cálculos de word_count y repetition_score.
        """
        main_text = trafilatura.extract(html, include_comments=False, include_tables=False) or ""
        if not main_text:
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
                tag.decompose()
            raw = soup.get_text(separator=" ")
            main_text = unescape(re.sub(r"\s+", " ", raw)).strip()
        return main_text


    def llm_classify_graymarket(self, text: str) -> str:
        """
        Clasifica el contenido del sitio en una de las etiquetas gray-market.
        Usa el LLM con salida estricta. Retorna una de _ALLOWED_GRAYMARKET_LABELS.
        """
        prompt = (
            "You are a compliance screening engine specialized in detecting gray-market and harmful web content.\n"
            "The excerpt below may be in ANY language. Your task is to assign EXACTLY ONE label from the list.\n\n"

            "=== CLASSIFICATION LABELS ===\n\n"

            "• Adult Content\n"
            "  CLASSIFY as Adult Content if the site's PRIMARY purpose involves any of:\n"
            "  - Pornography, explicit sexual imagery, nudity, or sexual acts (real or illustrated).\n"
            "  - Escort, companionship, or sex worker services (even if euphemistically described).\n"
            "  - Dating or hookup platforms with explicit or sexual-intent language.\n"
            "  - Products or pills aimed at sexual performance, libido, or enhancement (Viagra-type, penis enlargement, etc.).\n"
            "  - Cam sites, OnlyFans-type platforms, adult live streaming.\n"
            "  SIGNALS: words like 'xxx', 'nude', 'naked', 'escort', 'hookup', 'cam', 'onlyfans', 'sex', 'porn', "
            "'adult', 'erotic', 'libido', 'enhancement', 'enlargement', or equivalents in any language.\n"
            "  NOTE: Classify even if content uses soft euphemisms or indirect language to describe sexual services.\n\n"

            "• Gambling & Betting\n"
            "  CLASSIFY as Gambling & Betting if the site's PRIMARY purpose involves any of:\n"
            "  - Online casinos, slot machines, poker, roulette, blackjack, or table games.\n"
            "  - Sports betting, horse racing bets, eSports wagering.\n"
            "  - Lotteries, scratch cards, sweepstakes with monetary prizes.\n"
            "  - Fantasy sports platforms where real money is wagered.\n"
            "  - Tipster or betting prediction services (even if framed as 'analysis').\n"
            "  - Poker strategy or casino review sites whose main revenue is affiliate referrals to gambling platforms.\n"
            "  SIGNALS: words like 'bet', 'casino', 'odds', 'wager', 'jackpot', 'slot', 'poker', 'roulette', "
            "'sportsbook', 'tipster', 'free spins', 'bonus deposit', 'betting tips', or equivalents in any language.\n"
            "  NOTE: Classify affiliate/review sites whose primary call-to-action leads to gambling platforms.\n\n"

            "• Cryptocurrency Speculation\n"
            "  CLASSIFY as Cryptocurrency Speculation if the site's PRIMARY purpose involves any of:\n"
            "  - Promotion of high-risk, unregulated, or obscure crypto tokens and altcoins.\n"
            "  - NFT projects marketed primarily as financial investments or 'flips'.\n"
            "  - Get-rich-quick or passive income schemes based on crypto (yield farms, staking with unrealistic APY).\n"
            "  - Pump-and-dump communities, 'alpha' groups, or coordinated token promotion channels.\n"
            "  - Unregulated trading signal services or bots promising guaranteed crypto returns.\n"
            "  - ICO/presale promotions for new tokens with speculative framing.\n"
            "  SIGNALS: words like 'token', 'altcoin', 'NFT', 'presale', 'whitelist', '100x', 'moon', 'pump', "
            "'airdrop', 'yield', 'APY', 'staking rewards', 'crypto signals', 'buy now before launch', or equivalents.\n"
            "  NOTE: Do NOT classify mainstream financial news or educational crypto content. Focus on speculative promotion.\n\n"

            "• Supplement / Nutra\n"
            "  CLASSIFY as Supplement / Nutra if the site's PRIMARY purpose involves any of:\n"
            "  - Selling or promoting dietary supplements, vitamins, minerals, or herbal products.\n"
            "  - Weight loss pills, fat burners, appetite suppressants, or detox/cleanse products.\n"
            "  - Muscle growth, bodybuilding supplements, protein powders, pre-workouts, or testosterone boosters.\n"
            "  - Anti-aging creams, nootropics, or cognitive enhancement pills.\n"
            "  - Sexual health supplements (distinct from prescription drugs — e.g., 'natural' libido boosters, "
            "herbal Viagra, male enhancement pills).\n"
            "  - Keto, paleo, or other diet-branded supplement product lines.\n"
            "  SIGNALS: words like 'supplement', 'capsule', 'pill', 'formula', 'boost', 'burn fat', 'weight loss', "
            "'muscle', 'testosterone', 'collagen', 'probiotic', 'detox', 'cleanse', 'nootropic', 'keto', "
            "'natural remedy', 'buy now', 'order today', 'free trial', 'as seen on TV', or equivalents in any language.\n"
            "  NOTE: Classify nutra affiliate/review blogs whose primary purpose is to sell or refer supplement products.\n\n"

            "=== DECISION RULES ===\n"
            "1. Assign the label whose signals are MOST DOMINANT in the content.\n"
            "2. If two categories overlap (e.g., sexual health supplements + adult content), choose the one "
            "that best describes the site's MAIN commercial purpose.\n"
            "3. If the content is in a language you don't recognize, still classify based on any identifiable "
            "signals (product names, URLs, prices, images described, etc.).\n"
            "4. Respond with 'undeterminated' ONLY if none of the above categories clearly applies "
            "(e.g., general news, e-commerce, software, food, travel, etc.).\n\n"

            "VERY IMPORTANT: Respond with the label ONLY — no explanations, no punctuation, no extra text.\n"
            "Valid responses: Adult Content | Gambling & Betting | Cryptocurrency Speculation | "
            "Supplement / Nutra | undeterminated\n\n"

            f"Web-page excerpt:\n\"\"\"\n{text[:5000]}\n\"\"\""
        )
        messages = [
            {"role": "system", "content": (
                "You are a compliance classification engine. "
                "You output exactly one label per request and nothing else."
            )},
            {"role": "user", "content": prompt},
        ]
        response = self.__openai_client.chat.completions.create(
            model=self._LLM_MODEL,
            temperature=self._LLM_TEMP,
            messages=messages,
        )
        raw   = response.choices[0].message.content.strip()
        label = re.sub(r"[^\w &/]", "", raw.splitlines()[0]).strip()
        return label if label in self._ALLOWED_GRAYMARKET_LABELS else "undeterminated"

    def llm_confirm_mfa(self, text: str, features: dict) -> str:
        """
        Verificador semántico LLM para confirmar o revertir la señal MFA.
        Solo se invoca cuando el scoring determinístico supera el umbral.
        Retorna 'mfa' o 'unknow'.
        """
        vendors_str    = ", ".join(features.get('vendors_detected', [])) or "none"
        evidence_str   = ", ".join(features.get('ad_class_snippets', [])[:10]) or "none"

        prompt = (
            "You are a strict MFA (Made-for-Advertising / Made-for-Arbitrage) site detector.\n"
            "Analyze the following signals extracted from a website and respond with EXACTLY one word: "
            "'mfa' or 'unknow'.\n\n"
            "A site is MFA when it combines: high ad density + arbitrage widgets (taboola/outbrain/etc.) "
            "OR journey manipulation + thin/repetitive content.\n"
            "If one or more of those pillars is missing or uncertain, respond 'unknow'.\n\n"
            "--- EXTRACTED SIGNALS ---\n"
            f"word_count: {features.get('word_count', 0)}\n"
            f"iframe_count: {features.get('iframe_count', 0)}\n"
            f"ins_count: {features.get('ins_count', 0)}\n"
            f"ad_like_nodes: {features.get('ad_like_nodes', 0)}\n"
            f"ad_to_text_ratio: {features.get('ad_to_text_ratio', 0):.4f}\n"
            f"iframe_per_1k_words: {features.get('iframe_per_1k_words', 0):.4f}\n"
            f"vendor_gpt: {features.get('vendor_gpt', False)}\n"
            f"vendor_hb: {features.get('vendor_hb', False)}\n"
            f"vendor_arbitrage: {features.get('vendor_arbitrage', False)}\n"
            f"vendors_detected: {vendors_str}\n"
            f"rec_widget_present: {features.get('rec_widget_present', False)}\n"
            f"rec_widget_count: {features.get('rec_widget_count', 0)}\n"
            f"sticky_or_anchor_ads: {features.get('sticky_or_anchor_ads', False)}\n"
            f"autoplay_video_ads: {features.get('autoplay_video_ads', False)}\n"
            f"overlay_interstitial: {features.get('overlay_interstitial', False)}\n"
            f"pagination_score: {features.get('pagination_score', 0):.2f}\n"
            f"repetition_score: {features.get('repetition_score', 0):.2f}\n"
            f"clickbait_title: {features.get('clickbait_title', False)}\n"
            f"text_to_html_ratio: {features.get('text_to_html_ratio', 0):.4f}\n"
            f"ad_class_snippets (sample): {evidence_str}\n"
            "--- PAGE EXCERPT ---\n"
            f"\"\"\"{text[:1500]}\"\"\""
            "\n\nRespond with EXACTLY one word: mfa or unknow."
        )
        messages = [
            {"role": "system", "content": "You are an MFA site detection engine."},
            {"role": "user",   "content": prompt},
        ]
        response = self.__openai_client.chat.completions.create(
            model=self._LLM_MODEL,
            temperature=self._LLM_TEMP,
            messages=messages,
        )
        raw   = response.choices[0].message.content.strip().lower()
        label = raw.splitlines()[0]
        label = re.sub(r"[^a-z]", "", label)
        return label if label in self._ALLOWED_MFA_LABELS else "unknow"



    # ------------------------------------------------------------------ #
    #  EXTRACCIÓN DE FEATURES MFA                                        #
    # ------------------------------------------------------------------ #

    def extract_mfa_features(self, html: str) -> dict:
        """
        Calcula todas las señales estructurales y de contenido para MFA.
        Retorna un dict con los features normalizados listos para scoring.
        """
        soup      = BeautifulSoup(html, "html.parser")
        full_text = self.extract_main_text(html)
        word_count = len(full_text.split())

        # ---- A) Conteos estructurales de ads -------------------------
        iframe_count = len(soup.find_all("iframe"))
        ins_count    = len(soup.find_all("ins"))
        amp_ad_count = len(soup.find_all("amp-ad"))

        ad_like_nodes = 0
        ad_class_snippets = []
        for tag in soup.find_all(["iframe", "div", "section", "ins", "script"]):
            if self.looks_like_ad(tag):
                ad_like_nodes += 1
                cls_id = f"{tag.get('id', '')} {' '.join(tag.get('class', []))}".strip()
                if cls_id and len(ad_class_snippets) < 20:
                    ad_class_snippets.append(cls_id[:80])

        # ---- B) Vendors de monetización (buscar en scripts inline) ---
        inline_scripts = " ".join(
            s.string or "" for s in soup.find_all("script") if s.string
        )
        src_attrs = " ".join(
            tag.get("src", "") for tag in soup.find_all(src=True)
        )
        combined_js = inline_scripts + " " + src_attrs

        vendor_gpt       = bool(self._VENDOR_GPT.search(combined_js))
        vendor_hb        = bool(self._VENDOR_HB.search(combined_js))
        vendor_arbitrage = bool(self._VENDOR_ARBITRAGE.search(combined_js))

        vendors_detected = []
        for name, pattern in [
            ('googletag/DFP', self._VENDOR_GPT),
            ('header-bidding', self._VENDOR_HB),
            ('arbitrage-widgets', self._VENDOR_ARBITRAGE),
        ]:
            if pattern.search(combined_js):
                vendors_detected.append(name)

        # ---- C) Formatos agresivos -----------------------------------
        autoplay_video_ads = False
        for video in soup.find_all("video"):
            if video.has_attr("autoplay") and video.has_attr("muted"):
                autoplay_video_ads = True
                break

        sticky_or_anchor_ads = False
        for tag in soup.find_all(True, style=True):
            style_val = tag.get("style", "").lower()
            cls_val   = " ".join(tag.get("class", [])).lower()
            combined_val = style_val + " " + cls_val
            if re.search(r'position\s*:\s*(fixed|sticky)', combined_val) or \
               re.search(r'\b(sticky|anchor|bottom)[\s-]?(ad|banner|bar)\b', combined_val):
                sticky_or_anchor_ads = True
                break

        overlay_interstitial = False
        for tag in soup.find_all(True, {"class": True}):
            cls_val = " ".join(tag.get("class", [])).lower()
            if re.search(r'\b(modal|overlay|interstitial|lightbox|popup)\b', cls_val):
                overlay_interstitial = True
                break

        # ---- D) Recommendation widgets (arbitraje) ------------------
        rec_widget_count = 0
        for tag in soup.find_all(True, {"class": True}):
            cls_val = " ".join(tag.get("class", [])).lower()
            if self._VENDOR_ARBITRAGE.search(cls_val):
                rec_widget_count += 1
                continue
        for tag in soup.find_all(["div", "section", "aside", "p"]):
            direct_text = " ".join(tag.find_all(string=True, recursive=False)).strip()
            if direct_text and self._REC_WIDGET_TEXT.search(direct_text):
                rec_widget_count += 1
        rec_widget_present = rec_widget_count > 0

        # ---- E) Journey manipulation / paginación -------------------
        pagination_hits = 0
        for a in soup.find_all("a", href=True):
            href    = a.get("href", "")
            anchor  = a.get_text(" ", strip=True)
            if self._PAGINATION_ANCHORS.search(anchor) or self._PAGINATION_URL.search(href):
                pagination_hits += 1
        rel_next = len(soup.find_all("link", rel=lambda r: isinstance(r, list) and 'next' in r))
        pagination_score = min(1.0, (pagination_hits + rel_next * 2) / 10.0)

        # ---- F) Thin content / text quality -------------------------
        text_to_html_ratio = len(full_text) / max(len(html), 1)

        paragraphs = [p.strip() for p in full_text.split("\n") if len(p.split()) >= 10]
        repetition_score = self._compute_repetition_score(paragraphs)

        # ---- G) Clickbait en título / H1 ----------------------------
        title_h1 = ""
        if soup.title:
            title_h1 += soup.title.get_text(" ", strip=True) + " "
        h1 = soup.find("h1")
        if h1:
            title_h1 += h1.get_text(" ", strip=True)
        clickbait_title = bool(self._CLICKBAIT.search(title_h1))

        # ---- H) Ratios normalizados ----------------------------------
        ad_to_text_ratio    = ad_like_nodes / max(word_count, 1)
        iframe_per_1k_words = iframe_count  / max(word_count / 1000, 0.1)

        return {
            'word_count':          word_count,
            'iframe_count':        iframe_count,
            'ins_count':           ins_count,
            'amp_ad_count':        amp_ad_count,
            'ad_like_nodes':       ad_like_nodes,
            'ad_class_snippets':   ad_class_snippets,
            'vendor_gpt':          vendor_gpt,
            'vendor_hb':           vendor_hb,
            'vendor_arbitrage':    vendor_arbitrage,
            'vendors_detected':    vendors_detected,
            'autoplay_video_ads':  autoplay_video_ads,
            'sticky_or_anchor_ads': sticky_or_anchor_ads,
            'overlay_interstitial': overlay_interstitial,
            'rec_widget_present':  rec_widget_present,
            'rec_widget_count':    rec_widget_count,
            'pagination_score':    pagination_score,
            'text_to_html_ratio':  text_to_html_ratio,
            'repetition_score':    repetition_score,
            'clickbait_title':     clickbait_title,
            'ad_to_text_ratio':    ad_to_text_ratio,
            'iframe_per_1k_words': iframe_per_1k_words,
        }

    def _compute_repetition_score(self, paragraphs: list) -> float:
        """
        Estima la repetición de contenido entre párrafos usando shingling (5-grams).
        Retorna el porcentaje de pares con similitud Jaccard > 0.85.
        """
        if len(paragraphs) < 2:
            return 0.0

        def shingle(text: str, n: int = 5) -> set:
            tokens = re.sub(r"\s+", " ", text.lower()).split()
            return {tuple(tokens[i:i+n]) for i in range(len(tokens) - n + 1)}

        shingles = [shingle(p) for p in paragraphs]
        similar_pairs = 0
        total_pairs   = 0
        for i in range(len(shingles)):
            for j in range(i + 1, len(shingles)):
                a, b = shingles[i], shingles[j]
                if not a or not b:
                    continue
                jaccard = len(a & b) / len(a | b)
                if jaccard > 0.85:
                    similar_pairs += 1
                total_pairs += 1

        return similar_pairs / max(total_pairs, 1)

    # ------------------------------------------------------------------ #
    #  SCORING DETERMINÍSTICO MFA                                        #
    # ------------------------------------------------------------------ #

    def compute_mfa_score(self, features: dict) -> int:
        """
        Calcula un score MFA de 0 a 100 basado en señales ponderadas.
        Umbral de clasificación: >= 65 → mfa candidato (confirmar con LLM).
        """
        score = 0

        # ---- Señales de anuncios (hasta 45) -------------------------
        if features['ad_like_nodes'] > 25:
            score += 15
        elif features['ad_like_nodes'] > 12:
            score += 7

        if features['iframe_count'] > 12:
            score += 10
        elif features['iframe_count'] > 6:
            score += 5

        if features['vendor_gpt'] or features['vendor_hb']:
            score += 10

        if features['sticky_or_anchor_ads'] or \
           features['autoplay_video_ads']  or \
           features['overlay_interstitial']:
            score += 10

        # ---- Arbitraje (hasta 25) -----------------------------------
        if features['rec_widget_present']:
            score += 20
            if features['rec_widget_count'] > 3:
                score += 5

        # ---- Contenido / journey (hasta 30) ------------------------
        thin_content = features['word_count'] < 400 and features['ad_like_nodes'] > 8
        if thin_content:
            score += 15
        elif features['word_count'] < 250 and features['ad_like_nodes'] > 0:
            score += 8

        if features['pagination_score'] > 0.5:
            score += 10
        elif features['pagination_score'] > 0.2:
            score += 4

        if features['repetition_score'] > 0.3:
            score += 5

        return min(score, 100)

    # ------------------------------------------------------------------ #
    #  PIPELINE PRINCIPAL DE CLASIFICACIÓN                               #
    # ------------------------------------------------------------------ #

    def process_html_to_graymarket(self, html: str, mfa_features: dict = None, mfa_score: int = None) -> str:
        """
        Pipeline de clasificación con dos etapas:

        1. Si el scoring MFA supera el umbral, usa llm_confirm_mfa() para
           confirmar → retorna 'mfa' o 'unknow'.
        2. Si no hay señal MFA fuerte, usa llm_classify_graymarket() para
           detectar las categorías gray-market existentes.
        """
        if mfa_features is None:
            mfa_features = self.extract_mfa_features(html)
        if mfa_score is None:
            mfa_score = self.compute_mfa_score(mfa_features)

        relevant_text = self.extract_relevant_text(html)

        if mfa_score >= self._MFA_THRESHOLD:
            llm_mfa = self.llm_confirm_mfa(relevant_text, mfa_features)
            if llm_mfa == 'mfa':
                return 'mfa'

        return self.llm_classify_graymarket(relevant_text)

    def update_secondary_domain(self, sec_domain_id, ad_count, has_affiliate_handoff, is_ecommerce, graymarket_label):
        sql_string = """
            UPDATE public.secondary_domains
            SET ad_count = %s,
                has_affiliate_handoff = %s,
                is_ecommerce = %s,
                graymarket_label = %s
            WHERE sec_domain_id = %s
        """
        data = (ad_count, has_affiliate_handoff, is_ecommerce, graymarket_label, sec_domain_id)
        conn = self._db_connect()
        cursor = conn.cursor()
        try:
            cursor.execute(sql_string, data)
            conn.commit()
        except Exception as e:
            self.__logger.error(
                f'::Saver:: Error updating status on secondary domains with id {sec_domain_id} - {e}')
        finally:
            cursor.close()
            conn.close()

    def update_secondary_domain_graymarket_label(self, sec_domain_id, graymarket_label):
        sql_string = """
            UPDATE public.secondary_domains
            SET graymarket_label = %s
            WHERE sec_domain_id = %s
        """
        data = (graymarket_label, sec_domain_id)
        conn = self._db_connect()
        cursor = conn.cursor()
        try:
            cursor.execute(sql_string, data)
            conn.commit()
        except Exception as e:
            self.__logger.error(
                f'::Saver:: Error updating status on secondary domains with id {sec_domain_id} - {e}')
        finally:
            cursor.close()
            conn.close()

