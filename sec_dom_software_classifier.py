import asyncio
import logging
import os
import re

from psycopg2 import pool
from pydantic import BaseModel
from openai import AsyncOpenAI, RateLimitError, APIConnectionError, APIStatusError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from settings import DB_CONNECTION, openia_apikey
from dotenv import load_dotenv
from typing import Any, Literal

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
MAX_HTML_CHARS = 80000
MAX_CONCURRENT_REQUESTS = 5
DB_POOL_MIN_CONN = 1
DB_POOL_MAX_CONN = 10

MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-5.1-2025-11-13")
REASONING_EFFORT = os.getenv("OPENAI_REASONING_EFFORT", "none")
DOMAIN_PROCESS_LIMIT = int(os.getenv("DOMAIN_PROCESS_LIMIT", "1000"))

SECONDARY_DOMAIN_TABLE = os.getenv("SECONDARY_DOMAIN_TABLE", "secondary_domains")
SECONDARY_DOMAIN_HTML_TABLE = os.getenv("SECONDARY_DOMAIN_HTML_TABLE", "secondary_domains_html")
DOMAIN_ID_COLUMN = os.getenv("DOMAIN_ID_COLUMN", "sec_domain_id")
MEDIA_TYPE_COLUMN = os.getenv("MEDIA_TYPE_COLUMN", "sec_domain_media_type_id")
CLASSIFICATION_COLUMN = os.getenv("CLASSIFICATION_COLUMN", "ml_sec_domain_classification")

SOFTWARE_MEDIA_TYPE_ID = int(os.getenv("SOFTWARE_MEDIA_TYPE_ID", "7"))

# Database connection pool (initialized lazily)
db_pool: pool.ThreadedConnectionPool | None = None


class SoftwareSubtypeResponse(BaseModel):
    label_id: Literal[9, 12]


SOFTWARE_SUBTYPE_CLASSIFICATION_PROMPT = '''You are a web domain classifier specialized in SOFTWARE sites.

The domain has already been identified as a SOFTWARE site (media_type = "Software").

Your task is to analyze the HTML content of the site and determine whether it belongs to one of these two specific subtypes:

---

ID 12 = Chrome Extension or VPN site

Assign ID 12 when the site's PRIMARY purpose is to offer, promote, or distribute:
- A browser extension (Chrome extension, Firefox add-on, Edge add-on, or any browser plugin).
- A VPN service or VPN application (Virtual Private Network), including free or paid VPN tools, VPN clients, VPN apps for any platform.

Strong signals for ID 12:
- Page title, headings, or description explicitly mention "extension", "add-on", "plugin", "Chrome Web Store", "VPN", "Virtual Private Network", "proxy", "secure browsing", "hide your IP", "anonymous browsing".
- Call-to-action buttons like "Add to Chrome", "Install Extension", "Get VPN", "Download VPN", "Try for free" referencing a VPN.
- Screenshots or feature lists describing browser toolbar integration or VPN tunnel functionality.
- Landing page is clearly a product page for a single extension or VPN service.

---

ID 9 = Generic Software site (not Chrome Extension / VPN)

Assign ID 9 when the site is a software-related site that does NOT primarily offer a browser extension or VPN. This includes:
- Sites offering desktop applications, mobile apps, utilities, games, productivity tools, development tools, etc.
- Software download portals (pirated or legitimate) that are NOT specifically focused on extensions or VPNs.
- Software review sites, comparison sites, or news sites about software.
- Sites offering cracked software, keygens, patches, or serial keys (not related to extensions/VPNs).
- Any other software-related site that does not match the Chrome Extension / VPN criteria above.

---

DECISION RULE:

Ask yourself: "Is the MAIN product or service on this site a browser extension or a VPN?"
- YES → label_id = 12
- NO  → label_id = 9

If the site mentions VPN or extensions only as a side feature among many other software products, assign ID 9.
Only assign ID 12 when the extension or VPN is clearly the primary offering of the site.

---

OUTPUT REQUIREMENTS

You MUST respond using the SoftwareSubtypeResponse schema, setting `label_id` to EXACTLY ONE of: 9 or 12.
Do NOT include explanations, text, or additional fields.
'''


def is_html_too_short(text: str | None) -> bool:
    """Returns True if the preprocessed text is too short to classify reliably."""
    try:
        if not text:
            return True
        if len(text.split()) < 4:
            return True
        return False
    except Exception as e:
        logger.error(f"Error on is_html_too_short: {e}")
        return True


def safe_identifier(name: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


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


def should_skip_unreadable_page(prepared_text: str, raw_html: str) -> bool:
    """Returns True for parked, blocked, or otherwise unclassifiable pages."""
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

    blocked_patterns = [
        r"captcha",
        r"verify\s+you\s+are\s+human",
        r"attention\s+required",
        r"cloudflare",
        r"ddos\s+protection",
        r"access\s+denied",
        r"forbidden",
        r"error\s*403",
        r"temporarily\s+unavailable",
        r"unusual\s+traffic",
    ]

    for pat in parking_patterns:
        if re.search(pat, hay) or re.search(pat, raw):
            return True

    for pat in blocked_patterns:
        if re.search(pat, hay) or re.search(pat, raw):
            return True

    return False


def prepare_html_for_llm(raw_snapshot: str) -> str | None:
    if not raw_snapshot:
        return None
    extracted = extract_html_from_mhtml(raw_snapshot)
    extracted = remove_large_data_uris(extracted)
    return preprocess_html(extracted)


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


def get_software_domains_pending_subtype() -> list[int]:
    """
    Returns sec_domain_ids for Software domains (media_type_id=7) where
    ml_sec_domain_classification is NULL or 9 (pending subtype classification).
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
        classification_col = safe_identifier(CLASSIFICATION_COLUMN)

        sql_string = f"""
            SELECT sd.{domain_id_col}
            FROM {table} sd
            WHERE sd.{media_type_col} = %s
              AND (sd.{classification_col} IS NULL OR sd.{classification_col} = 9)
            ORDER BY sd.{domain_id_col} DESC
            LIMIT %s
        """

        cursor.execute(sql_string, (SOFTWARE_MEDIA_TYPE_ID, DOMAIN_PROCESS_LIMIT))
        results = cursor.fetchall()

        if results:
            domain_ids = [row[0] for row in results]
            logger.info(f"Found {len(domain_ids)} software domains pending subtype classification")
        else:
            logger.info("No software domains found to process")

    except Exception as e:
        logger.error(f"Error fetching software domains: {e}")
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)

    return domain_ids


def get_domain_html(sec_domain_id: int) -> str | None:
    """
    Fetches the most recent HTML snapshot for a given sec_domain_id
    from the secondary_domains_html table.
    """
    conn = None
    cursor = None
    html_content: str | None = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        table = safe_identifier(SECONDARY_DOMAIN_HTML_TABLE)
        sql_string = f"""
            SELECT html_content
            FROM {table}
            WHERE sec_domain_id = %s
            ORDER BY sec_domain_html_id DESC
            LIMIT 1
        """

        cursor.execute(sql_string, (sec_domain_id,))
        result = cursor.fetchone()

        if result:
            html_content = result[0]

    except Exception as e:
        logger.error(f"Error fetching HTML for sec_domain_id {sec_domain_id}: {e}")
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)

    return html_content


def update_software_subtype_label(sec_domain_id: int, label_id: int) -> bool:
    """
    Writes the software subtype label (9 or 12) into ml_sec_domain_classification
    for the given sec_domain_id.
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
        classification_col = safe_identifier(CLASSIFICATION_COLUMN)
        sql_string = f"""
            UPDATE {table}
            SET {classification_col} = %s
            WHERE {domain_id_col} = %s
        """

        cursor.execute(sql_string, (label_id, sec_domain_id))
        conn.commit()
        success = True

    except Exception as e:
        logger.error(f"Error updating subtype label for sec_domain_id {sec_domain_id}: {e}")
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
async def classify_software_subtype(
        client: AsyncOpenAI,
        raw_html: str,
        prepared_text: str | None,
        sec_domain_id: int
) -> int:
    """
    Classifies a Software domain into subtype label 9 (generic software) or
    12 (Chrome Extension / VPN) using OpenAI structured output.
    Fallback on any error: returns 9.
    """
    model = MODEL_NAME

    try:
        prepared = (prepared_text or prepare_html_for_llm(raw_html) or "")

        if is_html_too_short(prepared):
            logger.info(
                f"sec_domain_id {sec_domain_id}: insufficient content after preprocessing, defaulting to 9"
            )
            return 9

        if should_skip_unreadable_page(prepared, raw_html):
            logger.info(
                f"sec_domain_id {sec_domain_id}: unreadable/parked/blocked page, defaulting to 9"
            )
            return 9

        user_content = (
            "Classify the following Software domain into subtype 9 or 12.\n\n"
            "Below is extracted and cleaned page text from the site snapshot (possibly truncated):\n\n"
            "```text\n"
            f"{prepared}\n"
            "```\n\n"
            "Use the system instructions to set the label_id in the SoftwareSubtypeResponse schema."
        )

        messages = [
            {
                "role": "system",
                "content": SOFTWARE_SUBTYPE_CLASSIFICATION_PROMPT,
            },
            {
                "role": "user",
                "content": user_content,
            },
        ]

        common_kwargs = {
            "model": model,
            "messages": messages,
            "response_format": SoftwareSubtypeResponse,
            "temperature": 0,
            "max_completion_tokens": 64,
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
                f"sec_domain_id {sec_domain_id}: empty parsed result, defaulting to 9"
            )
            return 9

        label_id = int(result.label_id)

        if label_id in {9, 12}:
            logger.info(f"sec_domain_id {sec_domain_id}: classified as label_id {label_id}")
            return label_id

        logger.warning(
            f"sec_domain_id {sec_domain_id}: unexpected label_id {label_id}, defaulting to 9"
        )
        return 9

    except APIStatusError as e:
        logger.error(
            f"OpenAI API error for sec_domain_id {sec_domain_id}: "
            f"{e.status_code} - {e.message}"
        )
        raise
    except Exception as e:
        logger.error(
            f"Error classifying sec_domain_id {sec_domain_id}: {e}. Defaulting to 9"
        )
        return 9


async def process_software_domain(
        client: AsyncOpenAI,
        sec_domain_id: int,
        semaphore: asyncio.Semaphore,
) -> tuple[int, str]:
    """
    Processes a single Software domain: fetches HTML, classifies subtype,
    and writes the result to ml_sec_domain_classification.
    Returns (sec_domain_id, status) where status is 'processed', 'skipped', or 'error'.
    """
    async with semaphore:
        try:
            html_content = get_domain_html(sec_domain_id)

            if not html_content:
                logger.warning(f"sec_domain_id {sec_domain_id}: no HTML found, skipping")
                return (sec_domain_id, 'skipped')

            prepared_text = prepare_html_for_llm(html_content)

            if is_html_too_short(prepared_text):
                logger.info(
                    f"sec_domain_id {sec_domain_id}: invalid content after preprocessing, writing label 9"
                )
                update_software_subtype_label(sec_domain_id, 9)
                return (sec_domain_id, 'processed')

            if prepared_text is not None and should_skip_unreadable_page(prepared_text, html_content):
                logger.info(
                    f"sec_domain_id {sec_domain_id}: unreadable page detected, writing label 9"
                )
                update_software_subtype_label(sec_domain_id, 9)
                return (sec_domain_id, 'processed')

            label_id = await classify_software_subtype(
                client=client,
                raw_html=html_content,
                prepared_text=prepared_text,
                sec_domain_id=sec_domain_id,
            )

            success = update_software_subtype_label(sec_domain_id, label_id)

            if success:
                logger.info(
                    f"sec_domain_id {sec_domain_id}: written label_id {label_id} to {CLASSIFICATION_COLUMN}"
                )
                return (sec_domain_id, 'processed')
            else:
                return (sec_domain_id, 'error')

        except Exception as e:
            logger.error(f"Error processing sec_domain_id {sec_domain_id}: {e}")
            return (sec_domain_id, 'error')


async def main():
    """Main async entry point: classifies Software domains into subtype 9 or 12."""
    logger.info("Starting Software subtype classification (Chrome Extension / VPN detector)")

    api_key = OPENAI_APIKEY or os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        raise ValueError("Missing OpenAI API key. Set OPENAI_API_KEY env var or configure settings.")
    client = AsyncOpenAI(api_key=api_key)

    init_db_pool()

    try:
        domain_ids = get_software_domains_pending_subtype()

        if not domain_ids:
            logger.info("No domains to process. Exiting.")
            return

        logger.info(
            f"Processing {len(domain_ids)} domains with {MAX_CONCURRENT_REQUESTS} concurrent requests"
        )

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        tasks = [
            process_software_domain(client, sec_domain_id, semaphore)
            for sec_domain_id in domain_ids
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

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

        logger.info("=" * 60)
        logger.info("EXECUTION SUMMARY")
        logger.info(f"Total domains: {len(domain_ids)}")
        logger.info(f"Processed: {processed_count}")
        logger.info(f"Skipped (no HTML): {skipped_count}")
        logger.info(f"Errors: {error_count}")
        logger.info("=" * 60)
        logger.info("Ending execution")

    finally:
        close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
