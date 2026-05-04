import logging
import os
import re
from collections.abc import Sequence

from psycopg2 import pool

from settings import DB_CONNECTION


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


DB_POOL_MIN_CONN = 1
DB_POOL_MAX_CONN = 5
BATCH_SIZE = min(int(os.getenv("BATCH_SIZE", "30")), 30)
TARGET_CLASS_ID = int(os.getenv("TARGET_CLASS_ID", "3"))
TARGET_PUBLICATION_STATUS = int(os.getenv("TARGET_PUBLICATION_STATUS", "0"))
START_AFTER_SEC_DOMAIN_ID = int(os.getenv("START_AFTER_SEC_DOMAIN_ID", "0"))
DOMAIN_PROCESS_LIMIT = int(os.getenv("DOMAIN_PROCESS_LIMIT", "0"))

SECONDARY_DOMAIN_TABLE = os.getenv("SECONDARY_DOMAIN_TABLE", "secondary_domains")
SECONDARY_DOMAIN_ID_COLUMN = os.getenv("SECONDARY_DOMAIN_ID_COLUMN", "sec_domain_id")
DOMAIN_ID_COLUMN = os.getenv("DOMAIN_ID_COLUMN", "domain_id")
CLASSIFICATION_COLUMN = os.getenv("CLASSIFICATION_COLUMN", "ml_sec_domain_classification")
PUBLICATION_STATUS_COLUMN = os.getenv("PUBLICATION_STATUS_COLUMN", "publication_status")

AD_CHAIN_URLS_TABLE = os.getenv("AD_CHAIN_URLS_TABLE", "ad_chain_urls")
AD_URL_FEATURES_TABLE = os.getenv("AD_URL_FEATURES_TABLE", "ad_url_features")
AD_URL_COLUMN = os.getenv("AD_URL_COLUMN", "ad_url")
AD_RENDERING_COLUMN = os.getenv("AD_RENDERING_COLUMN", "ad_rendering")
AD_CREATIVE_COLUMN = os.getenv("AD_CREATIVE_COLUMN", "ad_creative")

db_pool: pool.ThreadedConnectionPool | None = None


def safe_identifier(name: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


def init_db_pool():
    global db_pool
    if db_pool is None:
        db_pool = pool.ThreadedConnectionPool(
            DB_POOL_MIN_CONN,
            DB_POOL_MAX_CONN,
            DB_CONNECTION,
        )
        logger.info("Database connection pool initialized")


def close_db_pool():
    global db_pool
    if db_pool:
        db_pool.closeall()
        db_pool = None
        logger.info("Database connection pool closed")


def get_db_connection():
    global db_pool
    if db_pool is None:
        init_db_pool()
    return db_pool.getconn()


def release_db_connection(conn):
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)


def fetch_candidate_sec_domain_ids(last_seen_id: int, batch_size: int) -> list[int]:
    conn = None
    cursor = None
    sec_domain_ids: list[int] = []

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        table = safe_identifier(SECONDARY_DOMAIN_TABLE)
        sec_domain_id_col = safe_identifier(SECONDARY_DOMAIN_ID_COLUMN)
        class_col = safe_identifier(CLASSIFICATION_COLUMN)
        publication_status_col = safe_identifier(PUBLICATION_STATUS_COLUMN)

        sql_string = f"""
            SELECT {sec_domain_id_col}
            FROM {table}
            WHERE {publication_status_col} = %s
              AND {class_col} = %s
              AND {sec_domain_id_col} > %s
            ORDER BY {sec_domain_id_col} ASC
            LIMIT %s
        """

        cursor.execute(
            sql_string,
            (TARGET_PUBLICATION_STATUS, TARGET_CLASS_ID, last_seen_id, batch_size),
        )
        rows = cursor.fetchall()
        sec_domain_ids = [int(row[0]) for row in rows]

    except Exception as e:
        logger.error(f"Error fetching candidate secondary domains after id {last_seen_id}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)

    return sec_domain_ids


def find_sec_domain_ids_with_invalid_ad_features(sec_domain_ids: Sequence[int]) -> list[int]:
    if not sec_domain_ids:
        return []

    conn = None
    cursor = None
    matched_ids: list[int] = []

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        secondary_domains_table = safe_identifier(SECONDARY_DOMAIN_TABLE)
        sec_domain_id_col = safe_identifier(SECONDARY_DOMAIN_ID_COLUMN)
        domain_id_col = safe_identifier(DOMAIN_ID_COLUMN)
        ad_chain_urls_table = safe_identifier(AD_CHAIN_URLS_TABLE)
        ad_url_features_table = safe_identifier(AD_URL_FEATURES_TABLE)
        ad_url_col = safe_identifier(AD_URL_COLUMN)
        ad_rendering_col = safe_identifier(AD_RENDERING_COLUMN)
        ad_creative_col = safe_identifier(AD_CREATIVE_COLUMN)

        sql_string = f"""
            SELECT DISTINCT sd.{sec_domain_id_col}
            FROM {secondary_domains_table} sd
            INNER JOIN {ad_chain_urls_table} acu
                ON sd.{domain_id_col} = acu.{domain_id_col}
            INNER JOIN {ad_url_features_table} auf
                ON acu.{ad_url_col} = auf.{ad_url_col}
            WHERE sd.{sec_domain_id_col} = ANY(%s)
              AND (
                    COALESCE(auf.{ad_rendering_col}, FALSE) = FALSE
                 OR COALESCE(auf.{ad_creative_col}, FALSE) = FALSE
              )
            ORDER BY sd.{sec_domain_id_col} ASC
        """

        cursor.execute(sql_string, (list(sec_domain_ids),))
        rows = cursor.fetchall()
        matched_ids = [int(row[0]) for row in rows]

    except Exception as e:
        logger.error(f"Error validating ad-chain features for sec_domain_ids {list(sec_domain_ids)}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)

    return matched_ids


def clear_secondary_domain_classification(sec_domain_ids: Sequence[int]) -> int:
    if not sec_domain_ids:
        return 0

    conn = None
    cursor = None
    updated_rows = 0

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        table = safe_identifier(SECONDARY_DOMAIN_TABLE)
        sec_domain_id_col = safe_identifier(SECONDARY_DOMAIN_ID_COLUMN)
        class_col = safe_identifier(CLASSIFICATION_COLUMN)
        publication_status_col = safe_identifier(PUBLICATION_STATUS_COLUMN)

        sql_string = f"""
            UPDATE {table}
            SET {class_col} = NULL
            WHERE {sec_domain_id_col} = ANY(%s)
              AND {publication_status_col} = %s
              AND {class_col} = %s
        """

        cursor.execute(
            sql_string,
            (list(sec_domain_ids), TARGET_PUBLICATION_STATUS, TARGET_CLASS_ID),
        )
        updated_rows = cursor.rowcount
        conn.commit()

    except Exception as e:
        logger.error(f"Error clearing class for sec_domain_ids {list(sec_domain_ids)}: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)

    return updated_rows


def main():
    logger.info("Starting ad-chain feature updater")
    logger.info(
        "Configuration: batch_size=%s target_class_id=%s publication_status=%s start_after_sec_domain_id=%s domain_process_limit=%s",
        BATCH_SIZE,
        TARGET_CLASS_ID,
        TARGET_PUBLICATION_STATUS,
        START_AFTER_SEC_DOMAIN_ID,
        DOMAIN_PROCESS_LIMIT,
    )

    scanned_count = 0
    matched_count = 0
    updated_count = 0
    batch_count = 0
    last_seen_id = START_AFTER_SEC_DOMAIN_ID

    init_db_pool()

    try:
        while True:
            remaining = DOMAIN_PROCESS_LIMIT - scanned_count if DOMAIN_PROCESS_LIMIT > 0 else BATCH_SIZE
            current_batch_size = min(BATCH_SIZE, remaining) if DOMAIN_PROCESS_LIMIT > 0 else BATCH_SIZE

            if current_batch_size <= 0:
                logger.info("DOMAIN_PROCESS_LIMIT reached, stopping execution")
                break

            candidate_ids = fetch_candidate_sec_domain_ids(last_seen_id, current_batch_size)
            if not candidate_ids:
                logger.info("No more candidate secondary domains found")
                break

            batch_count += 1
            scanned_count += len(candidate_ids)
            last_seen_id = candidate_ids[-1]

            ids_to_clear = find_sec_domain_ids_with_invalid_ad_features(candidate_ids)
            matched_count += len(ids_to_clear)

            updated_rows = clear_secondary_domain_classification(ids_to_clear)
            updated_count += updated_rows

            logger.info(
                "Batch %s processed: scanned=%s matched_invalid_features=%s updated=%s last_sec_domain_id=%s",
                batch_count,
                len(candidate_ids),
                len(ids_to_clear),
                updated_rows,
                last_seen_id,
            )

        logger.info("=" * 60)
        logger.info("EXECUTION SUMMARY")
        logger.info("Batches processed: %s", batch_count)
        logger.info("Secondary domains scanned: %s", scanned_count)
        logger.info("Secondary domains matched: %s", matched_count)
        logger.info("Secondary domains updated: %s", updated_count)
        logger.info("=" * 60)
    finally:
        close_db_pool()


if __name__ == "__main__":
    main()
