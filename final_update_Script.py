import logging
import psycopg2

from settings import db_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def update_secondary_domains_from_invalid_html():
    query = """
        UPDATE secondary_domains sd
        SET ml_sec_domain_classification = 11,
        decision_source = 'sql script'
        WHERE sd.sec_domain_media_type_id = 17
          AND (
                sd.ml_sec_domain_classification IS NULL
                OR sd.ml_sec_domain_classification = 9
              )
    """

    try:
        with psycopg2.connect(**db_connect) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                filas_actualizadas = cursor.rowcount

        logging.info("Filas actualizadas: %s", filas_actualizadas)
        return filas_actualizadas

    except Exception as e:
        logging.exception("Error al ejecutar el update: %s", e)
        raise

def update_secondary_domains_publishing_sites():
    query = """
        UPDATE secondary_domains sd
        SET ml_sec_domain_classification = 10,
        decision_source = 'sql script'
        WHERE sd.sec_domain_media_type_id = 5
          AND ml_sec_domain_classification != 10
    """

    try:
        with psycopg2.connect(**db_connect) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                filas_actualizadas = cursor.rowcount

        logging.info("Filas actualizadas: %s", filas_actualizadas)
        return filas_actualizadas

    except Exception as e:
        logging.exception("Error al ejecutar el update: %s", e)
        raise