import logging
import psycopg2

from settings import db_connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def update_secondary_domains_mfa_no_ads():
    query = """
        UPDATE secondary_domains sd
        SET ml_sec_domain_classification = Null,
        decision_source = 'mfa_no_ads'
        WHERE 
          (sd.ad_count =  0 or sd.ad_count is null)
          AND ml_sec_domain_classification = 3
    """

    try:
        with psycopg2.connect(**db_connect) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                filas_actualizadas = cursor.rowcount

        logging.info("Filas actualizadas (publishing_sites): %s", filas_actualizadas)
        return filas_actualizadas

    except Exception as e:
        logging.exception("Error al ejecutar el update (mfa_no_ads): %s", e)
        raise


def main():
    logging.info("=== Inicio del proceso de actualización ===")

    logging.info("--- Ejecutando: update_secondary_domains_mfa_no_ads ---")
    filas_mfa_no_ads = update_secondary_domains_mfa_no_ads()



    logging.info("=== Proceso finalizado. Total filas actualizadas: %s ===", filas_mfa_no_ads)


if __name__ == "__main__":
    main()