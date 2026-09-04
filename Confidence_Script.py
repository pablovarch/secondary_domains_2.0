from dependencies import log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

from classification_metadata import expected_metadata


class ConfidenceScript:
    """Reconcile classification metadata with the documented source matrix."""

    def __init__(self):
        self.__logger = log.Log().get_logger(name='ConfidenceScript.log')

    def main(self):
        self.__logger.info('-- starting classification metadata reconciliation')

        alchemy_engine = create_engine(
            db_connect_df,
            pool_recycle=3600,
            pool_pre_ping=True,
        )

        with alchemy_engine.connect() as conn:
            sec_domain = pd.read_sql("""
                SELECT
                    sd.sec_domain_id,
                    sd.sec_domain_source,
                    sd.decision_source,
                    sd.ml_sec_domain_classification,
                    sd.confidence,
                    sd.recommended_action_id,
                    sd.justification,
                    sd.exploit_type
                FROM public.secondary_domains AS sd
                WHERE sd.publication_status IS DISTINCT FROM 2
                  AND (
                      (LOWER(BTRIM(COALESCE(sd.sec_domain_source, ''))),
                       LOWER(BTRIM(COALESCE(sd.decision_source, ''))),
                       sd.ml_sec_domain_classification) IN (
                          ('similarweb', 'similarweb', 2),
                          ('similarweb', 'similarweb', 3),
                          ('ad sniffer', 'similarweb', 2),
                          ('ad sniffer', 'similarweb', 3),
                          ('ad sniffer', 'ad sniffer', 2),
                          ('ad sniffer', 'ad sniffer', 3),
                          ('domain telemetry', 'domain telemetry', 2)
                      )
                      OR sd.confidence IS NOT NULL
                      OR sd.recommended_action_id IS NOT NULL
                      OR sd.justification IS NOT NULL
                      OR sd.exploit_type IS NOT NULL
                  )
            """, conn)

        if sec_domain.empty:
            self.__logger.info('No domains require metadata reconciliation')
            return

        result = sec_domain.apply(
            lambda row: expected_metadata(
                row['sec_domain_source'],
                row['decision_source'],
                row['ml_sec_domain_classification'],
            ),
            axis=1,
            result_type='expand',
        )
        result.columns = [
            'confidence',
            'recommended_action_id',
            'justification',
            'exploit_type',
        ]

        data_to_save = pd.concat(
            [sec_domain[['sec_domain_id']], result], axis=1,
        ).to_dict('records')
        self.update_domains(data_to_save)

    def update_domains(self, save_data):
        """Persist the metadata policy in one batch update."""
        if not save_data:
            self.__logger.info('No metadata updates to persist')
            return

        conn = None
        cursor = None

        try:
            conn = psycopg2.connect(
                host=db_connect['host'],
                database=db_connect['database'],
                password=db_connect['password'],
                user=db_connect['user'],
                port=db_connect['port'],
            )
            cursor = conn.cursor()

            def clean_optional(value, converter=None):
                if value is None:
                    return None
                try:
                    if pd.isna(value):
                        return None
                except (TypeError, ValueError):
                    pass
                return converter(value) if converter else value

            data_to_update = [
                (
                    int(row['sec_domain_id']),
                    clean_optional(row['confidence'], str),
                    clean_optional(row['recommended_action_id'], int),
                    clean_optional(row['justification'], int),
                    clean_optional(row['exploit_type']),
                )
                for row in save_data
            ]

            values_template = ','.join(['(%s, %s, %s, %s, %s)'] * len(data_to_update))
            flat_values = [value for row in data_to_update for value in row]

            sql = f"""
                WITH updates AS (
                    SELECT
                        v.sec_domain_id::bigint AS sec_domain_id,
                        v.confidence::varchar AS confidence,
                        v.recommended_action_id::int2 AS recommended_action_id,
                        v.justification::int2 AS justification,
                        v.exploit_type::varchar[] AS exploit_type
                    FROM (
                        VALUES {values_template}
                    ) AS v(
                        sec_domain_id,
                        confidence,
                        recommended_action_id,
                        justification,
                        exploit_type
                    )
                )
                UPDATE public.secondary_domains AS t
                SET confidence = u.confidence,
                    recommended_action_id = u.recommended_action_id,
                    justification = u.justification,
                    exploit_type = u.exploit_type
                FROM updates AS u
                WHERE t.sec_domain_id = u.sec_domain_id
                  AND t.publication_status IS DISTINCT FROM 2;
            """

            cursor.execute(sql, flat_values)
            conn.commit()
            self.__logger.info('%s domains reconciled', len(data_to_update))

        except Exception:
            if conn:
                conn.rollback()
            self.__logger.exception('Error reconciling classification metadata')
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
