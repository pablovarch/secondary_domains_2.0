from dependencies import log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
from sqlalchemy import create_engine


# Para clasificar dominios de Betting y excluir sitios con piracy brand

class ConfidenceScript:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='Block_class.log')

    def main(self):
        self.__logger.info('-- starting ConfidenceScript')

        alchemyEngine = create_engine(
            db_connect_df,
            pool_recycle=3600,
            pool_pre_ping=True
        )

        with alchemyEngine.connect() as conn:
            sec_domain = pd.read_sql("""
                SELECT
                    sd.sec_domain_id,
                    sd.sec_domain_source,
                    sd.ml_sec_domain_classification,
                    sd.confidence,
                    sd.recommended_action_id,
                    sd.justification,
                    sd.exploit_type
                FROM secondary_domains sd
                WHERE sd.publication_status = 0
                  AND sd.sec_domain_source IN (
                      'SimilarWeb',
                      'Ad Sniffer',
                      'Domain Telemetry'
                  )
                  AND (
                      (
                          sd.sec_domain_source IN ('SimilarWeb', 'Ad Sniffer')
                          AND sd.ml_sec_domain_classification IN (2, 3)
                      )
                      OR (
                          sd.sec_domain_source = 'Domain Telemetry'
                          AND sd.ml_sec_domain_classification = 2
                      )
                      OR sd.confidence IS NOT NULL
                      OR sd.recommended_action_id IS NOT NULL
                      OR sd.justification IS NOT NULL
                      OR sd.exploit_type IS NOT NULL
                  )
            """, conn)

        if sec_domain.empty:
            self.__logger.info('No domains require confidence reconciliation')
            return

        def process_domains(row):
            source = row["sec_domain_source"]
            cls = row["ml_sec_domain_classification"]

            if source == 'SimilarWeb':
                if cls == 2:  # Referral cloaking
                    return 'HIGH', 1, 1, ['Referral Cloaking']
                if cls == 3:  # MFA
                    return 'MEDIUM', 2, 4, ['Made for Arbitrage']

            elif source == 'Ad Sniffer':
                if cls == 2:  # Referral cloaking
                    return 'MEDIUM', 2, 2, ['Referral Cloaking']
                if cls == 3:  # MFA
                    return 'LOW', 5, 3, ['Made for Arbitrage']

            elif source == 'Domain Telemetry' and cls == 2:
                return 'LOW', 3, 6, ['Referral Cloaking']

            return None, None, None, None

        result = sec_domain.apply(process_domains, axis=1, result_type='expand')
        result.columns = [
            'expected_confidence',
            'expected_recommended_action_id',
            'expected_justification',
            'expected_exploit_type'
        ]

        sec_domain = pd.concat([sec_domain, result], axis=1)

        df_filtered = sec_domain[
            [
                'sec_domain_id',
                'expected_confidence',
                'expected_recommended_action_id',
                'expected_justification',
                'expected_exploit_type'
            ]
        ].copy()
        df_filtered.columns = [
            'sec_domain_id',
            'confidence',
            'recommended_action_id',
            'justification',
            'exploit_type'
        ]

        data_to_save = df_filtered.to_dict('records')
        self.update_domains(data_to_save)

    def update_domains(self, save_data):
        """
        Actualiza masivamente secondary_domains usando:
        - sec_domain_id
        - confidence (varchar)
        - recommended_action_id (int2)
        - justification (int2)
        - exploit_type (varchar[])
        """
        if not save_data:
            print("No data to update.")
            return

        conn = None
        cursor = None

        try:
            conn = psycopg2.connect(
                host=db_connect['host'],
                database=db_connect['database'],
                password=db_connect['password'],
                user=db_connect['user'],
                port=db_connect['port']
            )
            print('DB connection opened')

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
                    int(row['sec_domain_id']) if row['sec_domain_id'] is not None else None,
                    clean_optional(row['confidence'], str),
                    clean_optional(row['recommended_action_id'], int),
                    clean_optional(row['justification'], int),
                    clean_optional(row['exploit_type']),
                )
                for row in save_data
            ]

            values_template = ",".join(["(%s, %s, %s, %s, %s)"] * len(data_to_update))
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
                FROM updates u
                WHERE t.sec_domain_id = u.sec_domain_id;
            """

            cursor.execute(sql, flat_values)
            conn.commit()
            print(f'{len(data_to_update)} domains updated using CTE VALUES method.')

        except Exception as e:
            print(f'Error during CTE batch update: {e}')
            if conn:
                conn.rollback()
            raise

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                print('DB connection closed')
