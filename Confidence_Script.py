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
                    sd.ml_sec_domain_classification
                FROM secondary_domains sd
                WHERE sd.publication_status = 0
                  AND sd.sec_domain_source IS NOT NULL
                  AND sd.ml_sec_domain_classification IS NOT NULL
            """, conn)

        def process_domains(row):
            source = row["sec_domain_source"]
            cls = row["ml_sec_domain_classification"]

            if source == 'SimilarWeb':
                if cls == 2:  # Referral cloaking
                    return 'HIGH', 1, 1
                if cls == 3:  # MFA
                    return 'MEDIUM', 2, 4

            elif source == 'Ad Sniffer':
                if cls == 2:  # Referral cloaking
                    return 'MEDIUM', 2, 2
                if cls == 3:  # MFA
                    return 'LOW', 5, 3

            return None, None, None

        result = sec_domain.apply(process_domains, axis=1, result_type='expand')
        result.columns = ['confidence', 'recommended_action_id', 'justification']

        sec_domain = pd.concat([sec_domain, result], axis=1)
        sec_domain = sec_domain.dropna(subset=['confidence'])

        df_filtered = sec_domain[
            ['sec_domain_id', 'confidence', 'recommended_action_id', 'justification']
        ].copy()

        data_to_save = df_filtered.to_dict('records')
        self.update_domains(data_to_save)

    def update_domains(self, save_data):
        """
        Actualiza masivamente secondary_domains usando:
        - sec_domain_id
        - confidence (varchar)
        - recommended_action_id (int2)
        - justification (int2)
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

            data_to_update = [
                (
                    int(row['sec_domain_id']) if row['sec_domain_id'] is not None else None,
                    str(row['confidence']) if row['confidence'] is not None else None,
                    int(row['recommended_action_id']) if row['recommended_action_id'] not in (None, '') else None,
                    int(row['justification']) if row['justification'] not in (None, '') else None,
                )
                for row in save_data
            ]

            values_template = ",".join(["(%s, %s, %s, %s)"] * len(data_to_update))
            flat_values = [value for row in data_to_update for value in row]

            sql = f"""
                WITH updates AS (
                    SELECT
                        v.sec_domain_id::bigint AS sec_domain_id,
                        v.confidence::varchar AS confidence,
                        v.recommended_action_id::int2 AS recommended_action_id,
                        v.justification::int2 AS justification
                    FROM (
                        VALUES {values_template}
                    ) AS v(sec_domain_id, confidence, recommended_action_id, justification)
                )
                UPDATE public.secondary_domains AS t
                SET confidence = u.confidence,
                    recommended_action_id = u.recommended_action_id,
                    justification = u.justification
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