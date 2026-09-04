from dependencies import log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

from classification_metadata import expected_metadata


# Para Clasificar dominios de Betting y excluir sitios con piracy brand

class Jarm_processing:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='jarm_rules.log')

    def main(self):
        self.__logger.info('-- starting Jarm rules classifier')

        alchemyEngine = create_engine(
            db_connect_df,  # ej: "postgresql+psycopg2://user:pass@host:5432/dbname"
            pool_recycle=3600,
            pool_pre_ping=True  # robustez ante conexiones caídas
        )
        # Correcting the syntax error by removing the invalid 'DB Connection' line
        with alchemyEngine.connect() as conn:
            sec_domain = pd.read_sql(""" select sec_domain_id,sec_domain, exc_domain_id, google_search_results, online_status, redirect_domain from secondary_domains 
            where ml_sec_domain_classification is null
            and sec_domain_source = 'Domain Telemetry'
            and google_search_results is not null
            -- and online_status is not null""", conn)

        if sec_domain.empty:
            self.__logger.info('No unclassified Domain Telemetry records to process')
            return

        def proccess_domains(row):
            if row["google_search_results"] <= 2:
                return 2
            if row['google_search_results'] > 2 and row['google_search_results'] < 50:
                return 3
            if row['google_search_results'] >= 50:
                return 4

        sec_domain['ml_sec_domain_classification'] = sec_domain.apply(proccess_domains, axis=1)

        sec_domain = sec_domain.dropna(subset=['ml_sec_domain_classification'])
        metadata = sec_domain['ml_sec_domain_classification'].apply(
            lambda classification: expected_metadata(
                'Domain Telemetry',
                'Domain Telemetry',
                classification,
            )
        )
        sec_domain[
            ['confidence', 'recommended_action_id', 'justification', 'exploit_type']
        ] = pd.DataFrame(metadata.tolist(), index=sec_domain.index)
        sec_domain['decision_source'] = 'Domain Telemetry'

        df_filtered = sec_domain[
            [
                'sec_domain_id',
                'ml_sec_domain_classification',
                'confidence',
                'recommended_action_id',
                'justification',
                'exploit_type',
                'decision_source',
            ]
        ]
        data_to_save = df_filtered.to_dict('records')
        self.update_domains(data_to_save)

    def update_domains(self, save_data):
        """
        Efficiently updates domain data using a CTE VALUES block (no temp table needed).
        """
        if not save_data:
            self.__logger.info('No domains matched JARM rules')
            return

        try:
            conn = psycopg2.connect(host=db_connect['host'],
                                    database=db_connect['database'],
                                    password=db_connect['password'],
                                    user=db_connect['user'],
                                    port=db_connect['port'])
            print('DB connection opened')
        except Exception as e:
            print(f'::DBConnect:: cannot connect to DB Exception: {e}')
            raise

        try:
            cursor = conn.cursor()

            # Preparamos los valores (tuplas de domain_id y valor nuevo)
            data_to_update = [
                (
                    domain['sec_domain_id'],
                    domain['ml_sec_domain_classification'],
                    domain.get('confidence'),
                    domain.get('recommended_action_id'),
                    domain.get('justification'),
                    domain.get('exploit_type'),
                    domain.get('decision_source'),
                ) for domain in save_data
            ]

            # Crea un VALUES string gigante para el UPDATE masivo usando CTE
            values_template = ",".join([
                "(%s::bigint, %s::smallint, %s::varchar, %s::smallint, %s::smallint, %s::varchar[], %s::varchar)"
            ] * len(data_to_update))
            flat_values = []
            for tup in data_to_update:
                flat_values.extend(tup)  # aplanamos la lista para pasar a execute

            sql = f"""
                WITH updates (
                    sec_domain_id,
                    value_to_update,
                    confidence_to_update,
                    recommended_action_id_to_update,
                    justification_to_update,
                    exploit_type_to_update,
                    decision_source_to_update
                ) AS (
                    VALUES {values_template}
                )
                UPDATE public.secondary_domains AS t
                SET ml_sec_domain_classification = u.value_to_update,
                    decision_source = u.decision_source_to_update,
                    confidence = u.confidence_to_update,
                    recommended_action_id = u.recommended_action_id_to_update,
                    justification = u.justification_to_update,
                    exploit_type = u.exploit_type_to_update
                FROM updates u
                WHERE t.sec_domain_id = u.sec_domain_id;
            """

            cursor.execute(sql, flat_values)
            conn.commit()
            print(f'{len(data_to_update)} domains updated using CTE VALUES method.')

        except Exception as e:
            print(f'Error during CTE batch update: {e}')
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
            print('DB connection closed')

