from dependencies import log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Para Clasificar dominios de Betting y excluir sitios con piracy brand

class Block_class:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='Block_class.log')

    def main(self):
        self.__logger.info('-- starting Block_class classifier')

        alchemyEngine = create_engine(
            db_connect_df,  # ej: "postgresql+psycopg2://user:pass@host:5432/dbname"
            pool_recycle=3600,
            pool_pre_ping=True  # robustez ante conexiones caídas
        )
        # Correcting the syntax error by removing the invalid 'DB Connection' line
        with alchemyEngine.connect() as conn:  
            sec_domain = pd.read_sql(""" select sec_domain_id,sec_domain, exc_domain_id, google_search_results, online_status, redirect_domain from secondary_domains sd
            where ml_sec_domain_classification is null           
            and google_search_results is not null
            and online_status is not null and sd.online_status !='Online' and sd.online_status !='Online-Bulk-check'""", conn)


        def proccess_domains(row):
            if row["redirect_domain"]:
                return 2
            else:
                if row["google_search_results"] <= 2:
                    return 2
                if row['google_search_results'] > 2 and row['google_search_results'] < 50:
                    return 3
                if row['google_search_results'] >= 50:
                    return 4
        
        sec_domain['ml_sec_domain_classification'] = sec_domain.apply(proccess_domains, axis=1)
        
        sec_domain = sec_domain.dropna(subset=['ml_sec_domain_classification'])

        df_filtered = sec_domain[['sec_domain_id', 'ml_sec_domain_classification']]
        df_filtered['sec_domain_source'] = 'Offline Class'
        data_to_save = df_filtered.to_dict('records')
        self.update_domains(data_to_save)

    def update_domains(self, save_data):
        """
        Efficiently updates domain data using a CTE VALUES block (no temp table needed).
        """
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
                (domain['sec_domain_id'], domain['ml_sec_domain_classification'], domain['sec_domain_source'],
                 domain['decision_source']) for domain in save_data
            ]

            # Crea un VALUES string gigante para el UPDATE masivo usando CTE
            values_template = ",".join(["(%s, %s, %s, %s)"] * len(data_to_update))
            flat_values = []
            for tup in data_to_update:
                flat_values.extend(tup)  # aplanamos la lista para pasar a execute

            sql = f"""
                WITH updates (sec_domain_id, value_to_update,sec_domain_source_to_update, decision_source ) AS (
                    VALUES {values_template}
                )
                UPDATE public.secondary_domains AS t
                SET ml_sec_domain_classification = u.value_to_update,
                    sec_domain_source = u.sec_domain_source_to_update,
                    decision_source = u.decision_source
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

