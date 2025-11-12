from dependencies import log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Para Clasificar dominios de Betting y excluir sitios con piracy brand

class Jarm_processing:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='jarm_rules.log')

    def main(self):


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
            and online_status is not null
            and google_search_results is not null""", conn)


        def proccess_domains(row):
            if row["redirect_domain"] is True:
                return 2
            if row["online_status"] == "Online":
                if row["google_search_results"] < 2:
                    return 2
                if row['google_search_results'] >= 10 and row['google_search_results'] < 50:
                    return 3
                if row['google_search_results'] >= 50:
                    return 4
            else:
                if row["online_status"] in ["Offline", "Blocked", "Offline | Status Checker", "Offline--Bulk-check", "Offline | Ad Sniffer"] and row["google_search_results"] < 10:
                    return 2
                else:
                    return 4
        
        sec_domain['ml_sec_domain_classification'] = sec_domain.apply(proccess_domains, axis=1)
        
        sec_domain = sec_domain.dropna(subset=['ml_sec_domain_classification'])

        df_filtered = sec_domain[['sec_domain_id', 'ml_sec_domain_classification']]
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
                (domain['sec_domain_id'], domain['ml_sec_domain_classification']) for domain in save_data
            ]

            # Crea un VALUES string gigante para el UPDATE masivo usando CTE
            values_template = ",".join(["(%s, %s)"] * len(data_to_update))
            flat_values = []
            for tup in data_to_update:
                flat_values.extend(tup)  # aplanamos la lista para pasar a execute

            sql = f"""
                WITH updates (sec_domain_id, value_to_update) AS (
                    VALUES {values_template}
                )
                UPDATE public.secondary_domains AS t
                SET ml_sec_domain_classification = u.value_to_update
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

