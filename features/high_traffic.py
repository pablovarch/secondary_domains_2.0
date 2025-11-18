from dependencies import  log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

class high_traffic:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='high_traffic.log')

    def main(self):
        self.__logger.info('Starting high traffic script')
        # Correcting the syntax error by removing the invalid 'DB Connection' line
        alchemyEngine = create_engine(
            db_connect_df,
            pool_recycle=3600)

        dbConnection = alchemyEngine.connect()
        traffic = pd.read_sql("""WITH latest_months AS (
                                SELECT 
                                    sec_domain_id,
                                    MAX(month) as last_month
                                FROM dim_traffic 
                                WHERE domain_country = 'World' 
                                AND sec_domain_id IS NOT NULL
                                GROUP BY sec_domain_id
                            )
                            SELECT 
                                dt.sec_domain_id,
                                dt.month,
                                dt.traffic,
                                dt.exc_domain_id
                            FROM dim_traffic dt
                            INNER JOIN latest_months lm 
                                ON dt.sec_domain_id = lm.sec_domain_id 
                                AND dt.month = lm.last_month
                            WHERE dt.domain_country = 'World' 
                            AND dt.sec_domain_id IS NOT NULL;""", dbConnection)
        dbConnection.close()
        traffic['high_traffic'] = traffic['traffic'] > 1500000
        df_filtered = traffic[['sec_domain_id', 'high_traffic']]
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
                (domain['sec_domain_id'], domain['high_traffic']) for domain in save_data
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
                SET high_traffic = u.value_to_update
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