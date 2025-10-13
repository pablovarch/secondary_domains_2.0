from dependencies import log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


class Address_bar_class:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='address_bar_class.log')

    def main(self):
        # Correcting the syntax error by removing the invalid 'DB Connection' line
        alchemyEngine = create_engine(db_connect_df, pool_recycle=3600)

        dbConnection = alchemyEngine.connect()
        address_bar_source = pd.read_sql("""
                                WITH t AS (
                                  SELECT
                                    session_id,
                                    ad_event_id,
                                    tab_num,
                                    address_bar_num,
                                    address_bar_url,
                                    source_domain,
                                    address_bar_domain,
                                    MAX(address_bar_num) OVER (
                                      PARTITION BY session_id, ad_event_id, tab_num
                                    ) AS grp_max
                                  FROM address_bar_url
                                )
                                SELECT
                                  session_id,
                                  ad_event_id,
                                  tab_num,
                                  address_bar_num,
                                  address_bar_url,
                                  source_domain,
                                  address_bar_domain
                                FROM t
                                WHERE tab_num > 0
                                  AND source_domain IS DISTINCT FROM address_bar_domain
                                  AND grp_max > 0
                                  AND address_bar_num < grp_max;
                                """, dbConnection)

        sec_domains = pd.read_sql("""
                                SELECT sec_domain_id, 
                                sec_domain, 
                                online_status, 
                                ml_sec_domain_classification, 
                                sec_domain_source
                                FROM secondary_domains
                                """, dbConnection)

        dbConnection.close()

        address_bar_filtered = address_bar_source[["address_bar_domain"]].drop_duplicates()

        filtered_list = pd.merge(sec_domains, address_bar_filtered, left_on='sec_domain', right_on="address_bar_domain", how='inner')

        filtered_list["ml_sec_domain_classification"] = 2
        # Add the mfa_engagement column based on the conditions
        df_filtered = filtered_list[['sec_domain_id', 'ml_sec_domain_classification']]
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