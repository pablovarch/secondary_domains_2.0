from dependencies import log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
import re
from dependencies import log
import random
import json
import requests


class Google_Search_results:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='Google_Search_results.log')

    def main(self):
        self.__logger.info('getting all secondary_domains')
        list_to_scan = self.get_all_secondary_domains()
        for dom in list_to_scan:
            sec_domain_id = dom['sec_domain_id']
            sec_domain = dom['sec_domain']
            self.__logger.info(f'------scrape site {dom} - ')

            google_search_result = self.get_subdomains_oxy_api(sec_domain)
            self.update_secondary_domain(sec_domain_id, google_search_result)


    def get_all_secondary_domains(self):
        # Try to connect to the DB
        try:
            conn = psycopg2.connect(host=db_connect['host'],
                                    database=db_connect['database'],
                                    password=db_connect['password'],
                                    user=db_connect['user'],
                                    port=db_connect['port'])
            cursor = conn.cursor()

        except Exception as e:
            print('::DBConnect:: cant connect to DB Exception: {}'.format(e))
            raise
        else:
            # sql_string = """select * from domain_discovery dd  where online_status = 'Online' and dd.status_details = 'Bulk-check' order by dd.disc_domain_id limit 5000"""
            sql_string = """
                select sd.sec_domain_id , sd.sec_domain  
                from secondary_domains sd
                WHERE sec_domain_source = 'SimilarWeb'
                  -- AND ml_sec_domain_classification IS NULL
                  AND online_status IN ('Blocked', 'Offline', 'Online')
                  and sd.google_search_results is null
             """
            list_all_domains = []
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string)
                respuesta = cursor.fetchall()
                conn.commit()
                if respuesta:

                    for elem in respuesta:
                        domain_data = {
                            'sec_domain_id': elem[0],
                            'sec_domain': elem[1],

                        }
                        list_all_domains.append(domain_data)
                else:
                    list_all_domains = []

            except Exception as e:
                self.__logger.error(':::: Error found trying to get_all_secondary_domains'.format(e))

            finally:
                cursor.close()
                conn.close()
                return list_all_domains

    def update_secondary_domain(self, sec_domain_id, google_search_results):
        try:
            conn = psycopg2.connect(host=db_connect['host'],
                                    database=db_connect['database'],
                                    password=db_connect['password'],
                                    user=db_connect['user'],
                                    port=db_connect['port'])
            cursor = conn.cursor()
        except Exception as e:
            print('::DBConnect:: cannot connect to DB Exception: {}'.format(e))
            raise
        else:

            sql_string = f"""
                       UPDATE public.secondary_domains
                       SET google_search_results = %s 
                       WHERE sec_domain_id = %s
                   """
            data = (google_search_results, sec_domain_id)
            try:
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error(
                    f'::Saver:: Error updating status on secondary domains with id {sec_domain_id} - {e}')
            finally:
                cursor.close()
                conn.close()

    def get_subdomains_oxy_api(self, domain_source):


        try:
            num_result = None
            list_dom = []
            country = "United States"
            url = "https://realtime.oxylabs.io/v1/queries"
            query = f' site:{domain_source}'
            payload = json.dumps({
                "source": "google_search",
                "domain": "com",
                "query": query,
                "geo_location": country,
                "parse": True
            })
            headers = {
                'Content-Type': 'application/json',
                # 'Authorization': 'Basic ZGF0YV9zY2llbmNlOm5xRHJ4UUZMeHNxNUpOOHpTekdwMg=='
                'Authorization': 'Basic Y2hfYWRfc25pZmZlcl9pc2ZQWjpNM00rRVhzazlNTm8zcExQZmFM'
            }

            response = requests.request("POST", url, headers=headers, data=payload)
            json_response = json.loads(response.text)
            json_result = json_response['results'][0]['content']['results']['organic']
            num_result = len(json_result)
            print(f"num_result: {num_result}")

        except Exception as e:
            self.__logger.error(f" ::Get subdomains Error:: {e}")
        return num_result

    def delete_duplicates_subdomains(self, subdomains):
        # Conjunto para llevar un registro de dominios únicos
        dominios_vistos = set()

        # Lista para almacenar elementos únicos basados en el dominio
        lista_sin_repetidos = []
        try:
            for elemento in subdomains:
                # Verifica si el dominio ya ha sido visto
                if elemento['domain'] not in dominios_vistos:
                    # Agrega el dominio al conjunto de dominios vistos
                    dominios_vistos.add(elemento['domain'])
                    # Agrega el elemento a la lista de elementos únicos
                    lista_sin_repetidos.append(elemento['domain'])

            return lista_sin_repetidos
        except Exception as e:
            self.__logger.error(f'Error on delete duplicates subdomains - Error {e}')
            return lista_sin_repetidos
