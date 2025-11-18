import json
import urllib.parse
from settings import db_connect, db_connect_df, bd_apikey
import psycopg2
from dependencies import log
import urllib3
import urllib.request
import ssl
import sys
import time
import requests
ssl._create_default_https_context = ssl._create_unverified_context


class Google_Search_results:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='Google_Search_results.log')

    def main(self):
        self.__logger.info('Start Google_Search_results script')
        self.__logger.info('getting all secondary_domains')
        list_to_scan = self.get_all_secondary_domains()
        for dom in list_to_scan:
            try:
                sec_domain_id = dom['sec_domain_id']
                sec_domain = dom['sec_domain']
                self.__logger.info(f'------scrape site {dom} - ')

                # google_search_result = self.get_subdomains_oxy_api_claude(sec_domain)
                google_search_result = self.google_serp_100_results(sec_domain)
                self.__logger.info(f"update num results: {google_search_result}")
                self.update_secondary_domain(sec_domain_id, google_search_result)
            except Exception as e:
                self.__logger.error(f'error on :{dom} - error {e}')


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
                select sd.sec_domain_id , sd.sec_domain_root
                 from secondary_domains sd
                WHERE sec_domain_source = 'SimilarWeb'
                  AND ml_sec_domain_classification not in (1, 2)
                  AND online_status IN ('Blocked', 'Offline', 'Online')
                  and ( sd.google_search_results is null or sd.google_search_results = 10)
             """

            # sql_string = """
            #                 select sd.sec_domain_id , sd.sec_domain
            #                 from secondary_domains sd
            #                 WHERE
            #                 sd.sec_domain = 'whitebit.com'
            #              """
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
                "parse": True,
                "pages": 20
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

    def get_subdomains_oxy_api_claude(self, domain_source):
        """
        Obtiene subdominios de un dominio usando la API de Oxylabs
        Itera sobre todas las páginas de resultados

        Args:
            domain_source (str): Dominio principal a buscar

        Returns:
            tuple: (número de resultados, lista de subdominios únicos)
        """
        try:
            appearance_count = 0
            list_dom = []
            country = "United States"
            url = "https://realtime.oxylabs.io/v1/queries"
            query = f'site:{domain_source}'

            payload = json.dumps({
                "source": "google_search",
                "domain": "com",
                "query": query,
                "geo_location": country,
                "parse": True,
                "pages": 10
            })

            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Basic Y2hfYWRfc25pZmZlcl9pc2ZQWjpNM00rRVhzazlNTm8zcExQZmFM' # ⚠️ Reemplaza con tu credencial
            }

            response = requests.post(url, headers=headers, data=payload, timeout=60)
            response.raise_for_status()

            json_response = response.json()

            # Verificar que existan resultados
            if 'results' not in json_response or not json_response['results']:
                self.__logger.warning(f"No se encontraron resultados para {domain_source}")
                return 0

            # ⭐ ITERAR SOBRE TODAS LAS PÁGINAS
            from urllib.parse import urlparse

            for page_result in json_response['results']:
                # Verificar que la página tenga resultados orgánicos
                if 'content' not in page_result:
                    continue

                if 'results' not in page_result['content']:
                    continue

                if 'organic' not in page_result['content']['results']:
                    continue

                organic_results = page_result['content']['results']['organic']

                # Procesar cada resultado de la página
                for result in organic_results:
                    if 'url' in result:
                        parsed_url = urlparse(result['url'])
                        subdomain = parsed_url.netloc

                        # ⭐ Contar TODAS las apariciones (incluso duplicados)
                        if domain_source in subdomain:
                            appearance_count += 1

                            # Agregar a lista única solo si no existe
                            if subdomain not in list_dom:
                                list_dom.append(subdomain)

            num_unique = len(list_dom)

            self.__logger.info(f"Total de apariciones (con duplicados): {appearance_count}")
            self.__logger.info(f"Subdominios únicos encontrados: {num_unique}")
            self.__logger.info(f"Lista de subdominios únicos: {list_dom}")

            return appearance_count

        except requests.exceptions.Timeout:
            self.__logger.error(f"Timeout al consultar API para {domain_source}")
            return 0
        except requests.exceptions.RequestException as e:
            self.__logger.error(f"Error en la petición HTTP: {e}")
            return 0
        except KeyError as e:
            self.__logger.error(f"Error parseando respuesta - clave faltante: {e}")
            return 0
        except Exception as e:
            self.__logger.error(f"Error inesperado obteniendo subdominios: {e}")
            return 0

    def google_serp_100_results(self,domain_source):
        try:
            API_KEY = bd_apikey
            DATASET_ID = "gd_mfz5x93lmsjjjylob"

            # Step 1: Trigger request
            trigger_url = f"https://api.brightdata.com/datasets/v3/trigger?dataset_id={DATASET_ID}&include_errors=true"
            trigger_response = requests.post(
                trigger_url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {API_KEY}",
                },
                json=[
                    {
                        "url": "https://www.google.com/",
                        "keyword": f'site:{domain_source}',
                        "language": "en",
                        "country": "US",
                        "start_page": 1,
                        "end_page": 10,
                    }
                ],
            )

            snapshot_id = trigger_response.json()["snapshot_id"]

            # Step 2: Poll for completion
            progress = None
            while progress is None or progress["status"] != "ready":
                time.sleep(5)  # Wait 5 seconds
                progress_url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
                progress_response = requests.get(
                    progress_url,
                    headers={"Authorization": f"Bearer {API_KEY}"},
                )
                progress = progress_response.json()

            # Step 3: Download results
            download_url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}?format=json"
            download_response = requests.get(
                download_url,
                headers={"Authorization": f"Bearer {API_KEY}"},
            )

            json_result = download_response.json()
            num_result = len(json_result[0]['organic'])
            return num_result
        except Exception as e:
            self.__logger.info(f'Error on get_subdomains_brightdata: {e}')
