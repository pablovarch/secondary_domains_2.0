from dependencies import  log
import requests
import re
import constants, settings
from settings import db_connect
import psycopg2


class site_map :
    def __init__(self):
        self.__logger = log.Log().get_logger(name='sitemap.log')

    def main(self):
        self.__logger.info('getting all secondary_domains')
        list_to_scan = self.get_all_secondary_domains()
        for dom in list_to_scan:
            sec_domain_id = dom['sec_domain_id']
            sec_domain = dom['sec_domain']
            coun = 'United States'
            self.__logger.info(f'------scrape site {dom} - country {coun}')

            domain = 'https://' + sec_domain
            try:
                # Intentar sitemap.xml
                response = requests.get(f"{domain}/sitemap.xml", timeout=10)
                if response.status_code == 200:
                    urls = re.findall(r'<loc>(.*?)</loc>', response.text)
                    site_map_count = len(urls)
                    self.__logger.info(f'update domain - {sec_domain_id}')
                    self.update_secondary_domain(sec_domain_id,site_map_count)
                else:
                    self.__logger.info('domain status is not online ')
            except:
                self.__logger.error(f'cant get site map for domain:{sec_domain}')
                pass

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
            sql_string = """SELECT  distinct sd.sec_domain_id , sd.sec_domain  
            FROM secondary_domains sd 
            where sd.site_map_count is null 
            and sd.online_status = 'Online'; """
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

    def update_secondary_domain(self, sec_domain_id,site_map_count ):
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
                       SET site_map_count = %s 
                       WHERE sec_domain_id = %s
                   """
            data = (site_map_count, sec_domain_id)
            try:
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error(
                    f'::Saver:: Error updating status on secondary domains with id {sec_domain_id} - {e}')
            finally:
                cursor.close()
                conn.close()