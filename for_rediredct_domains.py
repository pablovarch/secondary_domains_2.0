from dependencies import  log
import requests
import re
import constants, settings
from settings import db_connect
import psycopg2


class For_redirect_domains :
    def __init__(self):
        self.__logger = log.Log().get_logger(name='redirect_domains.log')

    def main(self):
        self.__logger.info('getting all redirect domains on secondary_domains')
        list_to_scan = self.get_all_redirect_secondary_domains()
        for dom in list_to_scan:
            sec_domain_id = dom['sec_domain_id']
            sec_domain = dom['sec_domain']
            online_status = dom['online_status']
            ml_piracy = dom['ml_piracy']
            if online_status == 'Online':
                # check in exclude domains
                is_in_exclude = self.is_in_exclude_domains(sec_domain)
                if is_in_exclude:
                    self.__logger.info('domain is a comercial target')
                else:
                    # check ml_piracy
                    if ml_piracy is True:
                        self.__logger.info('domain is a comercial target')
                    elif ml_piracy is False:
                        self.__logger.info('domain is a comercial target')



            else:
                if online_status == 'Offline | Status Checker':
                    self.__logger.info('domain is Offline ')



    def is_in_exclude_domains(self, sec_domain):

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
            browser_profile_id = None
            sql_string = "select exc_domain_id from exclude_domain_attributes eda where eda.exc_domain = %s;"

            data = (sec_domain,)

            is_in_exclude = False
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                exc_domain_id = cursor.fetchone()
                conn.commit()
                if exc_domain_id:
                    is_in_exclude = True
            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return is_in_exclude


    def get_all_redirect_secondary_domains(self):
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
            sql_string = """select sd.sec_domain_id, sd.sec_domain, sd.online_status,sd.ml_piracy 
                            from secondary_domains sd 
                            where sd.redirect_domain = true; """
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
                            'online_status': elem[2],
                            'ml_piracy': elem[3]

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