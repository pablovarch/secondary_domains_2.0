import constants
import psycopg2
import re
from settings import db_connect

from dependencies import  log

class Secondary_domains:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
        
    def get_secondary_domain(self, domain):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the secondary_domain information
        """

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

            sql_string = """select sec_domain_id, sec_domain from secondary_domains sd where sd.sec_domain =%s;"""
            
            data = (domain,)   
            sec_domain = None
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                response = cursor.fetchone()
                conn.commit()
                if response:
                    sec_domain = response[0]
                
            except Exception as e:
                self.__logger.error('::secondary_domain:: Error found trying to Save data - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return sec_domain

    
    

    def save_secondary_domain_html(self, sec_domain_id, html):
        """
        This method try to connect to the DB and save the data
        :param sec_domain_id: id of the secondary domain
        :param html: html content of the secondary domain
        """
        self.__logger.info(f" --- save secondary domain html ---")
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
            sql_string = "INSERT INTO public.secondary_domains_html (sec_domain_id, html_content) VALUES(%s,%s);"
            data = (sec_domain_id, html)
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error('::Secondary domains:: Error found trying to Save secondary_domain_html - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
        
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
            sql_string = """
                SELECT DISTINCT sd.sec_domain_id , sd.sec_domain
                from secondary_domains sd
                where
                sd.online_status = 'Online-Bulk-check'
                """
            sql_string = """SELECT DISTINCT sd.sec_domain_id , sd.sec_domain
                            FROM secondary_domains sd
                            -- INNER JOIN exclude_domain_attributes eda ON eda.exc_domain = sd.sec_domain
                            LEFT JOIN domain_discovery_features ddf ON sd.sec_domain_id = ddf.sec_domain_id
                            LEFT JOIN secondary_domains_html sdh ON sd.sec_domain_id = sdh.sec_domain_id
                            ORDER BY sd.sec_domain_id ASC; """

            # sql_string = """
            #                 SELECT sd.sec_domain_id , sd.sec_domain
            #                 from secondary_domains sd
            #                 where
            #                 sd.sec_domain = 'playerjs.com'
            #                 """
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

    def update_status(self, sec_domain_id, redirect_domain, online_status ):
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
                SET redirect_domain = %s , online_status =%s
                WHERE sec_domain_id = %s
            """
            data = (redirect_domain, online_status, sec_domain_id)
            try:
                cursor.execute(sql_string,data)
                conn.commit()
            except Exception as e:
                self.__logger.error(f'::Saver:: Error updating status on secondary domains with id {sec_domain_id} - {e}')
            finally:
                cursor.close()
                conn.close()


    def get_secondary_domain_html_id(self, sec_domain_id):
        """
        This method try to connect to the DB and save the data
        :param browser_dict: dictionary containing the crawler settings information
        """

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
            sec_domain_html_id  = None
            sql_string = "select sdh.sec_domain_id from secondary_domains_html sdh where sdh.sec_domain_id = %s ;"

            data = (sec_domain_id,)
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                sec_domain_html_id  = cursor.fetchone()
                conn.commit()
                if sec_domain_html_id :
                    sec_domain_html_id  = sec_domain_html_id [0]

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return sec_domain_html_id

    def update_secondary_domain_html_lenght(self, html_length,sec_domain_id ):

        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the secondary information
        """

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

            sql_string = """UPDATE public.secondary_domains SET  html_length=%s WHERE sec_domain_id=%s;"""

            data = (
                html_length,
                sec_domain_id
            )
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error(
                    '::subdomain:: Error found trying to Update secondary domains - {}'.format(e))

            finally:
                cursor.close()
                conn.close()