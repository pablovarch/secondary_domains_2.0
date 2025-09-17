from dependencies import  log
from settings import db_connect
import psycopg2
import re
from typing import Union


class tld :
    def __init__(self):
        self.__logger = log.Log().get_logger(name='tld.log')

    def main(self):
        self.__logger.info('getting all secondary_domains')



        list_to_scan = self.get_all_secondary_domains()
        for dom in list_to_scan:
            sec_domain_id = dom['sec_domain_id']
            sec_domain = dom['sec_domain']
            try:
                self.__logger.info(f'------scrape site {dom} ')
                tld = self.is_low_cost_tld(sec_domain)
                self.__logger.info('updating domain')
                self.update_secondary_domain(sec_domain_id,tld)
            except Exception as e:
                self.__logger.error(f'Error getting tld for - {dom}')

    def extract_tld(self, label):
        """
        Devuelve el TLD en minúsculas sin el punto inicial.
        Acepta:
            - 'example.com'
            - 'sub.dom.icu'
            - '.xyz'
            - 'xyz'
        """
        label = label.lower().lstrip('.')
        return label.split('.')[-1]  # lo que viene después del último punto


    def is_low_cost_tld(self, domain_or_tld: Union[str, bytes]) -> bool:
        """
        True  -> pertenece a LOW_COST_OR_UNCOMMON_TLDS
        False -> no pertenece
        """

        # ------------------------------ #
        #  Lista curada de TLD sospechosos
        # ------------------------------ #
        LOW_COST_OR_UNCOMMON_TLDS: set[str] = {
            # gTLD baratos o en promo constante (< 5 USD)  — Spamhaus Top-20 / promo lists
            "top", "xyz", "info", "biz", "online", "site", "store", "shop", "click",
            "fun", "bond", "cfd", "icu", "today", "sbs", "live", "pro", "vip",
            "club", "space", "press", "rocks", "link", "download", "loan", "bid",
            "one", "gdn", "work", "science", "trade", "party", "win", "stream",
            "men", "mom", "kim",

            # gTLD recién añadidos a rankings de abuso
            "xin", "dev", "pictures", "pizza", "poker", "qpon",

            # ccTLD gratuitos o casi gratuitos (ex-Freenom) + ccTLD con ratio alto de phishing
            "tk", "ml", "ga", "cf", "gq",  # Freenom fam.
            "li", "es", "ru", "cc", "cn",

            # Otros cc/gTLD con histórico de suspensiones o campañas masivas
            "ai", "cfd", "icu", "vip", "bond",  # repeticiones intencionales para claridad
        }
        if isinstance(domain_or_tld, bytes):
            domain_or_tld = domain_or_tld.decode()

        tld = self.extract_tld(domain_or_tld)
        return tld in LOW_COST_OR_UNCOMMON_TLDS

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
            where sd.tld_poor is null 
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

    def update_secondary_domain(self, sec_domain_id,tld ):
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
                       SET tld_poor = %s 
                       WHERE sec_domain_id = %s
                   """
            data = (tld, sec_domain_id)
            try:
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error(
                    f'::Saver:: Error updating status on secondary domains with id {sec_domain_id} - {e}')
            finally:
                cursor.close()
                conn.close()