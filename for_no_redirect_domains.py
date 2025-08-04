from dependencies import  log
import requests
import re
import constants, settings
from settings import db_connect
import psycopg2


class For_no_redirect_domains :
    def __init__(self):
        self.__logger = log.Log().get_logger(name='no_redirect_domains.log')

    def main(self):
        self.__logger.info('getting all Online domains on secondary_domains')
        list_to_scan = self.get_all_online_secondary_domains()
        dict_graymarket = {
            'Adult Content':5,
            'Gambling & Betting':6,
            'Cryptocurrency Speculation':7,
            'Supplement / Nutra':8,
            'undeterminated': 9
        }
        for dom in list_to_scan:

            ml_sec_domain_classification = None
            sec_domain_id = dom['sec_domain_id']
            sec_domain = dom['sec_domain']
            ssl_poor = dom['ssl_poor']
            high_traffic = dom['high_traffic']
            is_ecommerce = dom['is_ecommerce']
            mfa_engagement = dom['mfa_engagement']
            has_affiliate_handoff = dom['has_affiliate_handoff']
            graymarket_label = dom['graymarket_label']
            ad_density = dom['ad_density']
            tld_poor = dom['tld_poor']
            is_high_risk_geo = dom['is_high_risk_geo']
            # check poor ssl
            if ssl_poor and is_high_risk_geo:
                self.__logger.info('domain is a referal_cloaking')
                ml_sec_domain_classification = 2
            else:
                if high_traffic and is_ecommerce:
                    self.__logger.info('domain is a comercial target')
                    ml_sec_domain_classification = 4
                else:
                    if mfa_engagement:
                        if is_ecommerce or has_affiliate_handoff:
                            self.__logger.info('domain is a comercial target')
                            ml_sec_domain_classification = 4
                        else:
                            if ad_density and tld_poor:
                                self.__logger.info('domain is a MFA')
                                ml_sec_domain_classification = 3
                            else:
                                if graymarket_label:
                                    self.__logger.info('domain is a comercial target')
                                    ml_sec_domain_classification = dict_graymarket[graymarket_label]
                                else:
                                    ml_sec_domain_classification = 9
                    else:
                        if ad_density and tld_poor:
                            self.__logger.info('domain is a MFA')
                            ml_sec_domain_classification = 3
                        else:
                            if graymarket_label:
                                self.__logger.info('domain is a comercial target')
                                ml_sec_domain_classification = dict_graymarket[graymarket_label]
                            else:
                                ml_sec_domain_classification = 9
            if ml_sec_domain_classification:
                if ml_sec_domain_classification == 4 and graymarket_label and graymarket_label != 'undeterminated':
                    ml_sec_domain_classification = dict_graymarket[graymarket_label]

                self.__logger.info(f'update domain - {sec_domain} - ml_sec_domain_classification: {ml_sec_domain_classification}')
                self.update_secondary_domain(sec_domain_id,ml_sec_domain_classification)
            else:
                self.__logger.info(f'domain - {sec_domain_id} cant be classificated')











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


    def get_all_online_secondary_domains(self):
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
                            select sd.sec_domain_id ,
                            sd.sec_domain,
                            sd.ssl_poor,
                            sd.high_traffic,
                            sd.is_ecommerce,
                            sd.mfa_engagement,
                            sd.has_affiliate_handoff,
                            sd.graymarket_label,
                            sd.ad_density,
                            sd.tld_poor,
                            sd.is_high_risk_geo  
                            from secondary_domains sd  
                            where ml_sec_domain_classification is null
                            and sd.online_status ='Online' """
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
                            'sec_domain' : elem[1],
                            'ssl_poor': elem[2],
                            'high_traffic': elem[3],
                            'is_ecommerce': elem[4],
                            'mfa_engagement': elem[5],
                            'has_affiliate_handoff': elem[6],
                            'graymarket_label': elem[7],
                            'ad_density': elem[8],
                            'tld_poor': elem[9],
                            'is_high_risk_geo': elem[10]

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

    def update_secondary_domain(self, sec_domain_id,ml_sec_domain_classification ):
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
                       SET ml_sec_domain_classification = %s 
                       WHERE sec_domain_id = %s
                   """
            data = (ml_sec_domain_classification, sec_domain_id)
            try:
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error(
                    f'::Saver:: Error updating status on secondary domains with id {sec_domain_id} - {e}')
            finally:
                cursor.close()
                conn.close()