from dependencies import  playwright, log, proxy, tools
from models import  country, domain_attributes, domain_features, html_features
from datetime import datetime
import constants, settings
import random
import psycopg2
from settings import db_connect


class secondary_domains_crawler:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
        self.__playwright = playwright.Playwright()
        self.__country = country.Country()
        self.__proxy = proxy.Proxy()
        self.__tools = tools.Tools()
        self.__html_features = html_features.html_features()
        self.__list_country_oxy = self.__tools.read_csv(constants.name_csv_country_oxy)

    def crawl(self):
        global domain_item
        try:
            list_country_data = self.__country.get_country_data()
            supply_list = self.__tools.read_csv('supply.csv')
            supply_list = self.__tools.clean_country_supply(supply_list)
            list_to_scan = self.get_all_secondary_domains()
            list_to_scan = [{'html_feature_id':11,'domain':'chelovek-muravey-lordfilm.ru'}]
            # Start crawler
            self.__logger.info(f" --- Start secondary_domains_crawler ---")
            self.__logger.info(f" --- {len(supply_list)} elements")
            
            for dom in list_to_scan:
                try:

                    sec_domain_id = dom['html_feature_id']
                    domain_item = dom['domain']
                    coun = 'United States'
                    self.__logger.info(f'------scrape site {dom} - country {coun}')

                    # get random profile
                    random_profile = random.choice(settings.profile_list)

                    # get proxy data
                    proxy_data = self.__proxy.get_proxy_data(coun, list_country_data, self.__list_country_oxy)



                    # navigation
                    status_dict , dict_feature_domain , html_features = self.__playwright.navigation(domain_item,
                                                                                     proxy_data['proxy_dict'],
                                                                                     random_profile,
                                                                                     )
                        
                    if status_dict['online_status'] == 'Online':

                            self.__logger.info(f'insert features: {domain_item}')
                            # self.__html_features.update_html_features(sec_domain_id, html_features)
                            self.__logger.info(f'save html: {domain_item}')

                    else:
                        if status_dict['online_status'] == 'redirect':
                            print('actualizar redirect')

                except Exception as error:
                    self.__logger.error(f'::MainCrawler::Error on domain {domain_item} - {error}')
                    # self.save_error_sites(domain_item, coun, error, date_)

            self.__logger.info(f" ::MainCrawler:: Crawler ended")

        except Exception as e:
            text = f" ::MainCrawler:: General Exception; {e}"
            self.__logger.error(text)


        except Exception as er:
            self.__logger.error(f'::MainCrawler::Error on save navigation data - {er}')


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
            sql_string = """select sec_domain_id, sec_domain  from secondary_domains"""
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

