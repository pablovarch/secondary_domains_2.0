from dependencies import  playwright, log, proxy, tools
from models import  country, html_features, secondary_domains
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
        self.__secondary_domains = secondary_domains.Secondary_domains()

    def crawl(self):
        global sec_domain
        try:
            list_country_data = self.__country.get_country_data()
            supply_list = self.__tools.read_csv('supply.csv')
            supply_list = self.__tools.clean_country_supply(supply_list)
            list_to_scan = self.__secondary_domains.get_all_secondary_domains()
            # Start crawler
            self.__logger.info(f" --- Start secondary_domains_crawler ---")
            self.__logger.info(f" --- {len(supply_list)} elements")
            
            for dom in list_to_scan:
                try:

                    sec_domain_id = dom['sec_domain_id']
                    sec_domain = dom['sec_domain']
                    coun = 'United States'
                    self.__logger.info(f'------scrape site {dom} - country {coun}')

                    # get random profile
                    random_profile = random.choice(settings.profile_list)

                    # get proxy data
                    proxy_data = self.__proxy.get_proxy_data(coun, list_country_data, self.__list_country_oxy)



                    # navigation
                    status_dict , dict_feature_domain , html_features = self.__playwright.navigation(sec_domain,
                                                                                     proxy_data['proxy_dict'],
                                                                                     random_profile,
                                                                                     )
                        
                    if status_dict['online_status'] == 'Online':
                            self.__logger.info(f'insert features: {sec_domain}')
                            html_features['sec_domain_id'] = sec_domain_id
                            html_features['domain_name'] = sec_domain
                            self.__html_features.insert_feature(html_features)
                            # check sec_domain_html_id
                            sec_domain_html_id = self.__secondary_domains.get_secondary_domain_html_id(sec_domain_id)
                            if not sec_domain_html_id:
                                self.__logger.info(f'save html: {sec_domain}')
                                self.__secondary_domains.save_secondary_domain_html(sec_domain_id,dict_feature_domain['html'])



                    self.__logger.info(f'update status - id:{sec_domain_id} - {status_dict}')
                    if status_dict['offline_type'] == 'Redirect':
                        redirect_domain = True
                    else:
                        redirect_domain = False
                    self.__secondary_domains.update_status(sec_domain_id,redirect_domain,status_dict['online_status'])

                except Exception as error:
                    self.__logger.error(f'::MainCrawler::Error on domain {sec_domain} - {error}')
                    # self.save_error_sites(sec_domain, coun, error, date_)

            self.__logger.info(f" ::MainCrawler:: Crawler ended")

        except Exception as e:
            text = f" ::MainCrawler:: General Exception; {e}"
            self.__logger.error(text)


        except Exception as er:
            self.__logger.error(f'::MainCrawler::Error on save navigation data - {er}')


