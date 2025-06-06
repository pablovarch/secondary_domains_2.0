import constants, settings
import psycopg2
from settings import db_connect
from dependencies import log


class Browser_profile:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def scrape_and_save_driver(self, country, user_agent, proxy_zone, country_id):
        try:
            self.__logger.info(f" --- scrape and save driver settings ---")
            driver_used = f"Playwright-{constants.browser}"
            profile = f'default_{country}'
            crawler_method = f"{settings.crawler_mode} {constants.crawler_version}"

            browser_dict = {
                'driver': driver_used,
                'profile': profile,
                'proxy_zone': proxy_zone,
                'crawler_method': crawler_method,
                'os': 'Windows',
                'user_agent_string': user_agent,
                'country_id': country_id,
                'proxy_service': settings.proxy_service

            }
            browser_profile_id = self.get_browser_profile_id(browser_dict)
            if not browser_profile_id:
                self.__logger.info(f" --- Saving new browser profile ---")
                browser_profile_id = self.save_settings(browser_dict)
            return browser_profile_id
        except Exception as e:
            self.__logger.error('::Browser_profile:: Error found on scrape_and_save_driver - {}'.format(e))

            pass

    def get_browser_profile_id(self, browser_dict):
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
            browser_profile_id = None
            sql_string = "SELECT browser_profile_id FROM public.browser_profiles where driver = %s" \
                         " and profile=%s and" \
                         " crawler_method = %s;"

            data = (browser_dict['driver'], browser_dict['profile'], browser_dict['crawler_method'])
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                browser_profile_id = cursor.fetchone()
                conn.commit()
                if browser_profile_id:
                    browser_profile_id = browser_profile_id[0]

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return browser_profile_id

    def save_settings(self, values_dict):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the crawler settings information
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
            settings_id = None
            sql_string = " INSERT INTO browser_profiles (driver, profile, proxy_zone,crawler_method, os, " \
                         "user_agent_string, country_id, proxy_service) " \
                         "VALUES(%s, %s, %s,%s, %s,%s,%s,%s) RETURNING browser_profile_id;"

            data = (values_dict['driver'],
                    values_dict['profile'],
                    values_dict['proxy_zone'],
                    values_dict['crawler_method'],
                    values_dict['os'],
                    values_dict['user_agent_string'],
                    values_dict['country_id'],
                    values_dict['proxy_service'])

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                settings_id = cursor.fetchone()
                conn.commit()
                if settings_id:
                    settings_id = settings_id[0]
                else:
                    settings_id = None
            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data crawler - save_settings - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return settings_id
