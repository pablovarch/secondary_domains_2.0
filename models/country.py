import constants
import psycopg2
from settings import db_connect

from dependencies import log

class Country:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def get_country_data(self):
        """
        This method try to connect to the DB and save the data
        :param settings_dict: dictionary containing the crawler settings information
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

            sql_string = "select * from country_data"

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string)
                respuesta = cursor.fetchall()
                conn.commit()
                if respuesta:
                    list_country_data = []
                    for elem in respuesta:
                        country_data = {
                            'country_id': elem[0],
                            'country': elem[1],
                            'iso_name': elem[2],                            
                            'iso_abbreviation': elem[3],
                            'sw_name':  elem[4],
                            'bd_abbreviation': elem[5],
                            'languagew3c': elem[6]
                        }
                        list_country_data.append(country_data)
                else:
                    list_country_data = []

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Save Data - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return list_country_data



