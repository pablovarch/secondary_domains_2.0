from dependencies import log
import constants
import psycopg2
from settings import db_connect

class Domain_attributes:

    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def get_all_domain_attributes(self):
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
            sql_string = "select domain_id , domain from domain_attributes  "

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string)
                respuesta = cursor.fetchall()
                conn.commit()
                if respuesta:
                    list_all_domain_attributes = []
                    for elem in respuesta:
                        domain_data = {
                            'domain_id': elem[0],
                            'domain': elem[1],

                        }
                        list_all_domain_attributes.append(domain_data)
                else:
                    list_all_domain_attributes = []

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to get_all_domain_attributes - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return list_all_domain_attributes

    def get_domain_id(self, list_all_domain_attributes, domain_item):
        try:
            flag = False
            domain_id = None

            for elem in list_all_domain_attributes:
                if domain_item.strip() == elem['domain']:
                    domain_id = elem['domain_id']
                    flag = True
                    return domain_id

            if not flag:
                return domain_id

        except Exception as e:
            self.__logger.error(f" ::Get domain id Error:: {e}")

    def update_domain_attributes(self, values_dict, domain_item):
        """
        This method try to connect to the DB and save the data
        :param values_dict: dictionary containing the  collection job information
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

            sql_string = "UPDATE public.domain_attributes SET domain_classification_id=%s, online_status=%s, " \
                         "offline_type=%s WHERE domain =%s;"

            data = (values_dict['domain_classification_id'],
                    values_dict['online_status'],
                    values_dict['offline_type'],
                    domain_item
                    )
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                conn.commit()

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to Update Domain_attributes - {}'.format(e))

            finally:
                cursor.close()
                conn.close()


