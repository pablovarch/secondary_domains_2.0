from dependencies import log
import constants
import psycopg2
import re
from settings import db_connect

class Domain_features:

    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
        
    def get_features(self, domain_id):
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
            sql_string = "select dfeatures_id , domain_id , homepage_button, num_popups, html_text, last_update from public.domain_features where domain_id = %s"

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, (domain_id,))
                respuesta = cursor.fetchall()
                conn.commit()
                if respuesta:
                    list_features = []
                    for elem in respuesta:
                        feature_data = {
                            'dfeatures_id': elem[0],
                            'domain_id': elem[1],
                            'homepage_button': elem[2],
                            'num_popups': elem[3],
                            'html_text': elem[4],
                            'last_update': elem[5].strftime('%Y-%m-%d'),

                        }
                        list_features.append(feature_data)
                else:
                    list_features = []

            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to get_features - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                return list_features
            
    def manage_feature(self, domain_id, date_, input_features):         
        
        # check invalid html
        if self.check_invalid_html(input_features['html_text']):
            self.__logger.info(f" --- save features domain ---")            
        
            # get features        
            features = self.get_features(domain_id)
            
            # manage features
            if features == []:
                # insert
                self.__logger.info(
                    f" --- insert features domain - {domain_id} ---")
                self.insert_feature(domain_id, date_, input_features)            
            # elif features[0]['domain_id'] == domain_id and features[0]['last_update'] != date_:
            #     # update
            #     self.__logger.info
            #     (f"--- update features domain - {domain_id} ---")
            #     self.update_features(domain_id, date_, input_features)
            # else:
            #     self.__logger.info(f"--- domain - {domain_id} is already updated ---")
        else:            
            self.__logger.info(f"--- domain - {domain_id} - captured html is invalid ---")  
            
            
    def insert_feature(self, domain_id, date_, input_features):
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
            sql_string = "INSERT INTO public.domain_features (domain_id, homepage_button, num_popups, html_text, last_update, exc_domain_id) VALUES (%s, %s, %s, %s, %s,%s);"

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, (domain_id, input_features['homepage_button'], input_features['num_popups'],
                                            input_features['html_text'], date_, input_features['exc_domain_id']))
                conn.commit()
            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to insert_feature - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
    
    def update_features(self, domain_id, date_, input_features):
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
            sql_string = "UPDATE public.domain_features SET homepage_button=%s, num_popups=%s, html_text=%s, last_update=%s WHERE domain_id=%s;"

            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, (input_features['homepage_button'], input_features['num_popups'],
                                            input_features['html_text'], date_, domain_id))
                conn.commit()
            except Exception as e:
                self.__logger.error('::Saver:: Error found trying to update_features - {}'.format(e))

            finally:
                cursor.close()
                conn.close()
                
    def check_invalid_html(self, text):
        string_list = [
            'Click Allow if you are not a robot',
            'The page will automatically redirect',
            'Index of / Index of / NameLast ModifiedSize Proudly Served by LiteSpeed Web Server at cinehdencasa.com Port 443',
            'Comprobando si la conexión del sitio es segura',
            'Checking if the site connection is secure',
            'Error code 520',
            'No webpage was found for the web address: ',
            'Domain Currently Under Maintenance',
            'This Account has been suspended',
            'Attention Attention Please install the Adblock Pro - Browser Extension to continue watching in safe mode',
            'This website has been reported for potential phishing',
            'Access to the requested content/website has been disabled',
            'Pulsa en Permitir ¡Haz clic en "Permitir" para confirmar que no eres un robot!',
            'The server is temporarily unavailable',
            'DDoS-GuardChecking your browser',
            'The domain has expired',
            'The page will automatically redirect to',
            'SHP Redirector',
            'Click "Allow" to confirm that you are not a robot',
            'This is the default index.html, this page is automatically generated by the system',
            'We would like to inform you that we have decided to shut down our site',
            'Web server is down',
            'This domain has recently been registered with Namecheap',
            'Adblocker is the ultimate ad blocker and the most advanced video ad blocker',
            'ERR_TUNNEL_CONNECTION_FAILED',
            'Domain Seized',
            'Pulsa en Permitir ¡Haz clic en "Permitir" para confirmar que no eres un robot!',
            'Reqzone.com - Movies, TV and Celebrities',
            'Erro 404"Home Page"',
            'Back to Cart',
            "Nigeria's Citadel Of Entertainment",
            'Not found',
            'Access Denied.',
            'DNS points to prohibited IP',
            '404 Not Found',
            'Your file is ready for download!',
            'No se pudo establecer tu preferencia',
            'Verifying that you are not a robot...',
            'Sorry, the page you were looking for does not exist or is not available',
            "can't reach this page",
            'Website is no Longer Available',
            '500 Internal Server',
            '502: Bad gateway',
            'oldal új címe',
            'You have consumed 100% of your monthly basic quota',
            'Blocked Page',
            'Cloudflare Please enable cookies',
            'Ads Blocker Detected!!! ',
            'Welcome to nginx!',
            "I'm not a robot",
            'The new site has been successfully created',
            'Domain Name Check & Register',
            'Este domínio será desativado em breve',
            'Connection timed out',
            'Free Safari Ad Blocker'
        ]
        
        # word count < 4
        if length:= len(text.split(" ")) < 4:
            return False
        ## pattern domain domain Copyright
        if re.findall(r"\w+ \w+ Copyright", text) and length == 4:
            return False
        for string in string_list:
            if string in text:
                return False
        return True