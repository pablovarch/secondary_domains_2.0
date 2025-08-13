import re
from dependencies import log, playwright_automation, status_checker
from models import html_features
import time
import constants, settings
import json
import random
from bs4 import BeautifulSoup
from playwright.sync_api import Playwright, sync_playwright, BrowserType
from playwright.sync_api import expect

class Playwright_traffic:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])
        self.__playwright_automation = playwright_automation.Playwright_automation()
        self.__status_checker = status_checker.Status_checker()
        self.__html_features = html_features.html_features()


    def capture_traffic(self, page, site):
        try:
            # Crea una lista vacía para almacenar los datos de las solicitudes HTTP
            list_ad_chains_url = []
            list_bids = []
            list_vast = []
            list_current_url = []
            list_pop = []
            final_list_pop = []
            list_screenshots_traffic = []
            list_mhtml_traffic = []
            go_to_home_page = False
            self.__logger.info(f" --- start capture traffic playwright chromium ---")

            def handle_response(response):
                try:
                    # Extraer los datos de la respuesta y agregarlos a la lista "responses"
                    status = response.status
                    url = response.url
                    headers = response.headers
                    current_url_load = page.url
                    list_current_url.append(current_url_load)

                    # extrae todas urls de las responses y las agrega a la lista
                    data_url = {
                        "status": status,
                        "url": url,
                        "post_clic": False
                    }
                 


                    list_ad_chains_url.append(data_url)

                    # try to capture vast
                    try:
                        texto_plano = response.text()

                        # try capture vast xml
                        if 'vast' in texto_plano and 'xml version' in texto_plano:
                            vast_xml_dict = {
                                "url": url,
                                'vast_content': texto_plano
                            }
                            print('save xml dict')
                            list_vast.append(vast_xml_dict)
                    except:
                        pass

                    # try capture prebid
                    try:
                        resp_json = None
                        resp_json = response.json()

                        string_json = json.dumps(resp_json)
                        if '"cpm":' in string_json:
                            ad_domain = None
                            ad_subdomain = None
                            ad_full_domain = None
                            ad_buyer = None
                            try:
                                ad_full_domain = re.findall(r'^(?:https?:\/\/)?(?:[^@\n]+@)?(?:www\.)?([^:\/\n?]+)', url)[0]
                                list_split_domains = ad_full_domain.split('.')
                                if len(list_split_domains) == 3:
                                    ad_subdomain = list_split_domains[0]
                                    ad_domain = list_split_domains[1] + '.' + list_split_domains[2]
                                else:
                                    self.__logger.error('--- Navigation Playwright :: lengt list domain > 3')
                            except Exception as e:
                                self.__logger.error(f'--- Navigation Playwright :: cant get domain - error: {e} --')
                            ad_publisher = None
                            try:
                                ad_publisher = re.findall(r"publisher:\s*'([^']+)'", string_json)[0]
                            except Exception as e:
                                pass
                                # self.__logger.error(f'--- Navigation Playwright :: cant get ad_publisher - error: {e} --')
                            try:
                                ad_buyer = re.findall(r"\/\/([^\/]+)", string_json)[0]
                            except Exception as e:
                                pass
                                # self.__logger.error(f'--- Navigation Playwright :: cant get ad_buyer - error: {e} --')

                            response_data = {
                                "site": site,
                                "status": status,
                                "url": url,
                                "ad_domain": ad_domain,
                                "ad_subdomain": ad_subdomain,
                                "ad_full_domain": ad_full_domain,
                                "ad_publisher": ad_publisher,
                                "ad_buyer": ad_buyer,
                                "resp_json": resp_json
                            }
                            list_bids.append(response_data)

                    except Exception as e:
                        pass
                        # self.__logger.error(f'Playwright::handle_response on iterate json response - error {e}')

                except Exception as e:
                    self.__logger.error(f'Playwright::handle_response -  error {e}')

            # Subscribe al evento "response"
            page.on("response", handle_response)

            # Navega a una página web para capturar el tráfico de red
            site_to_load = f'https://{site}'
            # page.goto(site_to_load, wait_until="networkidle")
            load_site = False
            tries=1
            while tries < 3:
                try:
                    page.goto(site_to_load, wait_until='load', timeout=50000)
                    page.wait_for_selector("body", timeout=15000)
                    # page.wait_for_load_state(timeout=50000)
                    load_site = True
                    break
                except Exception as e:
                    errores_ssl = ['ERR_']
                    if any(err in str(e) for err in errores_ssl):
                        print('error de certificado')
                        dict_feature_domain = None
                        html_features = None
                        try:
                            status_msg = e.message.split("::")[1].split('at')[0]
                        except Exception as exc:
                            status_msg = 'Error cert'
                        status_dict = {
                            'online_status': 'Blocked',
                            'offline_type': 'Error cert',
                            'redirect_url': '',
                            'status_msg': status_msg
                        }

                        return status_dict, dict_feature_domain, html_features
                    self.__logger.error(f'error load page {e}')
                    tries += 1
            # site_url = page.url


            # check status page
            status_dict = self.__status_checker.status_checker(page, site, list_ad_chains_url)
            
            # list_page_tag = self.__page_tag.capture_page_tags(page)
            
            # try to acept cookies and terns of use
            self.click_consent_and_cookie(page)
            
            # Get all popups when they open
            def handle_popup(popup):

                def handle_response_popup(response_popup):
                    url_popup = response_popup.url
                    list_pop.append(url_popup)
                    # print(url_popup)
                    try:
                        # Extraer los datos de la respuesta y agregarlos a la lista "responses"
                        status = response_popup.status
                        url = response_popup.url
                        headers = response_popup.headers

                        current_url_load = page.url
                        list_current_url.append(current_url_load)

                        # extrae todas urls de las responses y las agrega a la lista
                        data_url = {
                            "status": status,
                            "url": url,
                            "post_clic": False
                        }
                        list_ad_chains_url_pop.append(data_url)

                        # try to capture vast
                        try:
                            texto_plano = response_popup.text()

                            # try capture vast xml
                            if 'vast' in texto_plano and 'xml version' in texto_plano:
                                vast_xml_dict = {
                                    "url": url,
                                    'vast_content': texto_plano
                                }
                                list_vast_pop.append(vast_xml_dict)
                        except:
                            pass

                        # try capture prebid
                        try:
                            resp_json = None
                            resp_json = response_popup.json()

                            string_json = json.dumps(resp_json)
                            if '"cpm":' in string_json:
                                ad_domain = None
                                ad_subdomain = None
                                ad_full_domain = None
                                ad_buyer = None
                                try:
                                    ad_full_domain = \
                                        re.findall(r'^(?:https?:\/\/)?(?:[^@\n]+@)?(?:www\.)?([^:\/\n?]+)', url)[0]
                                    list_split_domains = ad_full_domain.split('.')
                                    if len(list_split_domains) == 3:
                                        ad_subdomain = list_split_domains[0]
                                        ad_domain = list_split_domains[1] + '.' + list_split_domains[2]
                                    else:
                                        self.__logger.error('--- Navigation Playwright :: lengt list domain > 3')
                                except Exception as e:
                                    self.__logger.error(f'--- Navigation Playwright :: cant get domain - error: {e} --')
                                ad_publisher = None
                                try:
                                    ad_publisher = re.findall(r"publisher:\s*'([^']+)'", string_json)[0]
                                except Exception as e:
                                    pass
                                    # self.__logger.error(f'--- Navigation Playwright :: cant get ad_publisher - error: {e} --')
                                try:
                                    ad_buyer = re.findall(r"\/\/([^\/]+)", string_json)[0]
                                except Exception as e:
                                    pass
                                    # self.__logger.error(f'--- Navigation Playwright :: cant get ad_buyer - error: {e} --')

                                response_data = {
                                    "site": site,
                                    "status": status,
                                    "url": url,
                                    "ad_domain": ad_domain,
                                    "ad_subdomain": ad_subdomain,
                                    "ad_full_domain": ad_full_domain,
                                    "ad_publisher": ad_publisher,
                                    "ad_buyer": ad_buyer,
                                    "resp_json": resp_json
                                }
                                list_bids_pop.append(response_data)
                                

                        except Exception as e:
                            pass
                            # self.__logger.error(f'Playwright::handle_response on iterate json response - error {e}')

                    except Exception as e:
                        self.__logger.error(f'Playwright::handle_response -  error {e}')


                list_ad_chains_url_pop = []
                list_vast_pop = []
                list_bids_pop = []
                popup.on("response", handle_response_popup)
                # manage popup Wait for the popup to load
                for i in range(3):
                    try:                
                        # popup.wait_for_load_state(timeout=20000)
                        popup.wait_for_selector("body", timeout=15000)
                        self.__logger.info(f'--- wait for load state popup --- success')
                        break
                    except Exception as e:  
                        self.__logger.error(f'error wait for load state popup {e}')
                        i+=1
                        time.sleep(5)
                try:
                    html = popup.content()
                except Exception as e:
                    html = None
                    self.__logger.error(f'error capture html {e}')
                
                dict_pop = {
                    'url': popup.url,
                    'list_ad_chains_url_pop': list_ad_chains_url_pop,
                    'list_vast_pop': list_vast_pop,
                    'list_bids_pop': list_bids_pop,
                    'html': html

                }
                final_list_pop.append(dict_pop)
            page.on("popup", handle_popup)

            list_mhtml_traffic, go_to_home_page = self.__playwright_automation.check_home_page(page, list_mhtml_traffic)

            # try get terms of use
            terms = self.capture_terms(page)
            print(terms)
                                      
            user_agent = page.evaluate("() => navigator.userAgent")
            html = page.content()
            html_clean = self.extract_visible_text_from_html(html)
            # check bright data block    
            # status_dict = self.check_page(html, list_ad_chains_url)

            # delete duplicates from list current url
            html_features =  self.__html_features.main(page,site)

            num_popups = len(final_list_pop)
            try:
                client = page.context.new_cdp_session(page)
                mhtml = client.send("Page.captureSnapshot")['data']
            except Exception as e:
                mhtml = None
                self.__logger.error(f'error capture mhtml {e}')
            dict_feature_domain = {
                 'homepage_button': go_to_home_page,
                 'num_popups': num_popups,
                 'mhtml': mhtml,
                 'html':html
            }

            dict_feature_domain = {**dict_feature_domain, **terms}

            final_list_pop = self.clean_list_popup(final_list_pop)
            
            status_dict = self.recheck_status_dict(page, site, list_ad_chains_url, status_dict)


            return status_dict, dict_feature_domain, html_features
        except Exception as e:
            # self.__logger.error(f'Capture trafic - Error: {e}')
            raise

    def check_page(self, html, list_ad_chains_url):
        # check bright data block
        status_dict = {}
        offline_type = ''
        try:
            if len(list_ad_chains_url) == 1:
                if 'blocked' in html and 'Bright Data usage policy' in html:
                    online_status = 'Blocked'
                    offline_type = f"Error[BrightData-{list_ad_chains_url[0]['status']}]"
                elif 'Webpage not available' in html:
                    online_status = 'Offline'
                    offline_type = f"Error[{list_ad_chains_url[0]['status']}]"
                elif 'cloudflare' in html.lower():
                    online_status = 'Blocked'
                    offline_type = f"Error[Cloudflare-{list_ad_chains_url[0]['status']}]"
                else:
                    online_status = 'Offline'
                    offline_type = f"Error[{list_ad_chains_url[0]['status']}]"

            else:
                # check online
                if list_ad_chains_url[0]['status'] == 200:
                    online_status = 'Online'
                # check redirect
                elif 299 < list_ad_chains_url[0]['status'] < 400:
                    for ad_chain_url in list_ad_chains_url[1:10]:
                        if ad_chain_url['status'] == 200:
                            first_domain = re.findall(r'https?:\/\/([^\/]+)', list_ad_chains_url[0]['url'])[0]
                            second_domain = re.findall(r'https?:\/\/([^\/]+)', ad_chain_url['url'])[0]
                            if first_domain in second_domain:
                                online_status = 'Online'
                                break
                            else:
                                online_status = 'Online'
                                offline_type = 'Redirect'
                    # else:
                    #     # blocked
                    #     online_status = 'Blocked'
                    #     offline_type = f"Webpage not available - {list_ad_chains_url[1]['status']}"
                else:
                    if 'access was blocked as it might breach Bright Data usage policy' in html:
                        online_status = 'Blocked'
                        offline_type = f"Error[BrightData-{list_ad_chains_url[0]['status']}]"
                    elif 'Webpage not available' in html:
                        online_status = 'Offline'
                        offline_type = f"Error[{list_ad_chains_url[0]['status']}]"
                    elif 'cloudflare' in html.lower():
                        online_status = 'Blocked'
                        offline_type = f"Error[Cloudflare-{list_ad_chains_url[0]['status']}]"
                    else:
                        online_status = 'Offline'
                        offline_type = f"Error[{list_ad_chains_url[0]['status']}]"
            status_dict = {
                'online_status': online_status,
                'offline_type': offline_type
            }
        except:
            pass
        return status_dict

    def update_ad_chain_post(self, list_ad_chains_url, list_ad_ch_post):
        try:
            for chain_pre in list_ad_chains_url:
                for chain_post in list_ad_ch_post:
                    if chain_pre['url'] == chain_post['url']:
                        chain_pre['post_clic'] = True
                        break
            return list_ad_chains_url
        except Exception as e:
            print(f'error update ad_chain_post {e}')

    def deleted_dupplicates_current_url(self, list_current_url):
        try:
            list_clean = []
            prev_url = ''
            for url in list_current_url:
                if url != prev_url:
                    list_clean.append(url)
                    prev_url = url
            list_clean.pop(0)
            return list_clean


        except Exception as e:
            print(f'error deleted_dupplicates_current_url {e}')
            return list_clean

    def test_locator(self, page):
        try:
            first_list = []
            # Utiliza el locator para obtener todas las etiquetas <a>
            anchor_locator = page.locator("a").all()
            current_url = page.url
            try:
                domain_url_page = re.findall(r'https?:\/\/([^\/]+)', current_url)[0]
            except:
                pass

            # Itera sobre los elementos y obtén los atributos href
            for anchor in anchor_locator:
                try:
                    href = anchor.get_attribute("href")
                    if domain_url_page in href:
                        first_list.append(anchor)
                        # print(href)
                        # anchor.click()
                except:
                    pass
            for elem in first_list:
                href = elem.get_attribute("href")
                print(href)
        except Exception as e:
            print(e)
    
    def extract_visible_text_from_html(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Eliminar scripts, estilos y otros elementos no deseados
        for script in soup(["script", "style", "meta", "link", "noscript"]):
            script.extract()

        # Obtener texto
        text = soup.get_text()

        # Eliminar espacios en blanco adicionales
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text_cleaned = ' '.join(chunk for chunk in chunks if chunk)
        # Remove HTML code (if still present)
        text_cleaned = re.sub(r'<.*?>', '', text_cleaned)
        # Delete emojis
        text_cleaned = re.sub(r'[\U00010000-\U0010ffff]', '', text_cleaned)

        return text_cleaned
    
    def clean_list_popup(self, final_list_pop):
        for elem in final_list_pop:
            # delete chrome-error://chromewebdata/ , about:blank
            if 'chrome-error://chromewebdata/' == elem['url'] or 'about:blank' == elem['url']:  
                final_list_pop.remove(elem)                        
        return final_list_pop
    
    def capture_terms(self, page): 
        dict_term = {
            'terms_of_use': False,
            'dmca': False,
            'privacy_policy': False,
            # 'contact': False,            
        }       
        
        try:           
            self.__logger.info(f'--try to capture terms --')
            html = str(page.content()).lower()
            if 'terms' in html or 'tos' in html:
                dict_term['terms_of_use'] = True
            if 'disclaimer' in html or 'dmca' in html:
                dict_term['dmca'] = True
            if 'privacy policy' in html or 'privacy' in html or 'policy' in html :
                dict_term['privacy_policy'] = True
            # if 'contact' in html:
            #     dict_term['contact'] = True          
                        
        except Exception as e:
            self.__logger.error(f'Playwright_::go_to_home_page -  error {e}')
        return dict_term 
                           
    def click_consent_and_cookie(self, page):
        string_keywords = '-ok-ok, i accept-ok i agree-agree-allow-accept all-aceptar todo-aceptar-agree all-agree-accept-aceptar-allow all-allow-i accept cookies-close [x]-got it!-yes, please-si, por favor-'
        
        tags = ['button', 'link']
        found_flag = False

        def click_element(elements):
            nonlocal found_flag
            for element in elements:
                handle = element.element_handle()
                text = handle.inner_text().lower()
                # if text in keywords or keywords in text:
                if '-'+text+'-' in string_keywords:
                    found_flag = True
                    try:
                        element.click()
                        self.__logger.info(f'click successful on {text}')
                        break
                    except Exception as e:
                        self.__logger.error(f'error click on {text} - error {e}')

        try:
            for tag in tags:
                elements = page.get_by_role(tag).all()
                if elements: 
                    elements = self.clean_list_consent(page, elements)                   
                    click_element(elements)
                    if found_flag:
                        break
        except Exception as e:
            self.__logger.error(f'Playwright_traffic::click_consent_and_cookie -  error {e}')    

    def clean_list_consent(self, page, elements):
        try:
            list_elements = []
            for element in elements:
                handle = element.element_handle()
                text = handle.inner_text().lower()                                
                if  text and len(text)< 15 and len(text) > 1:
                    list_elements.append(element)  
                    # print(f'text: {text}')          
        except Exception as e:
            self.__logger.error(f'Playwright::clean_list_consent -  error {e}')
        return list_elements
        
    def recheck_status_dict(self, page, site, list_ad_chains_url, first_status_dict):
        status_dict = {}
        try:
            status_dict = self.__status_checker.status_checker(page, site, list_ad_chains_url)
            if first_status_dict['online_status'] == status_dict['online_status'] and first_status_dict['offline_type'] == status_dict['offline_type']:
                last_status_dict = first_status_dict
            else:
                last_status_dict = status_dict
            
        except Exception as e:
            self.__logger.error(f'Playwright::recheck_status_dict -  error {e}') 
        return last_status_dict  
    
    