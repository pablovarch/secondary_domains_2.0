from dependencies import log
import constants
import psycopg2
import requests
import json
import re
from bs4 import BeautifulSoup
from settings import db_connect
from constants import kw_parking

class Status_checker:

    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

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
            
    def get_subdomains_oxy_api(self, domain_source, country):
        try:
            list_dom = []
            
            url = "https://realtime.oxylabs.io/v1/queries"
            query = f' site:{domain_source}'
            payload = json.dumps({
            "source": "google_search",
            "domain": "com",
            "query": query,
            "geo_location": country,
            "parse": True
            })
            headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Basic ZGF0YV9zY2llbmNlOm5xRHJ4UUZMeHNxNUpOOHpTekdwMg=='
            }

            response = requests.request("POST", url, headers=headers, data=payload)
            json_response = json.loads(response.text)
            json_result = json_response['results'][0]['content']['results']['organic']

            
            for elem in json_result:
                try:                    
                    domain_aux = re.findall(r'https?:\/\/([^\/]+)', elem['url'])[0]
                    if domain_source in domain_aux:
                        list_dom.append(domain_aux)                        
                except:
                    print(f'error compare domains {elem}')
            # delete duplicates list_dom
            list_dom = list(set(list_dom))

        except Exception as e:
            self.__logger.error(f" ::Get subdomains Error:: {e}")
        
        return list_dom

    def status_checker(self, page, site, list_ad_chains_url):
        try:
            current_url = page.url
            current_domain = re.findall(r'https?:\/\/([^\/]+)', current_url)[0]
            html = page.content()
        except:
            current_url = list_ad_chains_url[0]['url']
            current_domain = re.findall(r'https?:\/\/([^\/]+)', current_url)[0]
            html = page.content()

        pp_flag = self.check_paking_page(html, kw_parking, site)

        if pp_flag:
            self.__logger.info(f'the domain: {site} is a parking page ')
            status_dict = {
                'online_status': 'Offline | Status Checker',
                'offline_type': 'Parking Page',
                'redirect_url': '',
                'status_msg': ''
            }
            return status_dict
        else:

            status_dict = {}
            offline_type = ''
            status_msg = ''
            redirect = False
            same_domain = False
            # check redirect
            redirect_url = None
            if current_domain != site:
                redirect = True
                redirect_url = current_url
            if site in current_domain:
                same_domain = True

            if redirect and not same_domain:
                status_dict = {
                    'online_status': 'Offline | Status Checker',
                    'offline_type': 'Redirect',
                    'redirect_url': redirect_url,
                    'status_msg': ''
                }
                return status_dict

            try:
                # check bright data block
                if len(list_ad_chains_url) == 1:
                    ad_chain_url_status_code = list_ad_chains_url[0]['status']
                    if 400 < ad_chain_url_status_code < 500:
                        online_status = 'Offline'
                        if redirect:
                            offline_type = 'Redirect'
                        else:
                            offline_type = 'Error[400-500]'
                        status_dict = {
                            'online_status': online_status,
                            'offline_type': offline_type,
                            'redirect_url': redirect_url,
                            'status_msg': ''
                        }

                    else:
                        status_dict = self.check_html(html, ad_chain_url_status_code)

                else:
                    # check online
                    if list_ad_chains_url[0]['status'] == 200:
                        online_status = 'Online'
                        status_dict = {
                            'online_status': online_status,
                            'offline_type': offline_type,
                            'redirect_url': redirect_url,
                            'status_msg': ''
                        }
                        if 'Domain Seized' in html:
                            online_status = 'Blocked'
                            offline_type = 'Domain Seized'
                            status_dict = {
                                'online_status': online_status,
                                'offline_type': offline_type,
                                'redirect_url': redirect_url,
                                'status_msg': 'Domain Seized'
                            }

                    # check redirect
                    elif 299 < list_ad_chains_url[0]['status'] < 400:
                        found_flag = False
                        for ad_chain_url in list_ad_chains_url[1:10]:
                            if ad_chain_url['status'] == 200:
                                found_flag = True
                                first_domain = re.findall(r'https?:\/\/([^\/]+)', list_ad_chains_url[0]['url'])[0]
                                second_domain = re.findall(r'https?:\/\/([^\/]+)', ad_chain_url['url'])[0]
                                if first_domain in second_domain:
                                    online_status = 'Online'
                                    if redirect and same_domain:
                                        offline_type = 'Redirect - same domain'

                                    status_dict = {
                                        'online_status': online_status,
                                        'offline_type': offline_type,
                                        'redirect_url': redirect_url,
                                        'status_msg': 'Redirect same domain'
                                    }
                                    if 'Domain Seized' in html:
                                        online_status = 'Blocked'
                                        offline_type = 'Domain Seized'
                                        status_dict = {
                                            'online_status': online_status,
                                            'offline_type': offline_type,
                                            'redirect_url': redirect_url,
                                            'status_msg': 'Domain Seized'
                                        }

                                    break
                                else:
                                    online_status = 'Online'
                                    offline_type = 'Redirect'
                                    status_dict = {
                                        'online_status': online_status,
                                        'offline_type': offline_type,
                                        'redirect_url': redirect_url,
                                        'status_msg': ''
                                    }
                            # check redirect off line
                            if found_flag == False:
                                ad_chain_url_status_code = list_ad_chains_url[-1]['status']
                                status_dict = self.check_html(html, ad_chain_url_status_code)
                                status_dict['redirect_url'] = redirect_url
                            if status_dict['online_status'] != 'Online':
                                break


                    else:
                        # offline
                        ad_chain_url_status_code = list_ad_chains_url[0]['status']
                        status_dict = self.check_html(html, ad_chain_url_status_code)
                    # check parking page

            except:
                pass
        return status_dict

    def check_html(self, html, ad_chain_url_status_code):
        html = html.lower()
        soup = BeautifulSoup(html, 'html.parser')
        visible_text = soup.get_text()
        html = visible_text.lower()
        status_dict = {}
        try:
            status_msg = ''
            if 'blocked' in html and 'bright data usage policy' in html:
                online_status = 'Blocked'
                offline_type = f"Error[BrightData-{ad_chain_url_status_code}]"
                status_msg = 'bright data usage policy'
            elif 'webpage not available' in html or '404 not found' in html or 'this page isn’t working' in html:
                online_status = 'Offline | Ad Sniffer'
                offline_type = f"Error[{ad_chain_url_status_code}]"
            elif 'cloudflare' in html and 'ray id' in html:
                online_status = 'Blocked'
                offline_type = f"Error[Cloudflare-{ad_chain_url_status_code}]"
                status_msg = 'cloudflare'
            elif 'verifying you are human. this may take a few seconds' in html:
                online_status = 'Blocked'
                offline_type = f"Error[Cloudflare-{ad_chain_url_status_code}]"
                status_msg = 'cloudflare'
            # elif 'captcha' in html :
            #     online_status = 'Blocked'
            #     offline_type = f"Error[captcha-{ad_chain_url_status_code}]"
            #     status_msg = 'captcha'
            elif 'proxy authentication required' in html:
                online_status = 'Blocked'
                offline_type = f"Error[Proxy Authentication Required-{ad_chain_url_status_code}]"
                status_msg = 'proxy authentication required'
            elif 'domain seized' in html:
                online_status = 'Offline | Ad Sniffer'
                offline_type = f"Error[Domain Seized-{ad_chain_url_status_code}]"
                status_msg = 'Domain Seized'
            elif 'cannot establish connection to requested target' in html or 'could not resolve host https in html' in html:
                online_status = 'Offline | Ad Sniffer'
                offline_type = f"Error[not resolve host-{ad_chain_url_status_code}]"
            elif 'bad request' in html:
                online_status = 'Blocked'
                offline_type = f"Error[Bad Request-{ad_chain_url_status_code}]"
            elif 'auth failed (code: ip_forbidden)' in html:
                online_status = 'Blocked'
                offline_type = f"Error[auth failed-{ad_chain_url_status_code}]"
            elif 'sorry, you have been blocked' in html:
                online_status = 'Blocked'
                offline_type = f"blocked-{ad_chain_url_status_code}]"


            else:
                if ad_chain_url_status_code == 200:
                    online_status = 'Online'
                    offline_type = 'None'
                else:
                    online_status = 'Offline | Ad Sniffer'
                    offline_type = f"Error[{ad_chain_url_status_code}]"

            status_dict = {
                'online_status': online_status,
                'offline_type': offline_type,
                'status_msg': status_msg,
            }
        except:
            self.__logger.error(f'Error check_html - {ad_chain_url_status_code}')

        return status_dict

    def check_paking_page(self, html, kw_parking, domain):
        html = html.lower()
        soup = BeautifulSoup(html, 'html.parser')
        visible_text = soup.get_text()
        # html = visible_text.lower()
        status_dict = {}
        for kw in kw_parking:
            try:
                if kw in html and domain in html:
                    return True

            except Exception as e:
                self.__logger.error('Error found trying to check parking page - {}'.format(e))
        return False
    

        