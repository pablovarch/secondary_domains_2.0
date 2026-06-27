from dependencies import log
import constants
import psycopg2
import requests
import json
import re
from bs4 import BeautifulSoup
from settings import db_connect

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

        # domains that indicate a seized/blocked site regardless of HTTP status
        _SEIZED_DOMAINS = [
            'alliance4creativity.com',
        ]
        if any(d in current_url.lower() for d in _SEIZED_DOMAINS):
            self.__logger.info(f'[Status_checker] seized domain detected in URL: {current_url}')
            return {
                'online_status': 'Offline',
                'offline_type' : 'Domain Seized',
                'redirect_url' : None,
                'status_msg'   : current_domain,
            }

        raw_html = html.lower()
        soup_top = BeautifulSoup(html, 'html.parser')
        visible_text_top = soup_top.get_text()

        if self.is_parked_page(visible_text_top, raw_html=raw_html):
            self.__logger.info(f'the domain: {site} is a parking page')
            return {
                'online_status': 'Offline | Status Checker',
                'offline_type': 'Parking Page',
                'redirect_url': '',
                'status_msg': ''
            }

        status_dict = {}
        offline_type = ''
        status_msg = ''
        redirect = False
        same_domain = False
        redirect_url = None
        if current_domain != site:
            redirect = True
            redirect_url = current_url
        if site in current_domain:
            same_domain = True

        if redirect and not same_domain:
            return {
                'online_status': 'Offline | Status Checker',
                'offline_type': 'Redirect',
                'redirect_url': redirect_url,
                'status_msg': ''
            }

        try:
            # check bright data block
            if len(list_ad_chains_url) == 1:
                ad_chain_url_status_code = list_ad_chains_url[0]['status']
                if 400 < ad_chain_url_status_code < 500:
                    online_status = 'Offline'
                    offline_type = 'Redirect' if redirect else 'Error[400-500]'
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
                    if 'Domain Seized' in html or 'no longer available due to copyright' in html.lower():
                        status_dict = {
                            'online_status': 'Blocked',
                            'offline_type': 'Domain Seized',
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
                                offline_type_rd = 'Redirect - same domain' if (redirect and same_domain) else offline_type
                                status_dict = {
                                    'online_status': 'Online',
                                    'offline_type': offline_type_rd,
                                    'redirect_url': redirect_url,
                                    'status_msg': 'Redirect same domain'
                                }
                                if 'Domain Seized' in html or 'no longer available due to copyright' in html.lower():
                                    status_dict = {
                                        'online_status': 'Blocked',
                                        'offline_type': 'Domain Seized',
                                        'redirect_url': redirect_url,
                                        'status_msg': 'Domain Seized'
                                    }
                                break
                            else:
                                status_dict = {
                                    'online_status': 'Online',
                                    'offline_type': 'Redirect',
                                    'redirect_url': redirect_url,
                                    'status_msg': ''
                                }
                        # check redirect offline
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

        except:
            pass

        return status_dict

    def check_html(self, html, ad_chain_url_status_code):
        raw_html = html.lower()
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
            elif 'proxy authentication required' in html:
                online_status = 'Blocked'
                offline_type = f"Error[Proxy Authentication Required-{ad_chain_url_status_code}]"
                status_msg = 'proxy authentication required'
            elif 'domain seized' in html or 'no longer available due to copyright' in html:
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
            elif ad_chain_url_status_code == 200 and self.is_parked_page(visible_text, raw_html=raw_html):
                online_status = 'Offline'
                offline_type = 'Parked Domain'
                status_msg = 'parked page'
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

    def is_parked_page(self, visible_text: str, raw_html: str = '') -> bool:
        """
        Multi-tier parked page detection.

        Tier 1 — Raw HTML signals (parking networks, monetization iframes).
        Tier 2 — Hard visible-text signals (registrar phrases).
        Tier 3 — Scored soft signals + structural check (score >= 3 → parked).
        """
        text = visible_text.lower()

        import re as _re
        real_words = [w for w in _re.split(r'\s+', text)
                      if w and not _re.search(r'[{};:/=\\]', w)]
        real_word_count = len(real_words)

        # ── TIER 1: raw HTML — parking network / monetization domains ──────
        _T1_RAW = [
            ('quickresultseeker.com',    'OLA domain monetization'),
            ('cdn-fileserver.com/bping', 'OLA parking pixel'),
            ('l2type=dmola',             'OLA dmola flag'),
            ('vgd_l2type=dmola',         'OLA dmola flag'),
            ('sedoparking.com',          'Sedo parking'),
            ('sedo.com/search',          'Sedo search page'),
            ('parkingcrew.net',          'ParkingCrew'),
            ('bodis.com',                'Bodis monetization'),
            ('above.com',                'Above.com monetization'),
            ('undeveloped.com',          'Undeveloped.com'),
            ('sav.com',                  'Sav.com monetization'),
            ('domainsponsor.com',        'DomainSponsor'),
            ('oversee.net',              'Oversee.net'),
            ('fabulous.com',             'Fabulous parking'),
            ('domainactive.com',         'DomainActive'),
            ('parklogic.com',            'ParkLogic'),
            ('godaddy.com/parking',      'GoDaddy parking'),
            ('parked.godaddy.com',       'GoDaddy parking'),
        ]
        if raw_html:
            raw_lower = raw_html if raw_html == raw_html.lower() else raw_html.lower()
            for signal, label in _T1_RAW:
                if signal in raw_lower:
                    self.__logger.info(
                        f'[Status_checker] parked — T1 raw HTML: {label} ({signal})'
                    )
                    return True

        # ── TIER 2: hard visible-text phrases ──────────────────────────────
        _T2_HARD = [
            'this domain is for sale',
            'buy this domain',
            'domain for sale',
            'make an offer on this domain',
            'this domain is available for purchase',
            'this domain is parked',
            'domain parking',
            'parked by',
            'this web page is parked free',
            'parked free, courtesy of godaddy',
            'this domain has been registered',
            'this domain has recently been registered',
            'this domain may be for sale',
            'godaddy.com parking',
            'sedo.com',
            'hugedomains.com',
            'dan.com',
            'afternic.com',
            'this account has been suspended',
            'website has been suspended',
            'account suspended',
            'this site has been disabled',
            'domain seized',
            'no longer available due to copyright',
            'future home of something quite cool',
            'proudly hosted by litespeed',
            'do not sell or share my personal information',
        ]
        for s in _T2_HARD:
            if s in text:
                self.__logger.info(f'[Status_checker] parked — T2 hard text: "{s}"')
                return True

        # ── TIER 3: scored soft signals ────────────────────────────────────
        _T3_SOFT = [
            ('namecheap',           2),
            ('bluehost',            2),
            ('hostgator',           2),
            ('network solutions',   2),
            ('register.com',        2),
            ('godaddy',             2),
            ('hostinger',           2),
            ('ionos',               2),
            ('this domain',         1),
            ('domain name',         1),
            ('register your domain',1),
            ('domain registration', 1),
            ('renew your domain',   1),
            ('web hosting',         1),
            ('coming soon',         1),
            ('under construction',  1),
            ('site coming soon',    1),
            ('launching soon',      1),
            ('find your next',      1),
            ('expired domain',      1),
            ('pending delete',      1),
            ('buy now',             1),
        ]
        score = sum(w for s, w in _T3_SOFT if s in text)

        if real_word_count < 50:
            score += 2
        elif real_word_count < 120:
            score += 1

        if raw_html and ('height: 100vh' in raw_html or 'height:100vh' in raw_html):
            score += 2

        if score >= 3:
            self.__logger.info(
                f'[Status_checker] parked — T3 score={score} '
                f'(real_words={real_word_count})'
            )
            return True

        return False
    

        