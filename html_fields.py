from dependencies import  log
from settings import db_connect
import psycopg2
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
import re


class html_fields:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='ad_count.log')

    def main(self):
        self.__logger.info('getting all secondary_domains')

        list_to_scan = self.get_all_secondary_domains()
        for dom in list_to_scan:
            sec_domain_id = dom['sec_domain_id']
            sec_domain = dom['sec_domain']
            try:
                self.__logger.info(f'------get html from site {dom}')
                html = self.get_html(sec_domain_id)
                if html:
                    result_detect_ecommerce_signals = self.detect_ecommerce_signals(html)
                    result_detect_affiliate_handoffs = self.detect_affiliate_handoffs(html)
                    count_ad = self.count_ad_slots_from_html(html)
                    print(result_detect_ecommerce_signals, result_detect_affiliate_handoffs,count_ad)
                else:
                    self.__logger.info('site has not html')



            except Exception as e:
                self.__logger.error(f'Error getting ad_count for - {dom}')

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
            sql_string = """SELECT  distinct sd.sec_domain_id , sd.sec_domain  
                            FROM secondary_domains sd 
                            inner join secondary_domains_html sdh on sd.sec_domain_id = sdh.sec_domain_id 
                            where sd.ad_count is null 
                            and sd.online_status = 'Online'; """
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

    def get_html(self, sec_domain_id):
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
            sql_string = """select sdh.html_content from secondary_domains_html sdh where sdh.sec_domain_id = %s """
            data = (sec_domain_id,)
            html = None
            try:
                # Try to execute the sql_string to save the data
                cursor.execute(sql_string, data)
                respuesta = cursor.fetchone()
                conn.commit()

                if respuesta:
                    html = respuesta[0]

            except Exception as e:
                self.__logger.error(':::: Error found trying to get_html'.format(e))

            finally:
                cursor.close()
                conn.close()
                return html

    def _is_affiliate_link(self, url: str) -> bool:
        """True si URL coincide con patrón afiliado."""


        AFFILIATE_DOMAINS = [
            # redes y acortadores frecuentes
            'amazon.', 'amzn.to', 'clickbank.', 'shareasale.', 'cj.com',
            'awin1.', 'impact.com', 'partnerize.', 'rakuten.', 'linksynergy.',
            'ebay.', 'glnk.io', 'go2cloud.', 'rstyle.me', 'go.redirectingat.com'
        ]

        AFFILIATE_QUERY_KEYS = [
            'affid', 'affiliate', 'aff', 'ref', 'refid', 'subid', 'tag',
            'utm_medium', 'utm_campaign'
        ]
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if any(dom for dom in AFFILIATE_DOMAINS if dom in host):
            return True
        params = parse_qs(parsed.query)
        for key in params:
            if key.lower() in AFFILIATE_QUERY_KEYS:
                return True
        return False

    def detect_affiliate_handoffs(self, html: str ) -> dict:
        """Devuelve dict con enlaces de afiliado detectados."""
        min_links: int = 1
        soup = BeautifulSoup(html, "html.parser")
        affiliate_links = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if self._is_affiliate_link(href):
                affiliate_links.append(href)

        return {
            "links": affiliate_links,
            "has_affiliate_handoff": len(affiliate_links) >= min_links
                                }

    def detect_ecommerce_signals(self,html: str) -> dict:
        """Devuelve dict con señales e-commerce encontradas."""
        min_hits: int = 2
        soup = BeautifulSoup(html, "html.parser")
        signals = []
        # --------- LISTAS DE PATRONES ---------
        ECOM_CDNS = [
            r'cdn\.shopify\.com', r'wp-content/plugins/woocommerce',
            r'checkout\.shopify\.com', r'cart\.js', r'mage/.*\.js',
            r'opencart', r'bigcommerce', r'squarespace-commerce',
            r'paypal\.com/sdk', r'stripe\.com'
        ]

        ECOM_FORM_HINTS = [r'cart', r'checkout', r'order', r'payment', r'wp-cart']

        ECOM_CLASS_HINTS = [r'add[-_]to[-_]cart', r'cart-btn', r'btn-buy', r'product-price']

        # 1) JSON-LD Product / Offer
        for script in soup.find_all("script", type="application/ld+json"):
            if re.search(r'"@type"\s*:\s*"(Product|Offer|AggregateOffer)"', script.string or '', re.I):
                signals.append("jsonld_product")
                break

        # 2) Assets de plataformas
        for tag in soup.find_all(src=True):
            for pat in ECOM_CDNS:
                if re.search(pat, tag["src"], re.I):
                    signals.append("ecom_asset:" + pat)
                    break

        # 3) Formularios de carrito / checkout
        for form in soup.find_all("form", action=True):
            if any(hint in form["action"].lower() for hint in ECOM_FORM_HINTS):
                signals.append("ecom_form")
                break

        # 4) Botones o enlaces con clases/ids típicos
        for elem in soup.find_all(True, {"class": True}):
            classes = " ".join(elem.get("class", [])).lower()
            if any(re.search(cls, classes) for cls in ECOM_CLASS_HINTS):
                signals.append("ecom_ui_hint")
                break

        return {
            "signals": signals,
            "is_ecommerce": len(signals) >= min_hits
        }

    def count_ad_slots_from_html(self,html: str) -> int:
        soup = BeautifulSoup(html, "html.parser")

        # Etiquetas a revisar
        ad_tags = soup.find_all(["iframe", "div", "section", "ins"])
        count = sum(1 for tag in ad_tags if self.looks_like_ad(tag))

        return count

    def looks_like_ad(self, tag):

        # Palabras clave para detectar publicidad
        AD_KEYWORDS = [
            "ad", "ads", "advert", "banner", "sponsored", "promo", "pub",
            "anuncio", "publicidad", "annonce", "werbung"
        ]

        # Patrones comunes en URLs de publicidad
        AD_SRC_PATTERNS = [
            "doubleclick", "googlesyndication", "adnxs", "adservice", "adsystem", "criteo", "taboola",
            "outbrain"
        ]

        attrs_to_check = [
            tag.get("id", ""),
            tag.get("class", ""),
            tag.get("name", ""),
            tag.get("src", ""),
            tag.get("data-*", "")
        ]
        flat_attrs = " ".join(
            [a if isinstance(a, str) else " ".join(a) for a in attrs_to_check]
        ).lower()

        # Heurística por palabras clave
        if any(re.search(rf"\b{k}\b", flat_attrs) for k in AD_KEYWORDS):
            return True

        # Heurística por URLs conocidas
        src = tag.get("src", "").lower()
        if any(p in src for p in AD_SRC_PATTERNS):
            return True

        return False