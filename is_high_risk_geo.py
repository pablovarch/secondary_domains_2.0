from dependencies import  log
import requests
import re
import constants, settings
from settings import db_connect
import psycopg2
import dns.resolver
from ipwhois import IPWhois
from datetime import datetime



class is_high_risk_geo :
    def __init__(self):
        self.__logger = log.Log().get_logger(name='is_high_risk_geo.log')

    def main(self):
        self.__logger.info('getting all secondary_domains')
        list_to_scan = self.get_all_secondary_domains()
        for dom in list_to_scan:
            sec_domain_id = dom['sec_domain_id']
            sec_domain = dom['sec_domain']
            self.__logger.info(f'------scrape site {dom} - ')


            dict_asn_ip_metrics = self.get_asn_ip_metrics(sec_domain)
            if dict_asn_ip_metrics:
                is_high_risk_geo = dict_asn_ip_metrics['is_high_risk_geo']
                self.update_secondary_domain(sec_domain_id,is_high_risk_geo)


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
            where sd.is_high_risk_geo is null 
            and sd.online_status = 'Online' """
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

    def update_secondary_domain(self, sec_domain_id,is_high_risk_geo ):
        try:
            conn = psycopg2.connect(host=db_connect['host'],
                                    database=db_connect['database'],
                                    password=db_connect['password'],
                                    user=db_connect['user'],
                                    port=db_connect['port'])
            cursor = conn.cursor()
        except Exception as e:
            print('::DBConnect:: cannot connect to DB Exception: {}'.format(e))
            raise
        else:

            sql_string = f"""
                       UPDATE public.secondary_domains
                       SET is_high_risk_geo = %s 
                       WHERE sec_domain_id = %s
                   """
            data = (is_high_risk_geo, sec_domain_id)
            try:
                cursor.execute(sql_string, data)
                conn.commit()
            except Exception as e:
                self.__logger.error(
                    f'::Saver:: Error updating status on secondary domains with id {sec_domain_id} - {e}')
            finally:
                cursor.close()
                conn.close()

    def get_asn_ip_metrics(self, domain: str) -> dict:
        """
        Dado un dominio, resuelve su IP y devuelve métricas de ASN y geolocalización:

          - ip: IP resuelta
          - asn: número de ASN
          - asn_age: años desde la asignación
          - ip_country: código de país del ASN
          - is_high_risk_geo: True si el país está en lista de alto riesgo
        """
        dict_asn_ip_metrics = {}
        try:
            # 1) Resolver dominio → IP
            try:
                answers = dns.resolver.resolve(domain, 'A')
                ip = answers[0].to_text()
            except Exception:
                return {}

            # 2) Consultar RDAP vía ipwhois
            obj = IPWhois(ip)
            rdap = obj.lookup_rdap(depth=1)
            asn = rdap.get('asn')
            country = rdap.get('asn_country_code')
            date_str = rdap.get('asn_date')  # 'YYYY-MM-DD'

            # 3) Calcular edad del ASN
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
                age = (datetime.utcnow() - dt).days / 365
            except Exception:
                age = None

            # 4) Clasificar jurisdicción de alto riesgo
            # Jurisdicciones prevalentes de bulletproof hosting según Wikipedia y Recorded Future
            high_risk_countries = {
                'RU',  # Russia
                'UA',  # Ukraine
                'CN',  # China
                'MD',  # Moldova
                'RO',  # Romania
                'BG',  # Bulgaria
                'BZ',  # Belize
                'PA',  # Panama
                'SC'  # Seychelles
            }
            is_high_risk = country in high_risk_countries

            # 5) Tipo de hosting (listado real de ASNs de bulletproof hosting)
            # Ejemplos extraídos de Intel471 y registros WHOIS públicos
            bulletproof_asns = {
                '197414',  # XHOST / Zservers :contentReference[oaicite:3]{index=3}
                '56873'  # ELITETEAM-PEERING-AZ2 :contentReference[oaicite:4]{index=4}
            }
            hosting_type = 'bullet-proof' if asn in bulletproof_asns else 'isp'

            # 6) Verificar en Spamhaus RBL
            rev_ip = '.'.join(reversed(ip.split('.')))
            try:
                dns.resolver.resolve(f'{rev_ip}.zen.spamhaus.org', 'A')
                in_rbl = True
            except dns.resolver.NXDOMAIN:
                in_rbl = False
            except Exception:
                in_rbl = None

            dict_asn_ip_metrics = {
                'asn_age': age,
                'ip_country': country,
                'is_high_risk_geo': is_high_risk,
            }
        except Exception as e:
            self.__logger.error(f'get_asn_ip_metrics: {e}')
        return dict_asn_ip_metrics