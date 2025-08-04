from operator import length_hint

from dependencies import log
import constants, settings
import re
from bs4 import BeautifulSoup
from collections import deque
import subprocess
import json
from typing import Dict
import dns.resolver
from ipwhois import IPWhois
from datetime import datetime
from settings import db_connect, api_key
import psycopg2
import pandas as pd
import requests

class html_features:
    def __init__(self):
        self.__logger = log.Log().get_logger(name=constants.log_file['log_name'])

    def main(self, page, site):

        html = page.content()
        dict_depth_metrics = self.get_dom_depth_metrics(html)
        dict_inline_script_metrics = self.get_inline_script_metrics(html)
        dict_schema_org_metrics = self.get_schema_org_metrics_2(html)
        dict_asn_ip_metrics = self.get_asn_ip_metrics(site)
        dict_cookie_wall_metrics = self.get_cookie_wall_metrics(page)
        dict_lighthouse_metrics = self.get_lighthouse_metrics(site)
        result_tags = self.count_html_tags_and_text( html)
        length_html= self.get_html_lenght(page)
        count_ad_script_src = self.count_ad_script_src(html, constants.ad_domains)

        all_metrics = {
            **dict_depth_metrics,
            **dict_inline_script_metrics,
            **dict_schema_org_metrics,
            **dict_asn_ip_metrics,
            **dict_cookie_wall_metrics,
            **dict_lighthouse_metrics,
            **result_tags,
            'length_html': length_html,
            'count_ad_script_src': count_ad_script_src
        }
        return all_metrics

    # Profundidad media del árbol DOM: Punto 1
    def get_dom_depth_metrics(self ,html: str) -> dict:
        """
        Recorre el DOM representado en `html` y devuelve:
          - avg_depth: profundidad media
          - max_depth: profundidad máxima
          - node_count: número total de nodos
        """
        dict_dom_depth_metrics = {}
        try:
            soup = BeautifulSoup(html, "lxml")
            # Usamos deque para rendimiento en FIFO
            queue = deque([(soup, 0)])
            total_depth = 0
            node_count  = 0
            max_depth   = 0

            while queue:
                node, depth = queue.popleft()
                node_count += 1
                total_depth += depth
                max_depth = max(max_depth, depth)

                # Solo consideramos children que aportan estructura
                for child in node.find_all(recursive=False):
                    queue.append((child, depth + 1))

            avg_depth = total_depth / node_count if node_count else 0
            dict_dom_depth_metrics = {
                "avg_depth": avg_depth,
                "max_depth": max_depth,
                "node_count": node_count
            }
        except Exception as e:
            self.__logger.error(f'get_dom_depth_metrics: {e}')
        return dict_dom_depth_metrics

     #úmero y tamaño de scripts inline	Scripts incrustados entre <script> … </script> sin atributo src y su longitud (bytes).
    def get_inline_script_metrics(self, html: str) -> dict:
        """
        Calcula el número y el tamaño total (en bytes) de los scripts inline
        (<script>…</script> sin atributo src) en un fragmento de HTML.

        Parámetros:
            html (str): Cadena con el HTML de la página.

        Retorna:
            dict: {
                'inline_script_count': int,   # número de scripts inline
                'inline_script_bytes': int    # tamaño total en bytes de su contenido
            }
        """
        dict_inline_script_metrics = {}
        try:
            soup = BeautifulSoup(html, 'lxml')

            # Filtrar solo los <script> sin atributo src
            inline_scripts = [tag for tag in soup.find_all('script') if not tag.get('src')]

            count = len(inline_scripts)

            total_bytes = 0
            for tag in inline_scripts:
                # Obtener todo el texto dentro del script
                content = tag.string or tag.get_text() or ""
                # Medir en bytes UTF-8
                total_bytes += len(content.encode('utf-8'))

            dict_inline_script_metrics = {
                'inline_script_count': count,
                'inline_script_bytes': total_bytes
            }
        except Exception as e:
            self.__logger.error(f'get_inline_script_metrics: {e}')
        return dict_inline_script_metrics

    #Pistas de contenido y lenguaje Uso de Schema.org
    def get_schema_org_metrics(self, html: str) -> dict:
        """
        Extrae y analiza los bloques JSON-LD de Schema.org en el HTML.

        Parámetros:
            html (str): HTML completo de la página.

        Retorna:
            dict: {
                'has_schema': bool,               # Si existe al menos un bloque JSON-LD
                'total_schema_blocks': int,       # Número total de <script type="application/ld+json">
                'schema_types_count': int,        # Cantidad de tipos @type únicos detectados
                'schema_movie_complete': bool     # True si hay al menos un bloque Movie con ≥80% de props requeridas
            }
        """
        dict_schema_org_metrics = {}
        try:
            soup = BeautifulSoup(html, "lxml")
            scripts = soup.find_all("script", type="application/ld+json")

            types_set = set()
            org_count = lic_count = cr_count = 0
            movie_complete = False

            # Propiedades consideradas "requeridas" para un Movie
            required_movie_props = {
                "@type", "name", "description", "image",
                "datePublished", "director", "actor",
                "genre", "contentRating", "provider"
            }

            for tag in scripts:
                try:
                    data = json.loads(tag.string or "")
                except (json.JSONDecodeError, TypeError):
                    continue

                # JSON-LD puede ser lista o dict único
                entries = data if isinstance(data, list) else [data]
                for entry in entries:
                    t = entry.get("@type")
                    if not t:
                        continue
                    # @type puede ser lista
                    if isinstance(t, list):
                        for sub in t:
                            types_set.add(sub)
                    else:
                        types_set.add(t)

                    # Contar tipos específicos
                    if t == "Organization":
                        org_count += 1
                    elif t == "License":
                        lic_count += 1
                    elif t == "ContentRating":
                        cr_count += 1

                    # Evaluar completitud para Movie
                    if t == "Movie":
                        present = required_movie_props.intersection(entry.keys())
                        completeness = len(present) / len(required_movie_props)
                        if completeness >= 0.8:
                            movie_complete = True

            dict_schema_org_metrics = {
                "has_schema": bool(types_set),
                "total_schema_blocks": len(scripts),
                "schema_types_count": len(types_set),
                "schema_movie_complete": movie_complete
            }
        except Exception as e:
            self.__logger.error(f'event_blocker_metrics: {e}')
        return dict_schema_org_metrics

    # Redes, hosting y reputación ASN y geolocalización de la IP
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
            obj  = IPWhois(ip)
            rdap = obj.lookup_rdap(depth=1)
            asn      = rdap.get('asn')
            country  = rdap.get('asn_country_code')
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
                'SC'   # Seychelles
            }
            is_high_risk = country in high_risk_countries

            # 5) Tipo de hosting (listado real de ASNs de bulletproof hosting)
            # Ejemplos extraídos de Intel471 y registros WHOIS públicos
            bulletproof_asns = {
                '197414',  # XHOST / Zservers :contentReference[oaicite:3]{index=3}
                '56873'    # ELITETEAM-PEERING-AZ2 :contentReference[oaicite:4]{index=4}
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

    #“Cookie wall” o “disable ad-block” overlays
    def get_cookie_wall_metrics(self, page) -> dict:
        """
        Detecta overlays de “cookie wall” / anti-adblock y mide la proporción
        de área cubierta respecto al viewport, además de un flag de detección
        de anti-adblock en scripts.

        Parámetros:
            page (Page): Instancia de Playwright ya navegada a la página.

        Retorna:
            dict: {
                'cookie_wall_ratio': float,          # overlay_area / viewport_area
                'overlay_count': int,                # número de overlays full-screen
            }
        """
        dict_cookie_wall_metrics = {}
        try:
            metrics = page.evaluate(r"""
            () => {
              const keywords = [
                "disable adblock",
                "turn off your ad blocker",
                "we value your privacy"
              ];
        
              // 1) Identificar elementos full-screen fixed
              const els = Array.from(document.querySelectorAll("*")).filter(el => {
                const style = getComputedStyle(el);
                const rect  = el.getBoundingClientRect();
                return style.position === "fixed"
                    && (style.top === "0px"  || parseFloat(style.top)  <= 0)
                    && (style.left === "0px" || parseFloat(style.left) <= 0)
                    && (rect.width  >= window.innerWidth  * 0.99)
                    && (rect.height >= window.innerHeight * 0.99);
              });
        
              // 2) Calcular área total de overlays
              let overlayArea = 0;
              els.forEach(el => {
                const { width, height } = el.getBoundingClientRect();
                overlayArea += width * height;
              });
              const viewportArea = window.innerWidth * window.innerHeight;
              const ratio = viewportArea > 0 ? overlayArea / viewportArea : 0;
        
              // 3) Contar matches de texto clave dentro de los overlays
              const overlayTextMatchCount = els.reduce((cnt, el) => {
                const text = (el.innerText || "").toLowerCase();
                return cnt + keywords.reduce((c, kw) => c + (text.includes(kw) ? 1 : 0), 0);
              }, 0);
        
              // 4) Detectar anti-adblock en JS
              const scripts = Array.from(document.scripts).map(s => s.innerText).join("\n");
              const antiAdblock = /window\.adblockDetected/i.test(scripts)
                               || keywords.some(kw => scripts.toLowerCase().includes(kw));
        
              return {
                ratio,
                overlayCount: els.length,
                overlayTextMatchCount,
                antiAdblock
              };
            }
            """)

            dict_cookie_wall_metrics = {
                'cookie_wall_ratio': metrics['ratio'],
                'overlay_count': metrics['overlayCount'],
            }
        except Exception as e:
            self.__logger.error(f'get_asn_ip_metrics: {e}')
        return dict_cookie_wall_metrics

    #señales de accesibilidad y calidad
    def get_lighthouse_metrics(self, site) -> Dict:
        """
        Ejecuta Lighthouse CI en modo headless y extrae las métricas Core Web Vitals.

        Parámetros:
            url (str): URL de la página a auditar.
            timeout (int): Tiempo máximo en milisegundos para la auditoría (por defecto 120000 ms).

        Retorna:
            dict: {
                'performance_score': float,  # Puntuación de performance (0–100)
                'largest_contentful_paint': float,  # LCP en ms
                'cumulative_layout_shift': float,   # CLS (sin unidades)
            }
        """
        url = f'https://{site}'
        dict_lighthouse_metrics = {}
        # Comando Lighthouse CI
        try:
            timeout: int = 120_000
            cmd = [
                r"C:\Users\pablo\AppData\Roaming\npm\lighthouse.cmd",
                url,
                "--quiet",
                "--chrome-flags=--headless --no-sandbox",
                "--output=json",
                "--output-path=stdout",
                f"--max-wait-for-load={timeout}"
            ]

            # Ejecutar Lighthouse CLI
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
            report = json.loads(proc.stdout)

            # Extraer métricas
            audits = report.get("audits", {})
            categories = report.get("categories", {}).get("performance", {})
            dict_lighthouse_metrics = {
                "performance_score": categories.get("score", 0) * 100,
                "largest_contentful_paint": audits.get("largest-contentful-paint", {}).get("numericValue", 0),
                "cumulative_layout_shift": audits.get("cumulative-layout-shift", {}).get("numericValue", 0),
            }
        except Exception as e:
            self.__logger.error(f'get_lighthouse_metrics: {e}')
        if not dict_lighthouse_metrics:
            url = f'http://{site}'
            try:
                result = self.psi_metrics(url, api_key)
                self.__logger.info(f"::Main:: result {result}")
                dict_lighthouse_metrics = {
                    "performance_score": result['performance_score'],
                    "largest_contentful_paint": result['largest_contentful_paint'],
                    "cumulative_layout_shift": result['cumulative_layout_shift'],
                }
            except Exception as e:
                self.__logger.error(f"::Main:: Error processing domain {site} - {e}")



        return dict_lighthouse_metrics

    def get_schema_org_metrics_2(self, html: str) -> dict:
        """
        Extrae y analiza los bloques JSON-LD de Schema.org en el HTML, generando métricas
        de presencia y completitud para categorías:
        Movie, Sports, Ecommerce, Anime, Manga, Books, Games, Adults, Music, Content Host.

        Parámetros:
            html (str): HTML completo de la página.

        Retorna:
            dict con las siguientes claves:
              - has_schema: bool
              - total_schema_blocks: int
              - schema_types_count: int
            Para cada categoría (p.ej. movie, sports, ecommerce, ...):
              - {cat}_count: int            # número de bloques de ese tipo
              - {cat}_complete: bool        # True si al menos un bloque cumple ≥80% props
        """
        soup = BeautifulSoup(html, "lxml")
        scripts = soup.find_all("script", type="application/ld+json")

        types_set = set()
        org_count = lic_count = cr_count = 0

        # Configuración de categorías y sus tipos/schema.org
        category_types = {
            "movie": {"Movie"},
            "sports": {"SportsEvent", "SportsOrganization"},
            "ecommerce": {"Product", "Offer", "AggregateOffer"},
            "anime": {"AnimeSeries"},
            "manga": {"MangaSeries"},
            "books": {"Book"},
            "games": {"VideoGame"},
            "adults": {"AdultEntertainment"},
            "music": {"MusicAlbum", "MusicRecording", "MusicGroup", "MusicVideoObject"},
            "contenthost": {"VideoObject", "AudioObject"}
        }

        # Propiedades requeridas por categoría para considerar "complete"
        required_props = {
            "movie": {
                "@type", "name", "description", "image",
                "datePublished", "director", "actor",
                "genre", "contentRating", "provider"
            },
            "sports": {
                "@type", "name", "startDate", "location",
                "competitor", "audience", "duration"
            },
            "ecommerce": {
                "@type", "name", "description", "image",
                "sku", "offers", "brand", "aggregateRating"
            },
            "anime": {
                "@type", "name", "description", "image",
                "episodeCount", "genre", "productionCompany"
            },
            "manga": {
                "@type", "name", "description", "image",
                "chapterCount", "genre", "author"
            },
            "books": {
                "@type", "name", "author", "datePublished",
                "isbn", "publisher", "description"
            },
            "games": {
                "@type", "name", "description", "image",
                "gamePlatform", "datePublished", "publisher"
            },
            "adults": {
                "@type", "name", "description", "provider",
                "contentRating"
            },
            "music": {
                "@type", "name", "artist", "datePublished",
                "genre", "duration", "inAlbum"
            },
            "contenthost": {
                "@type", "name", "description", "contentUrl",
                "uploadDate", "provider"
            }
        }

        # Inicializar contadores y flags de completitud
        counts = {cat: 0 for cat in category_types}
        completeness = {cat: False for cat in category_types}

        for tag in scripts:
            try:
                data = json.loads(tag.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            entries = data if isinstance(data, list) else [data]
            for entry in entries:
                t = entry.get("@type")
                if not t:
                    continue

                entry_types = t if isinstance(t, list) else [t]
                for et in entry_types:
                    types_set.add(et)

                    # Contar tipos básicos
                    if et == "Organization":
                        org_count += 1
                    elif et == "License":
                        lic_count += 1
                    elif et == "ContentRating":
                        cr_count += 1

                    # Procesar cada categoría
                    for cat, etypes in category_types.items():
                        if et in etypes:
                            counts[cat] += 1
                            # Si aún no es complete, verificar completitud
                            if not completeness[cat]:
                                props = required_props.get(cat, set())
                                present = props.intersection(entry.keys())
                                if props and len(present) / len(props) >= 0.8:
                                    completeness[cat] = True

        # Construir resultado final
        result = {
            "has_schema": bool(types_set),
            "total_schema_blocks": len(scripts),
            "schema_types_count": len(types_set)
        }
        # Añadir métricas por categoría
        # for cat in category_types:
        #     result[f"{cat}_count"] = counts[cat]
        #     result[f"{cat}_complete"] = completeness[cat]

        return result

    def insert_feature(self, input_features):
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

            columns = input_features.keys()
            values = [input_features[col] for col in columns]

            sql_string = f"""
                INSERT INTO public.domain_discovery_features ({', '.join(columns)})
                VALUES ({', '.join(['%s'] * len(values))})
            """

            try:
                cursor.execute(sql_string, values)
                conn.commit()
            except Exception as e:
                self.__logger.error(f'::Saver:: Error found trying to insert_feature - {e}')
            finally:
                cursor.close()
                conn.close()

    def update_html_features(self, html_feature_id, updated_fields):
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
            columns = updated_fields.keys()
            values = [updated_fields[col] for col in columns]

            # Construimos la parte del SET dinámicamente: col1 = %s, col2 = %s, ...
            set_clause = ', '.join([f"{col} = %s" for col in columns])

            sql_string = f"""
                UPDATE public.domain_discovery_features ddf
                SET {set_clause}
                WHERE html_feature_id = %s
            """

            try:
                cursor.execute(sql_string, values + [html_feature_id])
                conn.commit()
            except Exception as e:
                self.__logger.error(f'::Saver:: Error updating feature with id {html_feature_id} - {e}')
            finally:
                cursor.close()
                conn.close()

    def get_html_lenght(self, page):
        height = None
        try:
            # Obtener el tamaño total de la página en píxeles
            page_size = page.evaluate('''() => {
                                                   return {
                                                       width: document.documentElement.scrollWidth,
                                                       height: document.documentElement.scrollHeight
                                                   };
                                               }''')
            height = page_size["height"]
        except Exception as e:
            self.__logger.error(f'get_html_lenght : error {e}')
        return height

    def count_html_tags_and_text(self, html_string):
        soup = BeautifulSoup(html_string, 'html.parser')
        tags = [tag.name for tag in soup.find_all()]
        tag_counts = pd.Series(tags).value_counts().to_dict()

        # Extraer texto visible
        visible_text = soup.get_text()
        text_length = len(visible_text)

        # Crear diccionario de salida con conteos deseados
        result = {
            'div': tag_counts.get('div', 0),
            'a': tag_counts.get('a', 0),
            'text_length': text_length,
            'img': tag_counts.get('img', 0),
            'span': tag_counts.get('span', 0),
            'li': tag_counts.get('li', 0),
            'script': tag_counts.get('script', 0),
            'link': tag_counts.get('link', 0),
            'meta': tag_counts.get('meta', 0),
            'p': tag_counts.get('p', 0),
        }

        return result

    def count_ad_slot_containers(self, html: str) -> Dict[str, int]:
        """
        Cuenta contenedores de anuncios (slots) de múltiples proveedores a partir del HTML:
          - Google AdSense
          - Amazon Associates
          - Taboola
          - Revcontent
          - Outbrain
          - Genéricos (clases/IDs comunes de ad-slot)

        Retorna un dict con conteos por proveedor y total.
        """
        soup = BeautifulSoup(html, "lxml")
        counts = {
            "adsense_ins": 0,
            "amazon_assoc": 0,
            "taboola": 0,
            "revcontent": 0,
            "outbrain": 0,
            "generic_div_class": 0,
            "generic_div_id": 0,
            "total_ad_slots": 0
        }

        # Google AdSense
        for tag in soup.find_all("ins", class_="adsbygoogle"):
            counts["adsense_ins"] += 1

        # Amazon Associates
        for tag in soup.find_all("div", id=re.compile(r"^amzn_assoc", re.I)):
            counts["amazon_assoc"] += 1

        # Taboola
        for tag in soup.find_all("div", id=re.compile(r"^taboola-", re.I)):
            counts["taboola"] += 1

        # Revcontent
        for tag in soup.find_all("div", id=re.compile(r"^revcontent-", re.I)):
            counts["revcontent"] += 1

        # Outbrain
        for tag in soup.find_all("div", attrs={"data-widget-id": re.compile(r"^OB", re.I)}):
            counts["outbrain"] += 1

        # Genéricos por clase
        generic_class_re = re.compile(r"\b(ad-slot|ad-unit|ad-container|advertisement|advert)\b", re.I)
        for tag in soup.find_all("div", class_=generic_class_re):
            counts["generic_div_class"] += 1

        # Genéricos por ID
        for tag in soup.find_all("div", id=generic_class_re):
            counts["generic_div_id"] += 1

        # Total
        counts["total_ad_slots"] = (
                counts["adsense_ins"]
                + counts["amazon_assoc"]
                + counts["taboola"]
                + counts["revcontent"]
                + counts["outbrain"]
                + counts["generic_div_class"]
                + counts["generic_div_id"]
        )

        return counts

    def psi_metrics(self, url: str, api_key: str) -> dict:
        endpoint = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
        params = {
            "url": url,
            "strategy": "mobile",
            "category": "performance",
            "key": api_key
        }
        resp = requests.get(endpoint, params=params, timeout=30)
        data = resp.json().get("lighthouseResult", {})
        audits = data.get("audits", {})
        perf = data.get("categories", {}).get("performance", {})
        return {
            "performance_score": perf.get("score", 0) * 100,
            "largest_contentful_paint": audits.get("largest-contentful-paint", {}).get("numericValue", 0),
            "cumulative_layout_shift": audits.get("cumulative-layout-shift", {}).get("numericValue", 0),
            "interaction_to_next_paint": audits.get("interaction-to-next-paint", {}).get("numericValue", 0),
        }

    def count_ad_script_src(self, html: str, ad_domains) -> int:
        """
        Cuenta <script src="..."> cuya URL contiene alguno de los dominios de ad_domains.
        """
        soup = BeautifulSoup(html, "lxml")
        count = 0
        for tag in soup.find_all("script", src=True):
            src = tag["src"]
            if any(domain in src for domain in ad_domains):
                count += 1
        return count
