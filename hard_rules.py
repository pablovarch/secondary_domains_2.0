from dependencies import log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
import re
from sqlalchemy import create_engine, text
from urllib.parse import urlsplit


# Para Clasificar dominios de Betting y excluir sitios con piracy brand

LINK_SHORTENER_CLASSIFICATION_ID = 13

# Registro cerrado de dominios que deben conservar la clasificación Link Shortener.
# Se usan coincidencias exactas de hostname; no se infieren nuevos shorteners por patrón.
LINK_SHORTENER_DOMAINS = frozenset({
    'adf.ly',
    'adfoc.us',
    'bit.ly',
    'bl.ink',
    'buff.ly',
    'clk.sh',
    'cutt.ly',
    'dub.co',
    'exe.io',
    'fc.lc',
    'gplinks.in',
    'is.gd',
    'linkvertise.com',
    'lootlabs.gg',
    'mboost.me',
    'ouo.io',
    'ow.ly',
    'rb.gy',
    'rebrand.ly',
    's.id',
    'sh.st',
    'short.io',
    'shorturl.at',
    'shrinkearn.com',
    'shrinkme.io',
    't.co',
    't.ly',
    'tinyurl.com',
    'za.gl',
})

# El pre-filtro SQL también incluye la variante www para poder corregir registros
# previamente clasificados que se hayan guardado con ese prefijo.
LINK_SHORTENER_DOMAIN_VARIANTS = tuple(sorted(
    LINK_SHORTENER_DOMAINS | {f'www.{domain}' for domain in LINK_SHORTENER_DOMAINS}
))


def normalize_domain(value):
    """Return a canonical hostname for exact registry matching."""
    if not isinstance(value, str):
        return ''

    candidate = value.strip().lower()
    if not candidate:
        return ''

    parsed = urlsplit(candidate if '://' in candidate else f'//{candidate}')
    hostname = parsed.hostname or ''
    hostname = hostname.rstrip('.')
    return hostname[4:] if hostname.startswith('www.') else hostname

class Betting_piracy:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='hard_rules.log')

    def main(self):

        self.__logger.info('-- starting Hard Rules classifier')
        alchemyEngine = create_engine(
            db_connect_df,  # ej: "postgresql+psycopg2://user:pass@host:5432/dbname"
            pool_recycle=3600,
            pool_pre_ping=True  # robustez ante conexiones caídas
        )
        shortener_params = {
            f'link_shortener_{index}': domain
            for index, domain in enumerate(LINK_SHORTENER_DOMAIN_VARIANTS)
        }
        shortener_placeholders = ', '.join(f':{name}' for name in shortener_params)
        target_domains_query = text(f"""
            SELECT sec_domain_id, sec_domain, exc_domain_id, ml_sec_domain_classification
            FROM secondary_domains
            WHERE ml_sec_domain_classification IS NULL
               OR LOWER(BTRIM(sec_domain)) IN ({shortener_placeholders})
        """)

        with alchemyEngine.connect() as conn:
            sec_domain = pd.read_sql(target_domains_query, conn, params=shortener_params)
            piracy_brands = pd.read_sql("select keyword from ml_piracy_keywords where brand=true", conn)

        betting_list = ['bet365', 'betway', '1xbet', 'bwin', '888', 'williamhill', 'stake', 'betfair', 'leovegas',
                        'betsson', '10bet', 'unibet', 'dafabet', 'parimatch', 'sportingbet',
                        'bet-at-home', 'mrgreen', 'casumo', 'ggbet', 'rivalry', 'betano', 'paddypower', 'ladbrokes',
                        'skybet', 'betvictor', 'betfred', 'betclic', 'marathonbet', 'sbobet', 'boylesports', 'fonbet',
                        'pinnacle',
                        'betsafe', 'tonybet', 'interwetten', 'tipico', 'fanduel', 'draftkings', 'betmgm', 'caesars',
                        'betrivers', 'pointsbet',
                        'fanatics', 'espnbet', 'circa-sports', 'hardrockbet', 'codere', 'caliente', 'betcris', 'bplay',
                        'betwarrior', 'playdoit',
                        'pixbet', 'apostaganha', 'esportesdasorte', 'betsul', 'betnacional', 'galerabet', 'kto',
                        'coolbet']

        # Optimized: Use regex with word boundaries to avoid false positives
        # Escape special regex characters and add word boundaries
        escaped_terms = [re.escape(term) for term in betting_list]
        # Use word boundaries (\b) for longer terms, and start/separator boundaries for short ones
        patterns = []
        for term in escaped_terms:
            # For very short terms (<=3 chars), require them to be at domain start or after a separator
            if len(term.replace('\\', '')) <= 3:
                patterns.append(f'(^{term}[.-]|[.-]{term}[.-]|[.-]{term}$|^{term}$)')
            else:
                # For longer terms, use word boundaries
                patterns.append(f'\\b{term}\\b')

        betting_pattern = '|'.join(patterns)
        # Vectorized operation: much faster than apply()
        is_betting = sec_domain.sec_domain.str.contains(betting_pattern, case=False, na=False, regex=True)
        sec_domain.loc[is_betting, 'ml_sec_domain_classification'] = 6

        # Optimized: Use vectorized operations instead of apply(axis=1)
        # Create a regex pattern with all piracy brands for efficient matching
        brands_list = piracy_brands.keyword.values
        if len(brands_list) > 0:
            # Escape special regex characters and join with OR operator
            brands_pattern = '|'.join([re.escape(str(brand)) for brand in brands_list])
            # Find domains containing piracy brands
            has_piracy_brand = sec_domain.sec_domain.str.contains(brands_pattern, case=False, na=False, regex=True)
            # Set classification to 0 where piracy brand is found, otherwise keep existing classification
            sec_domain.loc[has_piracy_brand, 'ml_sec_domain_classification'] = 1

        # Set classification to 4 where exc_domain_id is not null
        sec_domain.loc[sec_domain.exc_domain_id.notnull(), 'ml_sec_domain_classification'] = 4

        # Link Shortener is a closed, absolute-priority registry rule. It runs after
        # the other hard rules so it also replaces an existing classification.
        sec_domain['decision_source'] = None
        # Hard rules are not an approved source of confidence metadata, so
        # their classifications must remove metadata left by prior classifiers.
        sec_domain['clear_classification_metadata'] = True
        is_link_shortener = sec_domain.sec_domain.map(normalize_domain).isin(LINK_SHORTENER_DOMAINS)
        sec_domain.loc[is_link_shortener, 'ml_sec_domain_classification'] = LINK_SHORTENER_CLASSIFICATION_ID
        sec_domain.loc[is_link_shortener, 'decision_source'] = 'Link Shortener Registry'
        self.__logger.info('-- Link Shortener registry matches: %s', int(is_link_shortener.sum()))

        sec_domain = sec_domain.dropna(subset=['ml_sec_domain_classification']).copy()

        df_filtered = sec_domain[['sec_domain_id', 'ml_sec_domain_classification']]
        df_filtered = df_filtered.copy()
        df_filtered['decision_source'] = sec_domain['decision_source'].fillna('Hard Rules')
        df_filtered['clear_classification_metadata'] = sec_domain[
            'clear_classification_metadata'
        ].fillna(False)
        data_to_save = df_filtered.to_dict('records')
        self.update_domains(data_to_save)

    def update_domains(self, save_data):
        """
        Efficiently updates domain data using a CTE VALUES block (no temp table needed).
        """
        if not save_data:
            self.__logger.info('-- No domains matched Hard Rules')
            return

        try:
            conn = psycopg2.connect(host=db_connect['host'],
                                    database=db_connect['database'],
                                    password=db_connect['password'],
                                    user=db_connect['user'],
                                    port=db_connect['port'])
            print('DB connection opened')
        except Exception as e:
            print(f'::DBConnect:: cannot connect to DB Exception: {e}')
            raise

        try:
            cursor = conn.cursor()

            # Preparamos los valores (tuplas de domain_id y valor nuevo)
            data_to_update = [
                (
                    domain['sec_domain_id'],
                    domain['ml_sec_domain_classification'],
                    domain['decision_source'],
                    bool(domain.get('clear_classification_metadata', False)),
                ) for domain
                in save_data
            ]

            # Crea un VALUES string gigante para el UPDATE masivo usando CTE
            values_template = ",".join(["(%s, %s, %s, %s)"] * len(data_to_update))
            flat_values = []
            for tup in data_to_update:
                flat_values.extend(tup)  # aplanamos la lista para pasar a execute

            sql = f"""
                WITH updates (
                    sec_domain_id,
                    ml_sec_domain_classification,
                    decision_source,
                    clear_classification_metadata
                ) AS (
                    VALUES {values_template}
                )
                UPDATE public.secondary_domains AS t
                SET ml_sec_domain_classification = u.ml_sec_domain_classification,
                    decision_source = u.decision_source,
                    confidence = CASE WHEN u.clear_classification_metadata THEN NULL ELSE t.confidence END,
                    recommended_action_id = CASE WHEN u.clear_classification_metadata THEN NULL ELSE t.recommended_action_id END,
                    justification = CASE WHEN u.clear_classification_metadata THEN NULL ELSE t.justification END,
                    exploit_type = CASE WHEN u.clear_classification_metadata THEN NULL ELSE t.exploit_type END
                FROM updates u
                WHERE t.sec_domain_id = u.sec_domain_id;
            """

            cursor.execute(sql, flat_values)
            conn.commit()
            print(f'{len(data_to_update)} domains updated using CTE VALUES method.')

        except Exception as e:
            print(f'Error during CTE batch update: {e}')
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
            print('DB connection closed')
