from dependencies import log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
import re
from sqlalchemy import create_engine, text

# Para Clasificar dominios de Betting y excluir sitios con piracy brand

class Betting_piracy:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='rude_rules.log')

    def main(self):


        alchemyEngine = create_engine(
            db_connect_df,  # ej: "postgresql+psycopg2://user:pass@host:5432/dbname"
            pool_recycle=3600,
            pool_pre_ping=True  # robustez ante conexiones caídas
        )
        # Correcting the syntax error by removing the invalid 'DB Connection' line
        with alchemyEngine.connect() as conn:  
            sec_domain = pd.read_sql(""" select sec_domain_id,sec_domain, exc_domain_id from secondary_domains where ml_sec_domain_classification is null""", conn)
            piracy_brands = pd.read_sql("select keyword from ml_piracy_keywords where brand=true", conn)


        betting_list = ['bet365','betway','1xbet','bwin','888','williamhill','stake','betfair', 'leovegas','betsson','10bet','unibet','dafabet','parimatch','sportingbet',
    'bet-at-home','mrgreen','casumo','ggbet','rivalry','betano',  'paddypower','ladbrokes','skybet','betvictor','betfred','betclic',   'marathonbet','sbobet','boylesports','fonbet','pinnacle',
  'betsafe','tonybet','interwetten','tipico', 'fanduel','draftkings','betmgm','caesars','betrivers','pointsbet',
  'fanatics','espnbet','circa-sports','hardrockbet', 'codere','caliente','betcris','bplay','betwarrior','playdoit',
  'pixbet','apostaganha','esportesdasorte','betsul','betnacional', 'galerabet','kto','coolbet' ]
        
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
    
        sec_domain = sec_domain.dropna(subset=['ml_sec_domain_classification'])

        df_filtered = sec_domain[['sec_domain_id', 'ml_sec_domain_classification']]
        data_to_save = df_filtered.to_dict('records')
        self.update_domains(data_to_save)



    def update_domains(self, save_data):
        """
        Efficiently updates domain data using a CTE VALUES block (no temp table needed).
        """
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
                (domain['sec_domain_id'], domain['ml_sec_domain_classification']) for domain in save_data
            ]

            # Crea un VALUES string gigante para el UPDATE masivo usando CTE
            values_template = ",".join(["(%s, %s)"] * len(data_to_update))
            flat_values = []
            for tup in data_to_update:
                flat_values.extend(tup)  # aplanamos la lista para pasar a execute

            sql = f"""
                WITH updates (sec_domain_id, value_to_update) AS (
                    VALUES {values_template}
                )
                UPDATE public.secondary_domains AS t
                SET ml_sec_domain_classification = u.value_to_update
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
