from dependencies import  log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

class ssl_analyzer :
    def __init__(self):
        self.__logger = log.Log().get_logger(name='analyzer.log')

    def main(self):
        # Correcting the syntax error by removing the invalid 'DB Connection' line
        alchemyEngine = create_engine(
            db_connect_df,
            pool_recycle=3600)

        dbConnection = alchemyEngine.connect()
        # sec_domain = pd.read_sql(""" select sec_domain_id, sec_domain_root,exc_domain_id,ml_sec_domain_classification,ml_piracy,
        # ad_count,site_map_count,tld_poor,site_traffic from secondary_domains where online_status = 'Online' and redirect_domain = False """,
        #                          dbConnection)

        sec_domain = pd.read_sql("""  
        select 
            sec_domain_id, 
            sec_domain_root,
            exc_domain_id,
            ml_sec_domain_classification,
            ml_piracy,
            ad_count,
            site_map_count,
            tld_poor,
            site_traffic 
        from secondary_domains sd
        where 
            sd.ssl_poor is null 
            and sd.online_status = 'Online'
            and sd.redirect_domain = False
            """,
                                 dbConnection)
        domain_ssl = pd.read_sql(
            """select requested_domain, validation_type, issuer_organization, valid_from, public_key_type, certificate_policies, dns_names from domain_ssl_data""",
            dbConnection)
        dbConnection.close()


        # domain_ssl = domain_ssl.dropna(subset='certificate_policies')
        sec_domain_merged = sec_domain.merge(domain_ssl, left_on="sec_domain_root", right_on="requested_domain")
        sec_domain_merged = self.evaluar_certificados_ssl(sec_domain_merged)
        df_filtered = sec_domain_merged[['sec_domain_id', 'ssl_poor']]
        data_to_save = df_filtered.to_dict('records')
        self.update_domains(data_to_save)



    def evaluate_ssl(self,row):
        score = 0

        if not row.get('validation_type') and not row.get('issuer_organization'):
            score += 0.7

        else:

            # 1. Tipo de validación (puede ser NaN)
            vt = row.get('validation_type')
            if isinstance(vt, str) and vt.lower() == 'domain':
                score += 0.3

            # 2. Emisor del certificado (puede ser NaN)
            issuer = row.get('issuer_organization')
            issuer_str = str(issuer).lower() if pd.notna(issuer) else ''
            if any(x in issuer_str for x in ["let's encrypt", "google"]):
                score += 0.2

            # 3. Duración del certificado (saltamos si falta alguna fecha)
            vf = row.get('valid_from')
            vt2 = row.get('valid_to')
            if pd.notna(vf) and pd.notna(vt2):
                try:
                    valid_from = datetime.fromisoformat(vf.replace('Z', ''))
                    valid_to = datetime.fromisoformat(vt2.replace('Z', ''))
                    if (valid_to - valid_from).days <= 90:
                        score += 0.2
                except Exception:
                    pass

            # 4. Tamaño y tipo de clave pública (NaN → bits=0 / tipo a cadena)
            pk_type = row.get('public_key_type')
            pk_type_str = str(pk_type) if pd.notna(pk_type) else ''
            try:
                pk_bits = int(row.get('public_key_bits', 0))
            except Exception:
                pk_bits = 0
            if (pk_type_str == 'RSA' and pk_bits < 2048) or \
                    (pk_type_str == 'ECDSA' and pk_bits < 256):
                score += 0.2

            # 5. Política del certificado (NaN → cadena vacía)
            policies = str(row.get('certificate_policies', '')) if pd.notna(row.get('certificate_policies')) else ''
            if '2.23.140.1.2.1' in policies:  # DV
                score += 0.2

            # 6. Comodines en DNS (NaN → cadena vacía)
            dns = str(row.get('dns_names', '')) if pd.notna(row.get('dns_names')) else ''
            if '*' in dns:
                score += 0.1

        return round(score, 2)

    def evaluar_certificados_ssl(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Toma un DataFrame con las columnas SSL necesarias y añade:
          - ssl_score: float
          - sospechoso: bool (ssl_score > 0.6)
        """
        df['ssl_score'] = df.apply(self.evaluate_ssl, axis=1)
        df['ssl_poor'] = df['ssl_score'] > 0.6
        return df

    #  update
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
                (domain['sec_domain_id'], domain['ssl_poor']) for domain in save_data
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
                SET ssl_poor = u.value_to_update
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