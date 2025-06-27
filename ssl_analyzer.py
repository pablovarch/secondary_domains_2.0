from dependencies import  log
from settings import db_connect
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
            'postgresql+psycopg2://algorym:bp9x3e.fvi7hf45.DLQ@supply.cmyrnxn5vuvh.us-east-1.rds.amazonaws.com:5432/supply',
            pool_recycle=3600)

        dbConnection = alchemyEngine.connect()
        sec_domain = pd.read_sql(""" select sec_domain_root,exc_domain_id,ml_sec_domain_classification,ml_piracy,
        ad_count,site_map_count,tld_poor,site_traffic from secondary_domains where online_status = 'Online' and redirect_domain = False """,
                                 dbConnection)
        domain_ssl = pd.read_sql(
            """select requested_domain, validation_type, issuer_organization, valid_from, public_key_type, certificate_policies, dns_names from domain_ssl_data""",
            dbConnection)
        dbConnection.close()


        domain_ssl = domain_ssl.dropna(subset='certificate_policies')
        sec_domain_merged = sec_domain.merge(domain_ssl, left_on="sec_domain_root", right_on="requested_domain")
        sec_domain_merged = self.evaluar_certificados_ssl(sec_domain_merged)
        print(sec_domain_merged)



    def evaluate_ssl(self,row):
        score = 0

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
        df['sospechoso'] = df['ssl_score'] > 0.6
        return df

    #  update