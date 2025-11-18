from dependencies import  log
from settings import db_connect, db_connect_df, ssl_apikey
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, time
import requests

class ssl_analyzer :
    def __init__(self):
        self.__logger = log.Log().get_logger(name='analyzer.log')

    def main(self):
        self.__logger.info('Starting ssl analyzer script')
        # complete ssl features
        # Obtener dominios a procesar
        print(f"{datetime.now()} - Obteniendo dominios de la base de datos...")
        batch_size = 100
        alchemyEngine = create_engine(
            db_connect_df,
            pool_recycle=3600)
        db = create_engine(db_connect_df)
        conn = db.connect()

        dbConnection = alchemyEngine.connect()
        # sec_domains_to_extract_ssl = pd.read_sql("""
        #   select
        #     sec_domain_id,
        #     sec_domain_root
        # from secondary_domains sd
        # left join domain_ssl_data dsd on sd.sec_domain_root = dsd.requested_domain
        # where
        #     sd.ssl_poor is null
        #     and sd.online_status = 'Online'
        #     and sd.redirect_domain = False
        #     and sd.ml_sec_domain_classification is null
        #     and sd.sec_domain_root is not null
        #     """,
        #                          dbConnection)

        sec_domains_to_extract_ssl = pd.read_sql("""  
                  select 
            sec_domain_id,
            sd.sec_domain 
        from secondary_domains sd
        left join domain_ssl_data dsd on sd.sec_domain_root = dsd.requested_domain 
                    """,
                                                 dbConnection)

        dbConnection.close()
        supply_list = sec_domains_to_extract_ssl.to_dict('records')
        print(f"✓ {len(supply_list)} dominios encontrados para procesar\n")

        all_records = []
        processed = 0
        total = len(supply_list)

        print(f"Iniciando procesamiento de {total} dominios...")
        print(f"Guardando en lotes de {batch_size} registros")

        for row in supply_list:
            domain = row['sec_domain']
            processed += 1

            print(f"{datetime.now()} - [{processed}/{total}] Procesando {domain}...")

            try:
                # Llamar a la API
                response = requests.get(
                    f"https://ssl-certificates.whoisxmlapi.com/api/v1?apiKey={ssl_apikey}&domainName={domain}",
                    timeout=30
                ).json()

                # Procesar respuesta
                records = self.process_ssl_response(response, domain)
                all_records.extend(records)

                print(f"✓ {domain} procesado correctamente ({len(records)} certificados)")

                # Guardar en lotes
                if len(all_records) >= batch_size:
                    df_batch = pd.DataFrame(all_records)
                    df_batch.to_sql('domain_ssl_data', con=conn, if_exists='append', index=False)
                    print(f"→ Lote de {len(all_records)} registros guardado en la base de datos")
                    all_records = []  # Limpiar lista

                # Pausa para no saturar la API
                time.sleep(1)

            except requests.exceptions.RequestException as e:
                print(f"✗ Error de conexión con {domain}: {e}")
                # Agregar registro vacío para marcar que falló
                all_records.extend(self.process_ssl_response({}, domain))

            except Exception as e:
                print(f"✗ Error procesando {domain}: {e}")
                all_records.extend(self.process_ssl_response({}, domain))

        # Guardar registros restantes
        if all_records:
            df_batch = pd.DataFrame(all_records)
            df_batch.to_sql('domain_ssl_data', con=conn, if_exists='append', index=False)
            print(f"→ Último lote de {len(all_records)} registros guardado en la base de datos")

        conn.close()
        print(f"\n✓ Proceso completado. Total procesado: {processed}/{total} dominios")




        # Correcting the syntax error by removing the invalid 'DB Connection' line
        alchemyEngine = create_engine(
            db_connect_df,
            pool_recycle=3600)

        dbConnection = alchemyEngine.connect()


        sec_domain = pd.read_sql("""  
        select 
            sec_domain_id, 
            sec_domain,
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
            and sd.ml_sec_domain_classification is null
            """,
                                 dbConnection)
        domain_ssl = pd.read_sql(
            """select requested_domain, validation_type, issuer_organization, valid_from, public_key_type, certificate_policies, dns_names from domain_ssl_data""",
            dbConnection)
        dbConnection.close()


        # domain_ssl = domain_ssl.dropna(subset='certificate_policies')
        sec_domain_merged = sec_domain.merge(domain_ssl, left_on="sec_domain", right_on="requested_domain")
        sec_domain_merged = self.evaluar_certificados_ssl(sec_domain_merged)
        df_filtered = sec_domain_merged[['sec_domain_id', 'ssl_poor']]
        data_to_save = df_filtered.to_dict('records')
        self.update_domains(data_to_save)

    def process_ssl_response(self, response_data, requested_domain):
        """
        Procesa la respuesta JSON de la API SSL directamente
        """
        formatted_data = []

        if response_data.get('domain'):
            domain = response_data.get('domain')
            ip = response_data.get('ip')
            port = response_data.get('port')
            audit_created = response_data.get('auditCreated')

            for cert in response_data.get('certificates', []):
                subject = cert.get("subject")
                if isinstance(subject, dict):
                    subject_common_name = subject.get("commonName")
                else:
                    subject_common_name = None

                issuer = cert.get("issuer")
                if isinstance(issuer, dict):
                    issuer_country = issuer.get("country")
                    issuer_organization = issuer.get("organization")
                    issuer_common_name = issuer.get("commonName")
                else:
                    issuer_country = None
                    issuer_organization = None
                    issuer_common_name = None

                extensions = cert.get("extensions")
                if isinstance(extensions, dict):
                    authority_key_identifier = cert.get("extensions", {}).get("authorityKeyIdentifier")
                    subject_key_identifier = cert.get("extensions", {}).get("subjectKeyIdentifier")
                    key_usage = str(cert.get("extensions", {}).get("keyUsage", ""))
                    extended_key_usage = str(cert.get("extensions", {}).get("extendedKeyUsage", ""))
                    crl_distribution_points = str(cert.get("extensions", {}).get("crlDistributionPoints", ""))
                    aia_issuers = str(cert.get("extensions", {}).get("authorityInfoAccess", {}).get("issuers", ""))
                    aia_ocsp = str(cert.get("extensions", {}).get("authorityInfoAccess", {}).get("ocsp", ""))
                    dns_names = str(cert.get("extensions", {}).get("subjectAlternativeNames", {}).get("dnsNames", ""))
                    certificate_policies = str(cert.get("extensions", {}).get("certificatePolicies", ""))
                else:
                    authority_key_identifier = None
                    subject_key_identifier = None
                    key_usage = None
                    extended_key_usage = None
                    crl_distribution_points = None
                    aia_issuers = None
                    aia_ocsp = None
                    dns_names = None
                    certificate_policies = None

                row = {
                    "requested_domain": domain,
                    "ip": ip,
                    "port": port,
                    "audit_created": audit_created,
                    "chain_hierarchy": cert.get("chainHierarchy"),
                    "validation_type": cert.get("validationType"),
                    "valid_from": cert.get("validFrom"),
                    "valid_to": cert.get("validTo"),
                    "serial_number": cert.get("serialNumber"),
                    "signature_algorithm": cert.get("signatureAlgorithm"),
                    "subject_common_name": subject_common_name,
                    "issuer_country": issuer_country,
                    "issuer_organization": issuer_organization,
                    "issuer_common_name": issuer_common_name,
                    "authority_key_identifier": authority_key_identifier,
                    "subject_key_identifier": subject_key_identifier,
                    "key_usage": key_usage,
                    "extended_key_usage": extended_key_usage,
                    "crl_distribution_points": crl_distribution_points,
                    "aia_issuers": aia_issuers,
                    "aia_ocsp": aia_ocsp,
                    "dns_names": dns_names,
                    "certificate_policies": certificate_policies,
                    "public_key_type": cert.get("publicKey", {}).get("type"),
                    "public_key_bits": cert.get("publicKey", {}).get("bits")
                }
                formatted_data.append(row)
        else:
            # Si no hay datos, crear un registro vacío
            formatted_data.append({
                "requested_domain": requested_domain,
                "ip": None,
                "port": None,
                "audit_created": None,
                "chain_hierarchy": None,
                "validation_type": None,
                "valid_from": None,
                "valid_to": None,
                "serial_number": None,
                "signature_algorithm": None,
                "subject_common_name": None,
                "issuer_country": None,
                "issuer_organization": None,
                "issuer_common_name": None,
                "authority_key_identifier": None,
                "subject_key_identifier": None,
                "key_usage": None,
                "extended_key_usage": None,
                "crl_distribution_points": None,
                "aia_issuers": None,
                "aia_ocsp": None,
                "dns_names": None,
                "certificate_policies": None,
                "public_key_type": None,
                "public_key_bits": None
            })

        return formatted_data


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