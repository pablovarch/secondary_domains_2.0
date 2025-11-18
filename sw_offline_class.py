from dependencies import log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime


class Sw_offline_class:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='Sw_offline_class.log')

    def main(self):

        self.__logger.info('-- starting Sw_offline_class classifier')
        alchemyEngine = create_engine(
            db_connect_df,  # ej: "postgresql+psycopg2://user:pass@host:5432/dbname"
            pool_recycle=3600,
            pool_pre_ping=True  # robustez ante conexiones caídas
        )
        # Correcting the syntax error by removing the invalid 'DB Connection' line
        sql = text("""
         WITH
          query_variables AS (
            SELECT
              -- Most recent SimilarWeb Data Month
              date_trunc('month', ((SELECT MAX(rt."month") FROM referral_traffic rt) - INTERVAL '5' MONTH))::DATE AS start_month
          ),
          -- Target domains to analyze (from secondary_domains with specific conditions)
          target_domains AS (
            SELECT DISTINCT
              sd.sec_domain_id,
              sd.sec_domain AS destination_domain
            FROM secondary_domains sd
            WHERE
              sd.sec_domain_source = 'SimilarWeb' or sd.sec_domain_source = 'Ad Sniffer'
          ),
          -- CTE to filter domains by traffic source: direct + referrals >= 40% and organic < 20%
          traffic_sources AS (
            SELECT DISTINCT
              sd.sec_domain_id,
              (dts.direct_share + dts.referrals_share) AS direct_plus_referrals
            FROM secondary_domains sd
            INNER JOIN dim_traffic_sources dts ON sd.sec_domain_id = dts.sec_domain_id
           
          ),
          -- Filter out destination domains that are in ad_domains (except approved companies)
          valid_destination_domains AS (
            SELECT DISTINCT
              rt.d_sec_domain_id,
              rt.destination_domain
            FROM referral_traffic rt
            CROSS JOIN query_variables qv
            WHERE
              rt.d_exc_domain_id IS NULL
              AND rt.d_domain_id IS NULL
              AND (
                NOT EXISTS (
                  SELECT 1 FROM ad_domains ad
                  WHERE LOWER(ad.ad_company_domain) = LOWER(rt.destination_domain)
                )
                OR EXISTS (
                  SELECT 1 FROM ad_domains ad
                  WHERE LOWER(ad.ad_company_domain) = LOWER(rt.destination_domain)
                  AND ad.ad_company_id IN (4375, 4527)
                )
              )
          ),
          combined_list AS (
            -- Total Traffic for destination domains (ONLY from start date forward)
            SELECT
              rt.d_sec_domain_id,
              rt.destination_domain,
              0::INTEGER AS inf_referring_sites,
              SUM(rt.referred_visits) AS total_referral_traffic,
              0::BIGINT AS inf_referral_traffic,
              0::BIGINT AS cust_inf_referral_traffic,
              0::BIGINT AS adult_referral_traffic,
              MIN(rt."month") AS first_seen,
              MAX(rt."month") AS last_seen
            FROM
              referral_traffic rt
              INNER JOIN valid_destination_domains vdd ON rt.d_sec_domain_id = vdd.d_sec_domain_id
              INNER JOIN target_domains td ON rt.d_sec_domain_id = td.sec_domain_id
              CROSS JOIN query_variables qv
            WHERE
              rt."month" >= qv.start_month
              AND rt.step_in_process = 2
              AND rt.d_exc_domain_id IS NULL
              AND rt.d_domain_id IS NULL
            GROUP BY
              rt.d_sec_domain_id,
              rt.destination_domain

            UNION ALL

            -- List of destination domains receiving traffic from generally infringing domains
            SELECT
              rt.d_sec_domain_id,
              rt.destination_domain,
              COUNT(DISTINCT rt.r_domain_id)::INTEGER AS inf_referring_sites,
              0::BIGINT AS total_referral_traffic,
              SUM(rt.referred_visits) AS inf_referral_traffic,
              0::BIGINT AS cust_inf_referral_traffic,
              0::BIGINT AS adult_referral_traffic,
              MIN(rt."month") AS first_seen,
              MAX(rt."month") AS last_seen
            FROM
              referral_traffic rt
              INNER JOIN valid_destination_domains vdd ON rt.d_sec_domain_id = vdd.d_sec_domain_id
              INNER JOIN target_domains td ON rt.d_sec_domain_id = td.sec_domain_id
              INNER JOIN domain_attributes da ON rt.r_domain_id = da.domain_id
              CROSS JOIN query_variables qv
            WHERE
              rt."month" >= qv.start_month
              AND rt.step_in_process = 2
              AND rt.d_exc_domain_id IS NULL
              AND rt.d_domain_id IS NULL
              AND (
                da.analyst_classification_id IN (1, 16)
                OR (
                  da.analyst_classification_id IS NULL
                  AND da.ml_domain_classification_id IN (1, 3, 4, 5)
                )
              )
            GROUP BY
              rt.d_sec_domain_id,
              rt.destination_domain

            UNION ALL

            -- List of Customer infringing domains
            SELECT
              rt.d_sec_domain_id,
              rt.destination_domain,
              0::INTEGER AS inf_referring_sites,
              0::BIGINT AS total_referral_traffic,
              0::BIGINT AS inf_referral_traffic,
              SUM(rt.referred_visits) AS cust_inf_referral_traffic,
              0::BIGINT AS adult_referral_traffic,
              MIN(rt."month") AS first_seen,
              MAX(rt."month") AS last_seen
            FROM
              referral_traffic rt
              INNER JOIN valid_destination_domains vdd ON rt.d_sec_domain_id = vdd.d_sec_domain_id
              INNER JOIN target_domains td ON rt.d_sec_domain_id = td.sec_domain_id
              INNER JOIN domain_attributes da ON rt.r_domain_id = da.domain_id
              INNER JOIN tenants_domain td2 ON da.domain_id = td2.domain_id
              INNER JOIN tenants t ON td2.tenant_id = t.tenant_id
              CROSS JOIN query_variables qv
            WHERE
              rt."month" >= qv.start_month
              AND rt.step_in_process = 2
              AND rt.d_exc_domain_id IS NULL
              AND rt.d_domain_id IS NULL
              AND (
                da.analyst_classification_id IN (1, 16)
                OR (
                  da.analyst_classification_id IS NULL
                  AND da.ml_domain_classification_id IN (1, 4, 5)
                )
              )
              AND t.customer_active = TRUE
            GROUP BY
              rt.d_sec_domain_id,
              rt.destination_domain

            UNION ALL

            -- List of Adult domains
            SELECT
              rt.d_sec_domain_id,
              rt.destination_domain,
              0::INTEGER AS inf_referring_sites,
              0::BIGINT AS total_referral_traffic,
              0::BIGINT AS inf_referral_traffic,
              0::BIGINT AS cust_inf_referral_traffic,
              SUM(rt.referred_visits) AS adult_referral_traffic,
              MIN(rt."month") AS first_seen,
              MAX(rt."month") AS last_seen
            FROM
              referral_traffic rt
              INNER JOIN valid_destination_domains vdd ON rt.d_sec_domain_id = vdd.d_sec_domain_id
              INNER JOIN target_domains td ON rt.d_sec_domain_id = td.sec_domain_id
              INNER JOIN domain_attributes da ON rt.r_domain_id = da.domain_id
              CROSS JOIN query_variables qv
            WHERE
              rt."month" >= qv.start_month
              AND rt.step_in_process = 2
              AND rt.d_exc_domain_id IS NULL
              AND rt.d_domain_id IS NULL
              --AND rt.category = 'Adult'
            GROUP BY
              rt.d_sec_domain_id,
              rt.destination_domain
          )
        -- Main query with 4 key metrics
        SELECT
          ROUND(AVG(ts.direct_plus_referrals), 2) AS "% Direct+Referrals",
          ROUND(SUM(cl.inf_referral_traffic)::NUMERIC / NULLIF(SUM(cl.total_referral_traffic), 0), 2) AS "% Referrals Infringing",
          ROUND(SUM(cl.cust_inf_referral_traffic)::NUMERIC / NULLIF(SUM(cl.total_referral_traffic), 0), 2) AS "% Referrals CH Customer Infringing",
          ROUND(SUM(cl.adult_referral_traffic)::NUMERIC / NULLIF(SUM(cl.total_referral_traffic), 0), 2) AS "% Referrals Adult",
          SUM(cl.total_referral_traffic) AS "Total Referral Traffic",
          sd.sec_domain_id,
          sd.google_search_results,
          sd.online_status,
          sd.sec_domain_root,
          sd.exc_domain_id,
          sd.sec_domain_source
        FROM
          combined_list cl
          INNER JOIN traffic_sources ts ON cl.d_sec_domain_id = ts.sec_domain_id
          JOIN secondary_domains sd on cl.d_sec_domain_id = sd.sec_domain_id
        where 
          sd.ml_sec_domain_classification not in (1) or sd.ml_sec_domain_classification is null
        GROUP BY
          sd.sec_domain_id,
          sd.google_search_results,
          sd.online_status,
          sd.sec_domain_root,
          sd.exc_domain_id,
           sd.sec_domain_source
        ORDER BY
          SUM(cl.total_referral_traffic) desc
        """)

        # 3) Ejecutar con Pandas (sin pasar "params" extra)
        #    Nota: pasamos el Engine directamente o un Connection abierto en context manager.
        with alchemyEngine.connect() as conn:
            sw_offline = pd.read_sql_query(sql, con=conn)

        def check_domains(row):
            # chech exclude domain
            if pd.notna(row['exc_domain_id']):
                return 4

            # chequear google_search_results
            if pd.notna(row['google_search_results']):  # ver si este chequeo funciona bien
                # offline search
                if row['google_search_results'] < 2:
                    return 2
                # online search
                elif row['% Referrals Infringing'] > 0.5 and row['% Referrals CH Customer Infringing'] > 0.2  and row[
                    '% Direct+Referrals'] > 0.6:
                    # chequear si esta offline o bloqueado
                    if row['online_status'] == "Online":
                        if row['google_search_results'] < 10:
                            return 2
                        elif row['google_search_results'] >= 10 and row['google_search_results'] < 50:
                            return 3
                        else:
                            return 4
                    elif row['online_status'] in ["Offline", "Blocked", "Offline | Status Checker",
                                                  "Offline--Bulk-check", "Offline | Ad Sniffer"]:
                        return 2
                # casos no previstos
                else:
                    if row['online_status'] in ["Offline", "Blocked", "Offline | Status Checker", "Offline--Bulk-check",
                                                "Offline | Ad Sniffer"]:
                        return 0
                    return
            # Sitios sin google_search_results
            elif row['% Referrals Infringing'] > 0.5 and row['% Referrals CH Customer Infringing'] > 0.2 and row[
                '% Direct+Referrals'] > 0.6:
                # chequear si esta offline o bloqueado
                if row['online_status'] == "Online":
                    return 3
                elif row['online_status'] in ["Offline", "Blocked", "Offline | Status Checker", "Offline--Bulk-check",
                                              "Offline | Ad Sniffer"]:
                    return 2
            # casos no previstos
            else:
                if row['online_status'] in ["Offline", "Blocked", "Offline | Status Checker", "Offline--Bulk-check",
                                              "Offline | Ad Sniffer"]:
                    return 0
                return

        sw_offline['ml_sec_domain_classification'] = sw_offline.apply(lambda row: check_domains(row), axis=1)
        sw_offline.dropna(subset='ml_sec_domain_classification',inplace=True)

        sw_offline['sec_domain_source'] = 'SimilarWeb'
        df_filtered = sw_offline[['sec_domain_id', 'ml_sec_domain_classification','sec_domain_source' ]]
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
                (domain['sec_domain_id'], domain['ml_sec_domain_classification'],domain['sec_domain_source'] ) for domain in save_data
            ]

            # Crea un VALUES string gigante para el UPDATE masivo usando CTE
            values_template = ",".join(["(%s, %s, %s)"] * len(data_to_update))
            flat_values = []
            for tup in data_to_update:
                flat_values.extend(tup)  # aplanamos la lista para pasar a execute

            sql = f"""
                WITH updates (sec_domain_id, value_to_update,sec_domain_source_to_update ) AS (
                    VALUES {values_template}
                )
                UPDATE public.secondary_domains AS t
                SET ml_sec_domain_classification = u.value_to_update
                set sec_domain_source = u.sec_domain_source_to_update
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