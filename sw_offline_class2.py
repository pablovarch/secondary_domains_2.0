from dependencies import log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime


class Sw_offline_class:
    # Umbrales configurables para detección de FP Risk en Referral Cloaking
    FP_RISK_THRESHOLDS = {
        'traffic_high': 10_000_000,
        'organic_high': 0.40,
        'organic_medium': 0.30,
        'direct_balanced': 0.60,
        'referrals_low': 0.25,
        'display_ads_low': 0.10,
    }

    # Estados considerados como offline/bloqueado
    OFFLINE_STATUSES = [
        "Offline", "Blocked", "Offline | Status Checker",
        "Offline--Bulk-check", "Offline | Ad Sniffer"
    ]

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
          sd.sec_domain_source,
          sd.ml_sec_domain_classification AS current_ml_sec_domain_classification
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
           sd.sec_domain_source,
          sd.ml_sec_domain_classification
        ORDER BY
          SUM(cl.total_referral_traffic) desc
        """)

        # 3) Ejecutar con Pandas (sin pasar "params" extra)
        #    Nota: pasamos el Engine directamente o un Connection abierto en context manager.
        with alchemyEngine.connect() as conn:
            sw_offline = pd.read_sql_query(sql, con=conn)

        sql_no_referral_candidates = text("""
            WITH referral_aggs AS (
                SELECT
                    rt.d_sec_domain_id,
                    SUM(rt.referred_visits) AS total_referral_traffic
                FROM referral_traffic rt
                WHERE
                    rt.step_in_process = 2
                    AND rt.d_exc_domain_id IS NULL
                    AND rt.d_domain_id IS NULL
                GROUP BY
                    rt.d_sec_domain_id
            ),
            traffic_source_aggs AS (
                SELECT
                    dts.sec_domain_id,
                    MAX(dts.direct_share + dts.referrals_share) AS direct_plus_referrals
                FROM dim_traffic_sources dts
                WHERE
                    dts.sec_domain_id IS NOT NULL
                GROUP BY
                    dts.sec_domain_id
            )
            SELECT
                tsa.direct_plus_referrals AS "% Direct+Referrals",
                NULL::NUMERIC AS "% Referrals Infringing",
                NULL::NUMERIC AS "% Referrals CH Customer Infringing",
                NULL::NUMERIC AS "% Referrals Adult",
                ra.total_referral_traffic AS "Total Referral Traffic",
                sd.sec_domain_id,
                sd.google_search_results,
                sd.online_status,
                sd.sec_domain_root,
                sd.exc_domain_id,
                sd.sec_domain_source,
                sd.ml_sec_domain_classification AS current_ml_sec_domain_classification
            FROM secondary_domains sd
            LEFT JOIN referral_aggs ra ON sd.sec_domain_id = ra.d_sec_domain_id
            LEFT JOIN traffic_source_aggs tsa ON sd.sec_domain_id = tsa.sec_domain_id
            WHERE
                (
                    sd.sec_domain_source IN ('SimilarWeb', 'Ad Sniffer')
                    OR sd.decision_source = 'SimilarWeb'
                )
                AND sd.ml_sec_domain_classification IN (2, 3)
                AND (
                    sd.first_reported >= DATE '2026-04-01'
                    OR sd.first_reported IS NULL
                )
                AND (
                    COALESCE(ra.total_referral_traffic, 0) = 0
                    OR COALESCE(tsa.direct_plus_referrals, 0) = 0
                )
        """)

        with alchemyEngine.connect() as conn:
            no_referral_candidates = pd.read_sql_query(sql_no_referral_candidates, con=conn)

        if not no_referral_candidates.empty:
            sw_offline = pd.concat([sw_offline, no_referral_candidates], ignore_index=True)
            sw_offline = sw_offline.drop_duplicates(subset='sec_domain_id', keep='first')

        # 4) Query secundaria para métricas de FP Risk
        sql_fp_metrics = text("""
            SELECT
                sd.sec_domain_id,
                ROUND(MAX(dts.organic_search_share), 2) AS organic_search_share,
                ROUND(AVG(dts.direct_share), 2) AS direct_share,
                ROUND(AVG(dts.referrals_share), 2) AS referrals_share,
                ROUND(AVG(dts.display_ads_share), 2) AS display_ads_share,
                COALESCE(SUM(dt.traffic), 0) AS total_traffic
            FROM
                secondary_domains sd
                LEFT JOIN dim_traffic_sources dts ON sd.sec_domain_id = dts.sec_domain_id
                LEFT JOIN dim_traffic dt ON sd.sec_domain_id = dt.sec_domain_id AND dt.domain_country = 'World'
            WHERE
                sd.sec_domain_id IN :sec_domain_ids
            GROUP BY
                sd.sec_domain_id
        """)

        # Ejecutar query secundaria solo si hay dominios
        if not sw_offline.empty:
            sec_domain_ids = tuple(sw_offline['sec_domain_id'].tolist())
            with alchemyEngine.connect() as conn:
                fp_metrics = pd.read_sql_query(
                    sql_fp_metrics,
                    con=conn,
                    params={'sec_domain_ids': sec_domain_ids}
                )
            # Merge de métricas FP Risk con el DataFrame principal
            sw_offline = sw_offline.merge(fp_metrics, on='sec_domain_id', how='left')
        else:
            self.__logger.info('No domains to process')
            return

        # Contadores para logging de FP Risk
        fp_risk_counts = {
            'traffic_high': 0,
            'organic_high': 0,
            'balanced_profile': 0,
            'balanced_organic': 0,
            'total_excluded': 0
        }
        fp_risk_domains = []  # Lista de sec_domain_id excluidos

        # Función para detectar FP Risk en clasificación Referral Cloaking
        def is_fp_risk(row):
            """Detecta si un dominio tiene perfil de falso positivo para Referral Cloaking."""
            thresholds = self.FP_RISK_THRESHOLDS

            traffic = row.get('total_traffic', 0) or 0
            organic = row.get('organic_search_share', 0) or 0
            direct = row.get('direct_share', 0) or 0
            referrals = row.get('referrals_share', 0) or 0
            display_ads = row.get('display_ads_share', 0) or 0
            sec_domain_id = row.get('sec_domain_id', None)

            # Condiciones de FP Risk (cualquiera dispara)
            if traffic > thresholds['traffic_high']:
                fp_risk_counts['traffic_high'] += 1
                fp_risk_counts['total_excluded'] += 1
                fp_risk_domains.append({'sec_domain_id': sec_domain_id, 'reason': 'traffic_high', 'value': traffic})
                return True
            if organic >= thresholds['organic_high']:
                fp_risk_counts['organic_high'] += 1
                fp_risk_counts['total_excluded'] += 1
                fp_risk_domains.append({'sec_domain_id': sec_domain_id, 'reason': 'organic_high', 'value': organic})
                return True
            if (direct <= thresholds['direct_balanced'] and
                    referrals <= thresholds['referrals_low'] and
                    display_ads <= thresholds['display_ads_low']):
                fp_risk_counts['balanced_profile'] += 1
                fp_risk_counts['total_excluded'] += 1
                fp_risk_domains.append({'sec_domain_id': sec_domain_id, 'reason': 'balanced_profile',
                                        'value': f'd:{direct}/r:{referrals}/da:{display_ads}'})
                return True
            if (direct <= thresholds['direct_balanced'] and
                    referrals <= thresholds['referrals_low'] and
                    organic >= thresholds['organic_medium']):
                fp_risk_counts['balanced_organic'] += 1
                fp_risk_counts['total_excluded'] += 1
                fp_risk_domains.append({'sec_domain_id': sec_domain_id, 'reason': 'balanced_organic',
                                        'value': f'd:{direct}/r:{referrals}/o:{organic}'})
                return True

            return False

        def check_domains(row):
            current_classification = row.get('current_ml_sec_domain_classification', None)
            total_referral_traffic = row.get('Total Referral Traffic', 0) or 0
            direct_plus_referrals = row.get('% Direct+Referrals', 0) or 0
            has_real_referral_evidence = total_referral_traffic > 0 and direct_plus_referrals > 0
            special_no_referral_case = (
                not has_real_referral_evidence
            )

            def classification_result(classification):
                result = {
                    'ml_sec_domain_classification': classification,
                    'sec_domain_source': 'SimilarWeb',
                    'confidence': None,
                    'recommended_action_id': None,
                    'justification': None
                }

                if classification == 2:
                    if special_no_referral_case:
                        result['sec_domain_source'] = 'Ad Sniffer'
                        result['confidence'] = 'MEDIUM'
                        result['recommended_action_id'] = 2
                        result['justification'] = 2
                    else:
                        result['confidence'] = 'HIGH'
                        result['recommended_action_id'] = 1
                        result['justification'] = 1
                elif classification == 3:
                    if special_no_referral_case:
                        result['sec_domain_source'] = 'Ad Sniffer'
                        result['confidence'] = 'LOW'
                        result['recommended_action_id'] = 5
                        result['justification'] = 3
                    else:
                        result['confidence'] = 'MEDIUM'
                        result['recommended_action_id'] = 2
                        result['justification'] = 4

                return result

            # chech exclude domain
            if pd.notna(row['exc_domain_id']):
                return classification_result(4)

            if special_no_referral_case and current_classification in (2, 3):
                return classification_result(current_classification)

            # chequear google_search_results
            if pd.notna(row['google_search_results']):  # ver si este chequeo funciona bien
                # offline search
                if row['google_search_results'] < 2:
                    # Validar FP Risk antes de clasificar como 2
                    if is_fp_risk(row):
                        return None
                    return classification_result(2)
                # online search
                elif row['% Referrals Infringing'] > 0.5 and row['% Referrals CH Customer Infringing'] > 0.2 and row[
                    '% Direct+Referrals'] > 0.6:
                    # chequear si esta offline o bloqueado
                    if row['online_status'] == "Online":
                        if row['google_search_results'] < 10:
                            # Validar FP Risk antes de clasificar como 2
                            if is_fp_risk(row):
                                return None
                            return classification_result(2)
                        elif row['google_search_results'] >= 10 and row['google_search_results'] < 50:
                            return classification_result(3)
                        else:
                            return classification_result(4)
                    elif row['online_status'] in self.OFFLINE_STATUSES:
                        # Validar FP Risk antes de clasificar como 2
                        if is_fp_risk(row):
                            return None
                        return classification_result(2)
                # casos no previstos
                else:
                    if row['online_status'] in self.OFFLINE_STATUSES:
                        return classification_result(0)
                    return None
            # Sitios sin google_search_results
            elif row['% Referrals Infringing'] > 0.5 and row['% Referrals CH Customer Infringing'] > 0.2 and row[
                '% Direct+Referrals'] > 0.6:
                # chequear si esta offline o bloqueado
                if row['online_status'] == "Online":
                    return classification_result(3)
                elif row['online_status'] in self.OFFLINE_STATUSES:
                    # Validar FP Risk antes de clasificar como 2
                    if is_fp_risk(row):
                        return None
                    return classification_result(2)
            # casos no previstos
            else:
                if row['online_status'] in self.OFFLINE_STATUSES:
                    return classification_result(0)
                return None

        classification_results = sw_offline.apply(lambda row: check_domains(row), axis=1)
        sw_offline['ml_sec_domain_classification'] = classification_results.apply(
            lambda result: result['ml_sec_domain_classification'] if result else None
        )
        sw_offline['sec_domain_source_to_update'] = classification_results.apply(
            lambda result: result['sec_domain_source'] if result else None
        )
        sw_offline['confidence_to_update'] = classification_results.apply(
            lambda result: result['confidence'] if result else None
        )
        sw_offline['recommended_action_id_to_update'] = classification_results.apply(
            lambda result: result['recommended_action_id'] if result else None
        )
        sw_offline['justification_to_update'] = classification_results.apply(
            lambda result: result['justification'] if result else None
        )

        # Logging de FP Risk excluidos
        self.__logger.info(
            f"-- FP Risk Summary: {fp_risk_counts['total_excluded']} domains excluded from Referral Cloaking classification")
        self.__logger.info(f"   - traffic_high (>10M): {fp_risk_counts['traffic_high']}")
        self.__logger.info(f"   - organic_high (>=40%): {fp_risk_counts['organic_high']}")
        self.__logger.info(f"   - balanced_profile: {fp_risk_counts['balanced_profile']}")
        self.__logger.info(f"   - balanced_organic: {fp_risk_counts['balanced_organic']}")

        if fp_risk_domains:
            self.__logger.debug(
                f"-- FP Risk domains detail: {fp_risk_domains[:10]}...")  # Primeros 10 para no saturar log

        sw_offline.dropna(subset='ml_sec_domain_classification', inplace=True)

        # Log de clasificaciones finales
        classification_counts = sw_offline['ml_sec_domain_classification'].value_counts().to_dict()
        self.__logger.info(f"-- Classification Summary: {len(sw_offline)} domains classified")
        for class_id, count in sorted(classification_counts.items()):
            self.__logger.info(f"   - Class {int(class_id)}: {count} domains")

        df_filtered = sw_offline[['sec_domain_id', 'ml_sec_domain_classification']]
        df_filtered = df_filtered.copy()
        df_filtered['decision_source'] = 'SimilarWeb'
        df_filtered['sec_domain_source'] = sw_offline['sec_domain_source_to_update']
        df_filtered['confidence'] = sw_offline['confidence_to_update']
        df_filtered['recommended_action_id'] = sw_offline['recommended_action_id_to_update']
        df_filtered['justification'] = sw_offline['justification_to_update']
        data_to_save = df_filtered.to_dict('records')
        if not data_to_save:
            self.__logger.info('No classified domains to update')
            return
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
                (
                    domain['sec_domain_id'],
                    domain['ml_sec_domain_classification'],
                    domain['sec_domain_source'],
                    domain['decision_source'],
                    domain.get('confidence'),
                    domain.get('recommended_action_id'),
                    domain.get('justification')
                ) for domain in save_data
            ]

            # Crea un VALUES string gigante para el UPDATE masivo usando CTE
            values_template = ",".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(data_to_update))
            flat_values = []
            for tup in data_to_update:
                flat_values.extend(tup)  # aplanamos la lista para pasar a execute

            sql = f"""
                WITH updates (
                    sec_domain_id,
                    value_to_update,
                    sec_domain_source_to_update,
                    decision_source,
                    confidence_to_update,
                    recommended_action_id_to_update,
                    justification_to_update
                ) AS (
                    VALUES {values_template}
                )
                UPDATE public.secondary_domains AS t
                SET ml_sec_domain_classification = u.value_to_update,
                    sec_domain_source = u.sec_domain_source_to_update,
                    decision_source = u.decision_source,
                    confidence = COALESCE(u.confidence_to_update, t.confidence),
                    recommended_action_id = COALESCE(u.recommended_action_id_to_update, t.recommended_action_id),
                    justification = COALESCE(u.justification_to_update, t.justification)
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