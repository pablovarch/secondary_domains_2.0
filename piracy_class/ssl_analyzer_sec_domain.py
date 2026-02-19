import asyncio
import logging
import os
import re
from datetime import datetime
from typing import Any

import requests
from psycopg2 import pool

from settings import db_connect, ssl_apikey

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DOMAIN_PROCESS_LIMIT = int(os.getenv('DOMAIN_PROCESS_LIMIT', '1500'))
MAX_CONCURRENT_REQUESTS = int(os.getenv('MAX_CONCURRENT_REQUESTS', '10'))
HTTP_TIMEOUT_SECONDS = int(os.getenv('HTTP_TIMEOUT_SECONDS', '30'))
DB_POOL_MIN_CONN = int(os.getenv('DB_POOL_MIN_CONN', '1'))
DB_POOL_MAX_CONN = int(os.getenv('DB_POOL_MAX_CONN', '10'))

db_pool: pool.ThreadedConnectionPool | None = None


def init_db_pool():
    global db_pool
    if db_pool is None:
        db_pool = pool.ThreadedConnectionPool(
            DB_POOL_MIN_CONN,
            DB_POOL_MAX_CONN,
            db_connect,
        )


def close_db_pool():
    global db_pool
    if db_pool:
        db_pool.closeall()
        db_pool = None


def get_db_connection():
    global db_pool
    if db_pool is None:
        init_db_pool()
    return db_pool.getconn()


def release_db_connection(conn):
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)


def normalize_domain(value: str) -> str:
    s = (value or "").strip().lower()
    if not s:
        return ""
    s = re.sub(r"^[a-z][a-z0-9+.-]*://", "", s)
    s = s.split("/")[0]
    s = s.split("?")[0]
    s = s.split("#")[0]
    if ":" in s and not s.startswith("["):
        s = s.split(":")[0]
    s = s.strip(".")
    return s


def canonical_domain(domain: str) -> str:
    d = normalize_domain(domain)
    if d.startswith('www.'):
        return d.removeprefix('www.')
    return d


def domain_variants(domain: str) -> list[str]:
    base = canonical_domain(domain)
    if not base:
        return []
    return [base, f'www.{base}']


def empty_ssl_row(requested_domain: str) -> dict[str, Any]:
    return {
        'requested_domain': requested_domain,
        'ip': None,
        'port': None,
        'audit_created': None,
        'chain_hierarchy': None,
        'validation_type': None,
        'valid_from': None,
        'valid_to': None,
        'serial_number': None,
        'signature_algorithm': None,
        'subject_common_name': None,
        'issuer_country': None,
        'issuer_organization': None,
        'issuer_common_name': None,
        'authority_key_identifier': None,
        'subject_key_identifier': None,
        'key_usage': None,
        'extended_key_usage': None,
        'crl_distribution_points': None,
        'aia_issuers': None,
        'aia_ocsp': None,
        'dns_names': None,
        'certificate_policies': None,
        'public_key_type': None,
        'public_key_bits': None,
    }


def get_existing_requested_domains(candidates: list[str]) -> set[str]:
    if not candidates:
        return set()
    conn = None
    cursor = None
    existing: set[str] = set()
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            'SELECT requested_domain FROM domain_ssl_data WHERE requested_domain = ANY(%s)',
            (candidates,),
        )
        for (d,) in cursor.fetchall():
            nd = normalize_domain(str(d or ''))
            if nd:
                existing.add(nd)
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)
    return existing


def get_domains_missing_ssl(limit: int) -> list[str]:
    conn = None
    cursor = None
    domains: list[str] = []

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = """
                SELECT sd.sec_domain_root
                FROM secondary_domains sd
                WHERE sd.sec_domain_media_type_id IS NOT NULL
                AND sd.sec_domain_piracy_class_v2_id IS NULL
                AND sd.sec_domain_root IS NOT NULL
                ORDER BY sd.sec_domain_id DESC
            LIMIT %s
        """
        prefetch_limit = min(limit * 5, 20000)
        cursor.execute(sql, (prefetch_limit,))
        raw_domains: list[str] = []
        for (d,) in cursor.fetchall():
            nd = normalize_domain(str(d or ''))
            if nd:
                raw_domains.append(nd)
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)

    seen: set[str] = set()
    ordered_unique: list[str] = []
    for d in raw_domains:
        if d not in seen:
            seen.add(d)
            ordered_unique.append(d)

    all_candidates: list[str] = []
    for d in ordered_unique:
        all_candidates.extend(domain_variants(d))

    candidates_unique = list(dict.fromkeys(all_candidates))
    existing = get_existing_requested_domains(candidates_unique)

    for d in ordered_unique:
        variants = domain_variants(d)
        if not any(v in existing for v in variants):
            domains.append(d)
            if len(domains) >= limit:
                break

    return domains


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        s = str(value)
        return datetime.fromisoformat(s.replace('Z', '').replace('z', ''))
    except Exception:
        return None


def process_ssl_response(response_data: dict[str, Any] | None, requested_domain: str) -> list[dict[str, Any]]:
    if not isinstance(response_data, dict):
        return [empty_ssl_row(requested_domain)]

    certificates = response_data.get('certificates')
    if not isinstance(certificates, list) or not certificates:
        return [empty_ssl_row(requested_domain)]

    ip = response_data.get('ip')
    port = response_data.get('port')
    audit_created = _parse_iso_datetime(response_data.get('auditCreated'))

    formatted: list[dict[str, Any]] = []
    for cert in certificates:
        if not isinstance(cert, dict):
            continue

        subject = cert.get('subject')
        subject_common_name = subject.get('commonName') if isinstance(subject, dict) else None

        issuer = cert.get('issuer')
        issuer_country = issuer.get('country') if isinstance(issuer, dict) else None
        issuer_organization = issuer.get('organization') if isinstance(issuer, dict) else None
        issuer_common_name = issuer.get('commonName') if isinstance(issuer, dict) else None

        extensions = cert.get('extensions')
        if isinstance(extensions, dict):
            authority_key_identifier = extensions.get('authorityKeyIdentifier')
            subject_key_identifier = extensions.get('subjectKeyIdentifier')
            key_usage = str(extensions.get('keyUsage', ''))
            extended_key_usage = str(extensions.get('extendedKeyUsage', ''))
            crl_distribution_points = str(extensions.get('crlDistributionPoints', ''))

            authority_info_access = extensions.get('authorityInfoAccess')
            if isinstance(authority_info_access, dict):
                aia_issuers = str(authority_info_access.get('issuers', ''))
                aia_ocsp = str(authority_info_access.get('ocsp', ''))
            else:
                aia_issuers = ''
                aia_ocsp = ''

            subject_alt_names = extensions.get('subjectAlternativeNames')
            dns_names = str(subject_alt_names.get('dnsNames', '')) if isinstance(subject_alt_names, dict) else ''
            certificate_policies = str(extensions.get('certificatePolicies', ''))
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

        public_key = cert.get('publicKey')
        public_key_type = public_key.get('type') if isinstance(public_key, dict) else None
        public_key_bits = public_key.get('bits') if isinstance(public_key, dict) else None

        formatted.append(
            {
                'requested_domain': requested_domain,
                'ip': ip,
                'port': port,
                'audit_created': audit_created,
                'chain_hierarchy': cert.get('chainHierarchy'),
                'validation_type': cert.get('validationType'),
                'valid_from': _parse_iso_datetime(cert.get('validFrom')),
                'valid_to': _parse_iso_datetime(cert.get('validTo')),
                'serial_number': cert.get('serialNumber'),
                'signature_algorithm': cert.get('signatureAlgorithm'),
                'subject_common_name': subject_common_name,
                'issuer_country': issuer_country,
                'issuer_organization': issuer_organization,
                'issuer_common_name': issuer_common_name,
                'authority_key_identifier': authority_key_identifier,
                'subject_key_identifier': subject_key_identifier,
                'key_usage': key_usage,
                'extended_key_usage': extended_key_usage,
                'crl_distribution_points': crl_distribution_points,
                'aia_issuers': aia_issuers,
                'aia_ocsp': aia_ocsp,
                'dns_names': dns_names,
                'certificate_policies': certificate_policies,
                'public_key_type': public_key_type,
                'public_key_bits': public_key_bits,
            }
        )

    return formatted if formatted else [empty_ssl_row(requested_domain)]


def _domain_has_ssl(conn, requested_domain: str) -> bool:
    variants = domain_variants(requested_domain)
    if not variants:
        return False
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT 1 FROM domain_ssl_data WHERE requested_domain = ANY(%s) LIMIT 1',
            (variants,),
        )
        return cursor.fetchone() is not None
    finally:
        cursor.close()


def domain_has_ssl(requested_domain: str) -> bool:
    conn = None
    try:
        conn = get_db_connection()
        return _domain_has_ssl(conn, requested_domain)
    finally:
        release_db_connection(conn)


def insert_ssl_rows_for_domain(requested_domain: str, rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False

    lock_key = canonical_domain(requested_domain) or normalize_domain(requested_domain)

    conn = None
    cursor = None
    inserted = False

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT pg_advisory_lock(hashtext(%s))', (lock_key,))
        conn.commit()
        try:
            if _domain_has_ssl(conn, requested_domain):
                return False

            sql = """
                INSERT INTO domain_ssl_data (
                    requested_domain,
                    ip,
                    port,
                    audit_created,
                    chain_hierarchy,
                    validation_type,
                    valid_from,
                    valid_to,
                    serial_number,
                    signature_algorithm,
                    subject_common_name,
                    issuer_country,
                    issuer_organization,
                    issuer_common_name,
                    authority_key_identifier,
                    subject_key_identifier,
                    key_usage,
                    extended_key_usage,
                    crl_distribution_points,
                    aia_issuers,
                    aia_ocsp,
                    dns_names,
                    certificate_policies,
                    public_key_type,
                    public_key_bits
                ) VALUES (
                    %(requested_domain)s,
                    %(ip)s,
                    %(port)s,
                    %(audit_created)s,
                    %(chain_hierarchy)s,
                    %(validation_type)s,
                    %(valid_from)s,
                    %(valid_to)s,
                    %(serial_number)s,
                    %(signature_algorithm)s,
                    %(subject_common_name)s,
                    %(issuer_country)s,
                    %(issuer_organization)s,
                    %(issuer_common_name)s,
                    %(authority_key_identifier)s,
                    %(subject_key_identifier)s,
                    %(key_usage)s,
                    %(extended_key_usage)s,
                    %(crl_distribution_points)s,
                    %(aia_issuers)s,
                    %(aia_ocsp)s,
                    %(dns_names)s,
                    %(certificate_policies)s,
                    %(public_key_type)s,
                    %(public_key_bits)s
                )
            """
            cursor.executemany(sql, rows)
            conn.commit()
            inserted = True
        finally:
            cursor.execute('SELECT pg_advisory_unlock(hashtext(%s))', (lock_key,))
            conn.commit()
    except Exception as e:
        logger.error(f'Error inserting SSL rows for {requested_domain}: {e}')
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        release_db_connection(conn)

    return inserted


def fetch_ssl_from_api(domain: str) -> dict[str, Any] | None:
    url = f'https://ssl-certificates.whoisxmlapi.com/api/v1?apiKey={ssl_apikey}&domainName={domain}'
    try:
        resp = requests.get(url, timeout=HTTP_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.warning(f'SSL API request failed for {domain}: {e}')
        return None


async def backfill_domain(domain: str, semaphore: asyncio.Semaphore) -> tuple[str, str]:
    async with semaphore:
        # Re-check right before hitting the API to avoid unnecessary calls in concurrent/multi-run scenarios
        already_has_ssl = await asyncio.to_thread(domain_has_ssl, domain)
        if already_has_ssl:
            return (domain, 'skipped')

        data = await asyncio.to_thread(fetch_ssl_from_api, domain)
        rows = process_ssl_response(data, domain)
        inserted = await asyncio.to_thread(insert_ssl_rows_for_domain, domain, rows)

        if data is None:
            return (domain, 'inserted_empty' if inserted else 'skipped')
        if rows and rows[0].get('ip') is None and rows[0].get('validation_type') is None and len(rows) == 1:
            return (domain, 'inserted_empty' if inserted else 'skipped')
        return (domain, 'inserted' if inserted else 'skipped')


async def run_backfill():
    logger.info('Starting ssl analyzer script')
    if not ssl_apikey:
        raise ValueError('Missing ssl_apikey in settings')

    init_db_pool()
    try:
        domains = get_domains_missing_ssl(DOMAIN_PROCESS_LIMIT)
        logger.info(f'Found {len(domains)} domains missing SSL data')
        if not domains:
            return

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        results = await asyncio.gather(
            *(backfill_domain(d, semaphore) for d in domains),
            return_exceptions=True,
        )

        inserted = 0
        inserted_empty = 0
        skipped = 0
        error = 0

        for r in results:
            if isinstance(r, Exception):
                error += 1
                continue
            _, status = r
            if status == 'inserted':
                inserted += 1
            elif status == 'inserted_empty':
                inserted_empty += 1
            elif status == 'skipped':
                skipped += 1
            else:
                error += 1

        logger.info('=' * 60)
        logger.info('EXECUTION SUMMARY')
        logger.info(f'Total domains: {len(domains)}')
        logger.info(f'Inserted: {inserted}')
        logger.info(f'Inserted empty: {inserted_empty}')
        logger.info(f'Skipped: {skipped}')
        logger.info(f'Errors: {error}')
        logger.info('=' * 60)
    finally:
        close_db_pool()


class ssl_analyzer:
    def main(self):
        asyncio.run(run_backfill())


if __name__ == '__main__':
    ssl_analyzer().main()