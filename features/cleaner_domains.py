# features/cleaner_domains_py.py

import argparse
import csv
import ipaddress
import sys
import os
from urllib.parse import urlsplit
from datetime import date
import pandas as pd

try:
    from sqlalchemy import create_engine as _create_engine
except Exception:
    _create_engine = None

# =========================
#  SQLs
# =========================
SQL_QUERY = """
    SELECT DISTINCT sd.sec_domain_id, sd.sec_domain, ssl_poor, mfa_engagement, high_traffic
    FROM secondary_domains sd
    WHERE 
        sd.online_status = 'Online'
        AND sd.redirect_domain = false
        AND (
            sd.ssl_poor IS NULL
            OR sd.mfa_engagement IS NULL
            OR sd.high_traffic IS NULL
          ) 
"""

SQL_TLD_LIST = "SELECT tld FROM tld_list"

# =========================
#  Plataformas hosteadas
# =========================
HOSTED_SUFFIXES = {
    "pages.dev", "workers.dev", "onrocket.site", "github.io", "vercel.app",
    "netlify.app", "herokuapp.com", "readthedocs.io", "cloudfront.net",
    "azurewebsites.net",
}

# =========================
#  Heurística (sin deps)
# =========================
MULTIPART_SUFFIXES = {
    "co.uk", "ac.uk",
    "com.br", "com.np", "com.pk",
    "org.ng",
    "co.tz",
    "biz.id",
}

# =========================
#  tldextract (opcional)
# =========================
_USE_TLDEXTRACT = False
try:
    import tldextract  # pip install tldextract
    _extract = tldextract.TLDExtract(suffix_list_urls=None)  # snapshot local
    _USE_TLDEXTRACT = True
except Exception:
    _USE_TLDEXTRACT = False


class CleanerDomain:
    """
    Limpia y exporta dominios desde DB a CSVs con 'clean_domain',
    excluyendo plataformas hosteadas e IPs. Permite usar una regla
    basada en tld_list (misma lógica que en tu SQL).
    """

    # ---------- Helpers URL/Host ----------
    @staticmethod
    def _normalize_url(u: str) -> str:
        u = (u or "").strip()
        if not u:
            return ""
        if "://" not in u and not u.startswith("//"):
            u = "http://" + u
        return u

    @staticmethod
    def _host_from_url(u: str) -> str:
        u = CleanerDomain._normalize_url(u)
        p = urlsplit(u)
        netloc = p.netloc

        if "@" in netloc:
            netloc = netloc.split("@", 1)[1]

        if netloc.startswith("["):  # IPv6 entre corchetes
            end = netloc.find("]")
            host = netloc[1:end] if end != -1 else netloc
        else:
            host = netloc.split(":", 1)[0]  # quitar puerto

        host = host.rstrip(".").lower()
        try:
            host = host.encode("idna").decode("ascii")
        except Exception:
            pass
        return host

    # ---------- Extracción con tldextract ----------
    @staticmethod
    def _extract_with_tldextract(host: str, keep_hosted_label: bool):
        try:
            ipaddress.ip_address(host)
            return {
                "host": host,
                "registered_domain": host,
                "subdomain": None,
                "suffix": "",
                "is_ip": True,
                "is_hosted_platform": False,
                "hosted_project": None,
            }
        except ValueError:
            pass

        ext = _extract(host)
        suffix = ext.suffix or ""
        domain = ext.domain or ""
        sub = ext.subdomain or None
        registered = ".".join([p for p in (domain, suffix) if p])

        is_hosted = registered in HOSTED_SUFFIXES
        hosted_project = None
        if keep_hosted_label and is_hosted and sub:
            first_label = sub.split(".")[-1]
            hosted_project = f"{first_label}.{registered}"

        return {
            "host": host,
            "registered_domain": registered,
            "subdomain": sub,
            "suffix": suffix,
            "is_ip": False,
            "is_hosted_platform": is_hosted,
            "hosted_project": hosted_project,
        }

    # ---------- Extracción heurística ----------
    @staticmethod
    def _extract_heuristic(host: str, keep_hosted_label: bool):
        try:
            ipaddress.ip_address(host)
            return {
                "host": host,
                "registered_domain": host,
                "subdomain": None,
                "suffix": "",
                "is_ip": True,
                "is_hosted_platform": False,
                "hosted_project": None,
            }
        except ValueError:
            pass

        labels = host.split(".")
        if len(labels) < 2:
            return {
                "host": host,
                "registered_domain": host,
                "subdomain": None,
                "suffix": "",
                "is_ip": False,
                "is_hosted_platform": False,
                "hosted_project": None,
            }

        registered = ".".join(labels[-2:])
        sub = ".".join(labels[:-2]) or None
        suffix = labels[-1]

        # sufijo múltiple conocido
        for sfx in MULTIPART_SUFFIXES:
            if registered.endswith("." + sfx) or registered == sfx:
                if len(labels) >= 3:
                    registered = ".".join(labels[-3:])
                    sub = ".".join(labels[:-3]) or None
                    suffix = ".".join(sfx.split(".")[-2:]) if "." in sfx else sfx
                break

        is_hosted = registered in HOSTED_SUFFIXES
        hosted_project = None
        if keep_hosted_label and is_hosted and sub:
            first_label = sub.split(".")[-1]
            hosted_project = f"{first_label}.{registered}"

        return {
            "host": host,
            "registered_domain": registered,
            "subdomain": sub,
            "suffix": suffix,
            "is_ip": False,
            "is_hosted_platform": is_hosted,
            "hosted_project": hosted_project,
        }

    # ---------- Regla SQL penúltimo∈tld_list → 3 labels ----------
    @staticmethod
    def _clean_domain_sql_rule(host: str, tld_set: set) -> str:
        host_no_port = host.split(":", 1)[0]
        try:
            ipaddress.ip_address(host_no_port)
            return host_no_port
        except ValueError:
            pass

        labels = [l for l in host_no_port.split(".") if l]
        if len(labels) < 2:
            return host_no_port

        penultimo = labels[-2]
        if penultimo in tld_set and len(labels) >= 3:
            return ".".join(labels[-3:])
        else:
            return ".".join(labels[-2:])

    # ---------- Carga tld_list ----------
    @staticmethod
    def _load_tld_set(engine) -> set:
        try:
            df_tld = pd.read_sql_query(SQL_TLD_LIST, con=engine)
            col = None
            for c in df_tld.columns:
                if c.lower() == "tld":
                    col = c
                    break
            if col is None or df_tld.empty:
                return set()
            return set(df_tld[col].astype(str).str.lower().str.strip().tolist())
        except Exception:
            return set()

    # ---------- API pública de extracción ----------
    @staticmethod
    def extract_domain(url_or_host: str, keep_hosted_label: bool = False, tld_set: set | None = None):
        host = CleanerDomain._host_from_url(url_or_host)
        if not host:
            return {
                "host": "",
                "registered_domain": "",
                "subdomain": None,
                "suffix": "",
                "is_ip": False,
                "is_hosted_platform": False,
                "hosted_project": None,
            }

        if tld_set:
            try:
                ipaddress.ip_address(host)
                is_ip = True
            except ValueError:
                is_ip = False

            registered = CleanerDomain._clean_domain_sql_rule(host, tld_set)
            is_hosted = registered in HOSTED_SUFFIXES
            hosted_project = None
            if keep_hosted_label and is_hosted:
                labels = host.split(".")
                reg_labels = registered.split(".")
                if len(labels) > len(reg_labels):
                    first_label = labels[-len(reg_labels)-1]
                    hosted_project = f"{first_label}.{registered}"

            return {
                "host": host,
                "registered_domain": registered,
                "subdomain": None,
                "suffix": "",
                "is_ip": is_ip,
                "is_hosted_platform": is_hosted,
                "hosted_project": hosted_project,
            }

        if _USE_TLDEXTRACT:
            return CleanerDomain._extract_with_tldextract(host, keep_hosted_label)
        else:
            return CleanerDomain._extract_heuristic(host, keep_hosted_label)

    # ---------- Procesamiento / CSV ----------
    @staticmethod
    def process_domains(domains, output_csv: str, keep_hosted_label: bool = False, tld_set: set | None = None):
        seen_clean = set()
        os.makedirs(os.path.dirname(os.path.abspath(output_csv)) or ".", exist_ok=True)
        with open(output_csv, "w", newline="", encoding="utf-8") as out:
            writer = csv.writer(out)
            writer.writerow(["clean_domain"])
            for raw in domains:
                raw = (str(raw) if raw is not None else "").strip()
                if not raw:
                    continue
                res = CleanerDomain.extract_domain(raw, keep_hosted_label=keep_hosted_label, tld_set=tld_set)
                if res.get("is_ip"):
                    continue
                if res["is_hosted_platform"]:
                    continue
                clean_domain = res["hosted_project"] or res["registered_domain"]
                if clean_domain and clean_domain not in seen_clean:
                    seen_clean.add(clean_domain)
                    writer.writerow([clean_domain])

    # ---------- Flujo desde DB ----------
    @staticmethod
    def run_from_db(db_url: str | None = None, out_dir: str | None = None, keep_hosted_label: bool = False):
        """
        Ejecuta SQL_QUERY contra la DB y genera CSVs.
        Los archivos se guardan en la carpeta 'domains_cleaned' y llevan la fecha en el nombre.
        """
        # Resolver db_url
        if db_url is None:
            try:
                from settings import db_url as _db_from_settings
                db_url = _db_from_settings
            except Exception:
                db_url = None
        if db_url is None:
            db_url = os.getenv("DATABASE_URL")

        if not db_url:
            raise SystemExit("No se pudo determinar la URL de base de datos (db_url).")

        if _create_engine is None:
            raise SystemExit("SQLAlchemy no está disponible para crear el engine.")

        engine = _create_engine(db_url)

        # Cargar tld_list
        tld_set = CleanerDomain._load_tld_set(engine)

        df = pd.read_sql_query(SQL_QUERY, con=engine)
        if "sec_domain" not in df.columns:
            raise SystemExit(f"La query no devolvió la columna 'sec_domain'. Columnas: {list(df.columns)}")

        # 🔹 Forzamos salida en carpeta domains_cleaned (dentro de out_dir si se da)
        base_dir = out_dir or os.path.dirname(__file__) or "."
        output_dir = os.path.join(base_dir, "domains_cleaned")
        os.makedirs(output_dir, exist_ok=True)

        # 🔹 Fecha de hoy para el sufijo del archivo
        today_str = date.today().strftime("%Y-%m-%d")

        datasets = [
            ("ssl_poor", "ssl_poor"),
            ("mfa_engagement", "mfa_engagement"),
            ("high_traffic", "high_traffic"),
        ]
        any_written = False
        for name, col in datasets:
            if col in df.columns:
                subset = df[df[col].isna()]
                if len(subset) > 0:
                    out_path = os.path.join(output_dir, f"{name}_clean_{today_str}.csv")
                    CleanerDomain.process_domains(
                        subset["sec_domain"].astype(str).tolist(),
                        out_path,
                        keep_hosted_label=keep_hosted_label,
                        tld_set=tld_set,
                    )
                    any_written = True

        if not any_written:
            out_path = os.path.join(output_dir, f"all_clean_{today_str}.csv")
            CleanerDomain.process_domains(
                df["sec_domain"].astype(str).tolist(),
                out_path,
                keep_hosted_label=keep_hosted_label,
                tld_set=tld_set,
            )

    # ---------- CLI ----------
    @staticmethod
    def _parse_args(argv):
        ap = argparse.ArgumentParser(
            description="Genera CSVs con 'clean_domain' desde la DB. Excluye plataformas hosteadas y aplica la regla penúltimo∈tld_list→3 labels."
        )
        ap.add_argument("--db-url", dest="db_url", default=None,
                        help="Cadena de conexión SQLAlchemy (si no, usa settings.db_url o env DATABASE_URL)")
        ap.add_argument("--out-dir", dest="out_dir", default=None,
                        help="Directorio base (se creará domains_cleaned dentro de éste)")
        ap.add_argument("--keep-hosted-label", action="store_true",
                        help="Conservar el label de proyecto en plataformas hosteadas (p. ej. foo.pages.dev)")
        return ap.parse_args(argv)

    @staticmethod
    def main(argv=None):
        """
        Punto de entrada invocable desde __init__.py o desde consola.
        """
        args = CleanerDomain._parse_args(sys.argv[1:] if argv is None else argv)
        CleanerDomain.run_from_db(
            db_url=args.db_url,
            out_dir=args.out_dir,
            keep_hosted_label=args.keep_hosted_label,
        )


if __name__ == "__main__":
    CleanerDomain.main()
