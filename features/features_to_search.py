import psycopg2
import pandas as pd
from datetime import datetime
from settings import db_connect
import os



# Fecha actual en formato YYYYMMDD
today = datetime.now().strftime("%Y-%m-%d")

# Directorio de salida
OUTPUT_DIR = "domain_cleaned"

# Crear directorio si no existe
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Queries con nombres base para los archivos
QUERIES = {
    f"ssl_poor_{today}.csv": """
        SELECT DISTINCT sd.sec_domain
        FROM secondary_domains sd
        WHERE 
            sd.online_status = 'Online'
            AND sd.redirect_domain = false
            AND sd.ssl_poor IS NULL;
    """,
    f"mfa_engagement_{today}.csv": """
        SELECT DISTINCT  sd.sec_domain
        FROM secondary_domains sd
        WHERE 
            sd.online_status = 'Online'
            AND sd.redirect_domain = false
            AND sd.mfa_engagement IS NULL;
    """,
    f"high_traffic_{today}.csv": """
        SELECT DISTINCT  sd.sec_domain
        FROM secondary_domains sd
        WHERE 
            sd.online_status = 'Online'
            AND sd.redirect_domain = false
            AND sd.high_traffic IS NULL;
    """
}


def main():
    try:
        # Conexión a la base de datos
        conn = psycopg2.connect(**db_connect)

        for filename, query in QUERIES.items():
            # Ejecutar cada query y cargar en DataFrame
            df = pd.read_sql_query(query, conn)

            # Ruta completa dentro de domain_cleaned
            filepath = os.path.join(OUTPUT_DIR, filename)

            # Guardar en CSV
            df.to_csv(filepath, index=False)
            print(f"Archivo generado: {filepath}")

        conn.close()
    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    main()
