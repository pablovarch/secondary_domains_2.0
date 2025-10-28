from dependencies import log
from settings import db_connect, db_connect_df
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime


class mfa_engagement:
    def __init__(self):
        self.__logger = log.Log().get_logger(name='mfa_enagement.log')

    def main(self):
        # Correcting the syntax error by removing the invalid 'DB Connection' line
        alchemyEngine = create_engine(db_connect_df, pool_recycle=3600)

        dbConnection = alchemyEngine.connect()
        engagement = pd.read_sql("""
                                WITH ranked AS (
                                    SELECT
                                        dem.sec_domain_id,
                                        dem.month,
                                        dem.pages_per_visit,
                                        dem.bounce_rate,
                                        dem.avg_visit_duration,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY dem.sec_domain_id
                                            ORDER BY dem.month DESC          -- el mes más nuevo queda primero
                                        ) AS rn
                                    FROM dim_engagement_metrics dem
                                    WHERE dem.sec_domain_id IS NOT NULL
                                    and ( dem.avg_visit_duration is not null and dem.avg_visit_duration != '0')                
                                )
                                SELECT
                                    sec_domain_id,
                                    month,
                                    pages_per_visit,
                                    bounce_rate,
                                    avg_visit_duration
                                FROM ranked
                                WHERE rn = 1;                                 -- solo la fila “más nueva” por dominio
                                """, dbConnection)

        dbConnection.close()

        # # Convert avg_visit_duration to seconds for comparison
        # engagement['avg_visit_duration_seconds'] = engagement['avg_visit_duration'].apply(
        #     lambda x: sum(int(t) * 60 ** i for i, t in enumerate(reversed(x.split(':'))))
        # )
        def duration_to_seconds(x, two_part='auto'):
            """
            Convierte 'hh:mm:ss' | 'mm:ss' | 'hh:mm' | 'ss' a segundos.
            - Mantiene NaN/None/Missing como NaN.
            - Acepta enteros/floats (se interpretan como segundos).
            - Para dos partes:
                - two_part='auto' (default): si el 2º componente >=60 => 'hh:mm', si no => 'mm:ss'
                - two_part='mm:ss' o 'hh:mm' para forzar una interpretación.
            """
            # nulos
            if x is None or (isinstance(x, float) and np.isnan(x)):
                return np.nan

            s = str(x).strip()
            if s == '' or s.lower() in {'nan', 'none', 'missing'}:
                return np.nan

            # ya numérico (ej. "0", 0, 12.0)
            try:
                if s.replace('.', '', 1).isdigit():
                    return int(float(s))
            except Exception:
                pass

            if ':' not in s:
                return np.nan

            # soportar h:m:s(.fff)
            parts = [p.split('.')[0].strip() for p in s.split(':')]
            try:
                parts = list(map(int, parts))
            except ValueError:
                return np.nan

            if len(parts) == 3:  # hh:mm:ss
                h, m, sec = parts
                return h * 3600 + m * 60 + sec
            elif len(parts) == 2:  # mm:ss  ó  hh:mm
                a, b = parts
                if two_part == 'hh:mm':
                    return a * 3600 + b * 60
                if two_part == 'mm:ss':
                    return a * 60 + b
                # auto (heurística)
                return a * 3600 + b * 60 if b >= 60 else a * 60 + b
            elif len(parts) == 1:  # "ss"
                return parts[0]
            else:
                return np.nan

        # Uso:
        engagement['avg_visit_duration_seconds'] = (
            engagement['avg_visit_duration'].apply(duration_to_seconds)  # two_part='auto' por defecto
        )


        # Add the mfa_engagement column based on the conditions
        engagement['mfa_engagement'] = (
                (engagement['bounce_rate'] >= 0.65) &
                (engagement['pages_per_visit'] <= 3) &
                (engagement['avg_visit_duration_seconds'] <= 180)
        )
        print(engagement)
        df_filtered = engagement[['sec_domain_id', 'mfa_engagement']]
        data_to_save = df_filtered.to_dict('records')
        self.update_domains(data_to_save)

        def duration_to_seconds(x, two_part='auto'):
            """
            Convierte 'hh:mm:ss' | 'mm:ss' | 'hh:mm' | 'ss' a segundos.
            - Mantiene NaN/None/Missing como NaN.
            - Acepta enteros/floats (se interpretan como segundos).
            - Para dos partes:
                - two_part='auto' (default): si el 2º componente >=60 => 'hh:mm', si no => 'mm:ss'
                - two_part='mm:ss' o 'hh:mm' para forzar una interpretación.
            """
            # nulos
            if x is None or (isinstance(x, float) and np.isnan(x)):
                return np.nan

            s = str(x).strip()
            if s == '' or s.lower() in {'nan', 'none', 'missing'}:
                return np.nan

            # ya numérico (ej. "0", 0, 12.0)
            try:
                if s.replace('.', '', 1).isdigit():
                    return int(float(s))
            except Exception:
                pass

            if ':' not in s:
                return np.nan

            # soportar h:m:s(.fff)
            parts = [p.split('.')[0].strip() for p in s.split(':')]
            try:
                parts = list(map(int, parts))
            except ValueError:
                return np.nan

            if len(parts) == 3:  # hh:mm:ss
                h, m, sec = parts
                return h * 3600 + m * 60 + sec
            elif len(parts) == 2:  # mm:ss  ó  hh:mm
                a, b = parts
                if two_part == 'hh:mm':
                    return a * 3600 + b * 60
                if two_part == 'mm:ss':
                    return a * 60 + b
                # auto (heurística)
                return a * 3600 + b * 60 if b >= 60 else a * 60 + b
            elif len(parts) == 1:  # "ss"
                return parts[0]
            else:
                return np.nan



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
                (domain['sec_domain_id'], domain['mfa_engagement']) for domain in save_data
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
                SET mfa_engagement = u.value_to_update
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