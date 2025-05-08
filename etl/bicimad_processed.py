import pandas as pd
import psycopg2
import os
from pathlib import Path

# Configuración de rutas
BASE_DIR = Path(__file__).parent.parent  # Asume que el script está en proyecto/scripts/
PROCESSED_DATA = BASE_DIR / 'data' / 'processed'
os.makedirs(PROCESSED_DATA, exist_ok=True)

# Parámetros de conexión
db_user = 'postgres'
db_pass = 'postgres'
db_host = 'postgres'  # nombre del servicio en docker-compose
db_port = '5432'
db_name = 'postgres'

# Conexión a la base de datos
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_pass,
    host=db_host,
    port=db_port
)

# Obtener el listado de tablas del esquema público
with conn.cursor() as cur:
    cur.execute("""
        SELECT tablename 
        FROM pg_catalog.pg_tables 
        WHERE schemaname='public';
    """)
    tablas = [row[0] for row in cur.fetchall()]

# Exportar cada tabla a CSV
for tabla in tablas:
    df = pd.read_sql_query(f'SELECT * FROM "{tabla}"', conn)
    output_path = os.path.join(PROCESSED_DATA, f'{tabla}.csv')
    df.to_csv(output_path, index=False)
    print(f"Exportada {output_path}")

conn.close()
