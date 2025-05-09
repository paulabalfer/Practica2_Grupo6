import pandas as pd
import psycopg2
import os
from pathlib import Path

# Configuración de rutas
BASE_DIR = Path(__file__).parent.parent  # Asume que el script está en proyecto/scripts/
RAW_DATA = BASE_DIR /'data' / 'raw'
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

    # Limpieza y procesado de datos 
    df  = df.drop_duplicates().dropna()
    if 'fecha' in df.columns: 
        df['fecha'] = pd.to_datetime(df['fecha'])  # Aseguro formato datetime
        df['dia_semana'] = df['fecha'].dt.day_name() # Agregar nombre del día

    output_path = os.path.join(PROCESSED_DATA, f'{tabla}.csv')
    df.to_csv(output_path, index=False)
    print(f"Exportada {output_path}")

conn.close()

# Proceso y limpio tambien datos de bicimad-usos.csv 
file = os.path.join(RAW_DATA, 'bicimad-usos.csv')
df = pd.read_csv(file)

# Limpieza 

df  = df.drop_duplicates().dropna()
# Convertir las columnas a datetime
df['fecha_hora_inicio'] = pd.to_datetime(df['fecha_hora_inicio'])
df['fecha_hora_fin'] = pd.to_datetime(df['fecha_hora_fin'])
# Crear nuevas columnas como string
df['fecha'] = df['fecha_hora_inicio'].dt.strftime('%Y-%m-%d')
df['hora_inicio'] = df['fecha_hora_inicio'].dt.strftime('%H:%M:%S')
df['hora_fin'] = df['fecha_hora_fin'].dt.strftime('%H:%M:%S')
# Borrar las columnas originales
df = df.drop(['fecha_hora_inicio', 'fecha_hora_fin'], axis=1)

# Guardar datos
output_path = os.path.join(PROCESSED_DATA, 'bicimad_usos_processed.csv')
df.to_csv(output_path, index=False)
print(f"Exportada {output_path}")