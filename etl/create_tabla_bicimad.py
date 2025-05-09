import psycopg2
import pandas as pd
import psycopg2
import os
from pathlib import Path
from io import BytesIO
from io import StringIO
import boto3
import os
from botocore.exceptions import ClientError

# # Configuración de rutas
# BASE_DIR = Path(__file__).parent.parent  # Asume que el script está en proyecto/scripts/
# PROCESSED_DATA = BASE_DIR / 'data' / 'processed'
# csv_path = os.path.join(PROCESSED_DATA, 'bicimad_usos_processed.csv') # Ruta al archivo CSV

# Configurar cliente boto3 para MinIO
s3 = boto3.client(
    's3',
    endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin")
)

# Parámetros de conexión
db_user = 'postgres'
db_pass = 'postgres'
db_host = 'postgres'
db_port = '5432'
db_name = 'postgres'

# Conexión
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_pass,
    host=db_host,
    port=db_port
)
cur = conn.cursor() # inicio un cursos



# Leer parquet desde MinIO
bucket_input = 'processed'
key_input = 'bicimad_usos_processed.parquet'

try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)

# Definición de la tabla 
create_table_sql = """
CREATE TABLE IF NOT EXISTS bicimad_usos (
    id INT PRIMARY KEY,
    usuario_id INT,
    tipo_usuario VARCHAR(20),
    estacion_origen INT,
    estacion_destino INT,
    fecha DATE, 
    hora_inicio TIME,
    hora_fin TIME,
    duracion_segundos INT,
    distancia_km FLOAT,
    calorias_estimadas INT,
    co2_evitado_gramos INT
);
"""

# 1. Crear la tabla
cur.execute(create_table_sql)
conn.commit()

# 2. Insertar los datos fila a fila
for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO bicimad_usos (id, usuario_id, tipo_usuario, estacion_origen, estacion_destino, fecha, hora_inicio, hora_fin, duracion_segundos, distancia_km, calorias_estimadas, co2_evitado_gramos) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (row['id'], row['usuario_id'], row['tipo_usuario'], row['estacion_origen'], row['estacion_destino'], row['fecha'], row['hora_inicio'], row['hora_fin'], row['duracion_segundos'], row['distancia_km'], row['calorias_estimadas'], row['co2_evitado_gramos'])
    )
conn.commit()

# 3. (Opcional) Verifica cuántas filas se importaron
cur.execute("SELECT COUNT(*) FROM bicimad_usos;")
print(f"Filas importadas: {cur.fetchone()[0]}")

cur.close()
conn.close()
