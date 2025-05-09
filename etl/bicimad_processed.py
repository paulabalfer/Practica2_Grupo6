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
# RAW_DATA = BASE_DIR /'data' / 'raw'
# PROCESSED_DATA = BASE_DIR / 'data' / 'processed'
# os.makedirs(PROCESSED_DATA, exist_ok=True)


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

# Exportar cada tabla a parquet
for tabla in tablas:
    df = pd.read_sql_query(f'SELECT * FROM "{tabla}"', conn)

    # Limpieza y procesado de datos 
    df  = df.drop_duplicates().dropna()
    if 'fecha' in df.columns: 
        df['fecha'] = pd.to_datetime(df['fecha'])  # Aseguro formato datetime
        df['dia_semana'] = df['fecha'].dt.day_name() # Agregar nombre del día

    # output_path = os.path.join(PROCESSED_DATA, f'{tabla}.parquet')
    # df.to_parquet(output_path, index=False)
    # print(f"Exportada {output_path}")
    
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)  # Importante: volver al inicio del buffer

    bucket_name = "processed"  # Ajusta esto al bucket real
    object_key = f"{tabla}.parquet"  # Nombre del archivo en el bucket

    try:
        s3.upload_fileobj(buffer, bucket_name, object_key)
        print(f"{tabla}.parquet subido a MinIO en el bucket '{bucket_name}'")
    except ClientError as e:
        print(f"Error al subir {tabla}.parquet: {e}")

conn.close()

# Proceso y limpio tambien datos de bicimad-usos.csv 

# Leer CSV desde MinIO
bucket_input = 'raw'
key_input = 'bicimad-usos.csv'


try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    csv_data = StringIO(response['Body'].read().decode('utf-8'))
except ClientError as e:
    print(f"Error al leer el archivo desde MinIO: {e}")
    exit(1)

df = pd.read_csv(csv_data)

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


# Convertir a Parquet en memoria
parquet_buffer = BytesIO()
df.to_parquet(parquet_buffer, index=False)
parquet_buffer.seek(0)

# Guardar el contenido del buffer en una variable temporal
parquet_data = parquet_buffer.read()

# Subir a MinIO directamente desde memoria
bucket_output = 'processed'
key_output = 'bicimad_usos_processed.parquet'

# Crear bucket si no existe
try:
    s3.head_bucket(Bucket=bucket_output)
except ClientError:
    try:
        s3.create_bucket(Bucket=bucket_output)
        print(f"Bucket '{bucket_output}' creado.")
    except ClientError as e:
        print(f"Error al crear el bucket: {e}")
        exit(1)

# Subir archivo usando un nuevo buffer con los mismos datos
try:
    s3.upload_fileobj(BytesIO(parquet_data), bucket_output, key_output)
    print("Archivo Parquet procesado subido correctamente a MinIO.")
except ClientError as e:
    print(f"Error al subir el archivo a MinIO: {e}")
    exit(1)

# Guardar localmente desde los mismos datos
os.makedirs('data/processed', exist_ok=True)
try:
    with open('data/processed/bicimad_usos_processed.parquet', 'wb') as f:
        f.write(parquet_data)
    print("Archivo Parquet guardado localmente.")
except Exception as e:
    print(f"Error al guardar el archivo localmente: {e}")
    exit(1)


# # Guardar datos
# output_path = os.path.join(PROCESSED_DATA, 'bicimad_usos_processed.csv')
# df.to_csv(output_path, index=False)
# print(f"Exportada {output_path}")