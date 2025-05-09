import pandas as pd
import psycopg2
import os
from pathlib import Path
from io import BytesIO
from io import StringIO
import boto3
from botocore.exceptions import ClientError

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

def create_bucket_if_not_exists(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError:
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' creado.")
        except ClientError as e:
            print(f"Error creando el bucket '{bucket_name}': {e}")
            raise

# Leer archivo Parquet desde MinIO
bucket_input = 'processed'
key_input = 'trafico_horario_processed.parquet'

try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)

# --- Consulta 1: Tabla de hechos densidad de población e infraestructuras de transportes ---

consulta_sql_1 = """
SELECT 
	et.distrito_id as id_distrito,
    d.nombre AS nombre_distrito,
    d.densidad_poblacion,
    et.id as estacion_id,
    et.linea_id as linea_id 
FROM distritos d
LEFT JOIN estaciones_transporte et ON d.id = et.distrito_id;
"""

# Ejecución e importación 

df_1 = pd.read_sql_query(consulta_sql_1, conn)

# Convertir DataFrame a Parquet en memoria
parquet_buffer = BytesIO()
df_1.to_parquet(parquet_buffer, index=False)
parquet_data_bytes = parquet_buffer.getvalue()  # Guardamos copia del contenido
parquet_buffer.close()

# Subir archivo Parquet a MinIO (access)
bucket_output = 'access'
key_output = 'hecho_densidad_y_transportes.parquet'

try:
    create_bucket_if_not_exists(bucket_output)
    s3.upload_fileobj(BytesIO(parquet_data_bytes), bucket_output, key_output)
    print("Archivo Parquet de access subido a MinIO.")
except ClientError as e:
    print(f"Error subiendo archivo a bucket 'access': {e}")
    exit(1)

# Guardar localmente
Path("data/access").mkdir(parents=True, exist_ok=True)
try:
    with open("data/access/hecho_densidad_y_transportes.parquet", "wb") as f:
        f.write(parquet_data_bytes)
    print("Archivo Parquet de access guardado localmente.")
except Exception as e:
    print(f"Error al guardar el archivo localmente: {e}")
    exit(1)

# --- Consulta 2: Dimensión de usuario para el objetivo 2 ---

consulta_sql_2 = """
SELECT 
	b.usuario_id, 
    b.tipo_usuario
FROM bicimad_usos b ;
"""


# Ejecución e importación 

df_2 = pd.read_sql_query(consulta_sql_2, conn)

# Convertir DataFrame a Parquet en memoria
parquet_buffer = BytesIO()
df_2.to_parquet(parquet_buffer, index=False)
parquet_data_bytes = parquet_buffer.getvalue()  # Guardamos copia del contenido
parquet_buffer.close()

# Subir archivo Parquet a MinIO (access)
bucket_output = 'access'
key_output = 'dim_usuario.parquet'

try:
    create_bucket_if_not_exists(bucket_output)
    s3.upload_fileobj(BytesIO(parquet_data_bytes), bucket_output, key_output)
    print("Archivo Parquet de access subido a MinIO.")
except ClientError as e:
    print(f"Error subiendo archivo a bucket 'access': {e}")
    exit(1)

# Guardar localmente
Path("data/access").mkdir(parents=True, exist_ok=True)
try:
    with open("data/access/dim_usuario.parquet", "wb") as f:
        f.write(parquet_data_bytes)
    print("Archivo Parquet de access guardado localmente.")
except Exception as e:
    print(f"Error al guardar el archivo localmente: {e}")
    exit(1)
    
# --- Consulta 3: Dimensión de estación para el objetivo 2 ---

consulta_sql_3 = """
SELECT 
	et.id as estacion_id, 
    et.nombre as estacion_nombre, 
    et.linea_id as linea_id, 
    et.distrito_id, 
    et.latitud, 
    et.longitud
FROM estaciones_transporte et ;
"""

# Ejecución e importación 
output_file_3 = 'dim_estacion.csv'
df_3 = pd.read_sql_query(consulta_sql_3, conn)

# Convertir DataFrame a Parquet en memoria
parquet_buffer = BytesIO()
df_3.to_parquet(parquet_buffer, index=False)
parquet_data_bytes = parquet_buffer.getvalue()  # Guardamos copia del contenido
parquet_buffer.close()

# Subir archivo Parquet a MinIO (access)
bucket_output = 'access'
key_output = 'dim_estacion.parquet'

try:
    create_bucket_if_not_exists(bucket_output)
    s3.upload_fileobj(BytesIO(parquet_data_bytes), bucket_output, key_output)
    print("Archivo Parquet de access subido a MinIO.")
except ClientError as e:
    print(f"Error subiendo archivo a bucket 'access': {e}")
    exit(1)

# Guardar localmente
Path("data/access").mkdir(parents=True, exist_ok=True)
try:
    with open("data/access/dim_estacion.parquet", "wb") as f:
        f.write(parquet_data_bytes)
    print("Archivo Parquet de access guardado localmente.")
except Exception as e:
    print(f"Error al guardar el archivo localmente: {e}")
    exit(1)

# --- Consulta 4: Dimensión de distrito para el objetivo 2 ---

consulta_sql_4 = """
SELECT 
	d.id as distrito_id, 
    d.nombre as distrito_nombre, 
    d.densidad_poblacion
FROM distritos d; 
"""

# Ejecución e importación 

df_4 = pd.read_sql_query(consulta_sql_4, conn)

# Convertir DataFrame a Parquet en memoria
parquet_buffer = BytesIO()
df_4.to_parquet(parquet_buffer, index=False)
parquet_data_bytes = parquet_buffer.getvalue()  # Guardamos copia del contenido
parquet_buffer.close()

# Subir archivo Parquet a MinIO (access)
bucket_output = 'access'
key_output = 'dim_distrito.parquet'

try:
    create_bucket_if_not_exists(bucket_output)
    s3.upload_fileobj(BytesIO(parquet_data_bytes), bucket_output, key_output)
    print("Archivo Parquet de access subido a MinIO.")
except ClientError as e:
    print(f"Error subiendo archivo a bucket 'access': {e}")
    exit(1)

# Guardar localmente
Path("data/access").mkdir(parents=True, exist_ok=True)
try:
    with open("data/access/dim_distrito.parquet", "wb") as f:
        f.write(parquet_data_bytes)
    print("Archivo Parquet de access guardado localmente.")
except Exception as e:
    print(f"Error al guardar el archivo localmente: {e}")
    exit(1)

# --- Consulta 5: Dimensión de tiempo para el objetivo 2 ---

consulta_sql_5 = """
SELECT 
	b.fecha, 
    b.hora_inicio, 
    b.hora_fin
FROM bicimad_usos b; 
"""

# Ejecución e importación 
output_file_5 = 'dim_tiempo.csv'
df_5 = pd.read_sql_query(consulta_sql_5, conn)

# Convertir DataFrame a Parquet en memoria
parquet_buffer = BytesIO()
df_5.to_parquet(parquet_buffer, index=False)
parquet_data_bytes = parquet_buffer.getvalue()  # Guardamos copia del contenido
parquet_buffer.close()

# Subir archivo Parquet a MinIO (access)
bucket_output = 'access'
key_output = 'dim_tiempo.parquet'

try:
    create_bucket_if_not_exists(bucket_output)
    s3.upload_fileobj(BytesIO(parquet_data_bytes), bucket_output, key_output)
    print("Archivo Parquet de access subido a MinIO.")
except ClientError as e:
    print(f"Error subiendo archivo a bucket 'access': {e}")
    exit(1)

# Guardar localmente
Path("data/access").mkdir(parents=True, exist_ok=True)
try:
    with open("data/access/dim_tiempo.parquet", "wb") as f:
        f.write(parquet_data_bytes)
    print("Archivo Parquet de access guardado localmente.")
except Exception as e:
    print(f"Error al guardar el archivo localmente: {e}")
    exit(1)

# "Copia" de los csvs para las que las tablas de hechos son fieles a ellos

# --------- BICIMAD USOS ---------
bucket_input = 'processed'
key_input = 'bicimad_usos_processed.parquet'

try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df_bicimad = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)
    
# Convertir DataFrame a Parquet en memoria
parquet_buffer_bicimad = BytesIO()
df_bicimad.to_parquet(parquet_buffer_bicimad, index=False)
parquet_data_bytes_bicimad = parquet_buffer_bicimad.getvalue()
parquet_buffer_bicimad.close()

# Subir archivo Parquet a MinIO (access)
bucket_output = 'access'
key_output = 'hecho_bicimad_usos.parquet'

try:
    create_bucket_if_not_exists(bucket_output)
    s3.upload_fileobj(BytesIO(parquet_data_bytes_bicimad), bucket_output, key_output)
    print("Archivo Parquet de access subido a MinIO.")
except ClientError as e:
    print(f"Error subiendo archivo a bucket 'access': {e}")
    exit(1)

# Guardar localmente
Path("data/access").mkdir(parents=True, exist_ok=True)
try:
    with open("data/access/hecho_bicimad_usos.parquet", "wb") as f:
        f.write(parquet_data_bytes_bicimad)
    print("Archivo Parquet de access guardado localmente.")
except Exception as e:
    print(f"Error al guardar el archivo localmente: {e}")
    exit(1)

# --------- APARCAMIENTOS INFO ---------
bucket_input = 'processed'
key_input = 'aparcamientos_info_processed.parquet'

try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df_aparcamientos = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)

# Convertir DataFrame a Parquet en memoria
parquet_buffer_aparcamientos = BytesIO()
df_aparcamientos.to_parquet(parquet_buffer_aparcamientos, index=False)
parquet_data_bytes_aparcamientos = parquet_buffer_aparcamientos.getvalue()
parquet_buffer_aparcamientos.close()

# Subir archivo Parquet a MinIO (access)
bucket_output = 'access'
key_output = 'dim_aparcamiento.parquet'

try:
    create_bucket_if_not_exists(bucket_output)
    s3.upload_fileobj(BytesIO(parquet_data_bytes_aparcamientos), bucket_output, key_output)
    print("Archivo Parquet de access subido a MinIO.")
except ClientError as e:
    print(f"Error subiendo archivo a bucket 'access': {e}")
    exit(1)

# Guardar localmente
Path("data/access").mkdir(parents=True, exist_ok=True)
try:
    with open("data/access/dim_aparcamiento.parquet", "wb") as f:
        f.write(parquet_data_bytes_aparcamientos)
    print("Archivo Parquet de access guardado localmente.")
except Exception as e:
    print(f"Error al guardar el archivo localmente: {e}")
    exit(1)

conn.close()
