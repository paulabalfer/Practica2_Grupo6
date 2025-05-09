# from pathlib import Path
# import pandas as pd
# import os

# import boto3
# from botocore.exceptions import ClientError

# # Leer archivo preprocesado
# df = pd.read_parquet('data/processed/trafico_horario_processed.parquet')

# # Obtener nivel de congestión más frecuente por hora
# congestion_hora = df.groupby('hora')['nivel_congestion'].agg(lambda x: x.mode()[0]).reset_index()

# # Sumar tipos de vehículos por hora
# tipos_por_hora = df.groupby('hora')[['coches', 'motos', 'camiones', 'buses']].sum().reset_index()

# # Determinar el vehículo predominante por hora
# tipos_por_hora['vehiculo_predominante'] = tipos_por_hora[['coches', 'motos', 'camiones', 'buses']].idxmax(axis=1)

# # Unir ambos resultados
# df_unido = pd.merge(congestion_hora, tipos_por_hora, on='hora')

# # Guardar capa de acceso como archivo Parquet
# os.makedirs('data/access', exist_ok=True)
# access_path = 'data/access/trafico_horario_access.parquet'
# df_unido.to_parquet(access_path, index=False)

# # Subir a MinIO
# minio = boto3.client(
#     's3',
#     endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
#     aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
#     aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin")
# )

# bucket = 'trafico-access'

# # Crear bucket si no existe
# try:
#     minio.head_bucket(Bucket=bucket)
# except ClientError:
#     minio.create_bucket(Bucket=bucket)

# # Subir archivo final a MinIO
# with open(access_path, 'rb') as f:
#     minio.upload_fileobj(f, bucket, 'trafico_horario_access.parquet')

# # Crear bucket de gobernanza

# minio = boto3.client(
#     's3',
#     endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
#     aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
#     aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin")
# )

# bucket = 'governance'

# try:
#     minio.head_bucket(Bucket=bucket)
# except ClientError:
#     minio.create_bucket(Bucket=bucket)

# # Subir archivo final a MinIO
# with open(access_path, 'rb') as f:
#     minio.upload_fileobj(f, bucket, 'trafico_horario_access.parquet')

from pathlib import Path
import pandas as pd
from io import BytesIO
import boto3
import os
from botocore.exceptions import ClientError

# Configurar cliente boto3 para MinIO
s3 = boto3.client(
    's3',
    endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin")
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

# Procesar datos
congestion_hora = df.groupby('hora')['nivel_congestion'].agg(lambda x: x.mode()[0]).reset_index()
tipos_por_hora = df.groupby('hora')[['coches', 'motos', 'camiones', 'buses']].sum().reset_index()
tipos_por_hora['vehiculo_predominante'] = tipos_por_hora[['coches', 'motos', 'camiones', 'buses']].idxmax(axis=1)

df_unido = pd.merge(congestion_hora, tipos_por_hora, on='hora')

# Convertir DataFrame a Parquet en memoria
parquet_buffer = BytesIO()
df_unido.to_parquet(parquet_buffer, index=False)
parquet_data_bytes = parquet_buffer.getvalue()  # Guardamos copia del contenido
parquet_buffer.close()

# Subir archivo Parquet a MinIO (access)
bucket_output = 'access'
key_output = 'trafico_horario_access.parquet'

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
    with open("data/access/trafico_horario_access.parquet", "wb") as f:
        f.write(parquet_data_bytes)
    print("Archivo Parquet de access guardado localmente.")
except Exception as e:
    print(f"Error al guardar el archivo localmente: {e}")
    exit(1)

# Subir README.md al bucket 'governance'
bucket_governance = 'governance'
create_bucket_if_not_exists(bucket_governance)

try:
    with open('README.md', 'rb') as f:
        s3.upload_fileobj(f, bucket_governance, 'README.md')
    print("README.md subido al bucket 'governance'.")
except FileNotFoundError:
    print("ERROR: No se encontró el archivo README.md.")
except ClientError as e:
    print(f"Error subiendo README.md al bucket 'governance': {e}")
