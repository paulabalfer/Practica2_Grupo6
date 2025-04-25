
import pandas as pd
import os
import boto3
from botocore.exceptions import ClientError

# Leer datos crudos
df = pd.read_csv('data/raw/trafico-horario.csv')

# Limpieza básica
df = df.drop_duplicates()
df = df.dropna()

# Procesamiento de fechas y horas
df['fecha_hora'] = pd.to_datetime(df['fecha_hora'])
df['fecha'] = df['fecha_hora'].dt.date
df['hora'] = df['fecha_hora'].dt.hour

# Quitar columna original
df = df.drop(columns=['fecha_hora'])

# Guardar resultado preprocesado como Parquet
os.makedirs('data/preprocessed', exist_ok=True)
preprocessed_path = 'data/preprocessed/trafico_horario_preprocessed.parquet'
df.to_parquet(preprocessed_path, index=False)

# Subir a MinIO
minio = boto3.client(
    's3',
    endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin")
)

bucket = 'trafico-preprocessed'

# Crear bucket si no existe
try:
    minio.head_bucket(Bucket=bucket)
except ClientError:
    minio.create_bucket(Bucket=bucket)

# Subir archivo final a MinIO
with open(preprocessed_path, 'rb') as f:
    minio.upload_fileobj(f, bucket, 'trafico_horario_preprocessed.parquet')
