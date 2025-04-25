import boto3
from botocore.exceptions import ClientError
import os

# Conectar a MinIO usando boto3
minio = boto3.client(
    's3',
    endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),  # URL de MinIO
    aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),       # Clave de acceso
    aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin")    # Clave secreta
)

bucket = 'raw'  # Nombre del bucket que quieres crear

# Intentar acceder al bucket, si no existe, lo creamos
try:
    minio.head_bucket(Bucket=bucket)
except ClientError:
    minio.create_bucket(Bucket=bucket)

raw_path = 'data/raw/trafico-horario.csv'

# Subir archivo final a MinIO
with open(raw_path, 'rb') as f:
    minio.upload_fileobj(f, bucket, 'trafico-horario.csv')