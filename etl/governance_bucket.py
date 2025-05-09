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