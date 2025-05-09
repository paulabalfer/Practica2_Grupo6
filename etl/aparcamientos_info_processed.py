# from pathlib import Path
# import pandas as pd
# import os

# # Configuración de rutas
# BASE_DIR = Path(__file__).parent.parent  # Asume que el script está en proyecto/scripts/
# RAW_DATA = BASE_DIR / 'data' / 'raw' / 'ext_aparcamientos_info.csv'
# PROCESSED_DATA = BASE_DIR / 'data' / 'processed' / 'aparcamientos_info_processed.csv'

# try: 
#     # Cargar datos
#     rotacion = pd.read_csv(RAW_DATA)

#     # Transformaciones
#     rotacion = rotacion.drop_duplicates().dropna()

#     # Guardar datos 
#     os.makedirs(PROCESSED_DATA.parent, exist_ok=True)
#     rotacion.to_csv(PROCESSED_DATA, index=True)
    
# except FileNotFoundError:
#     print(f"Error: Archivo no encontrado en {RAW_DATA}")
# except Exception as e:
#     print(f"Error inesperado: {str(e)}")

import pandas as pd
from io import BytesIO
from io import StringIO
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

# Leer CSV desde MinIO
bucket_input = 'raw'
key_input = 'ext_aparcamientos_info.csv'


try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    csv_data = StringIO(response['Body'].read().decode('utf-8'))
except ClientError as e:
    print(f"Error al leer el archivo desde MinIO: {e}")
    exit(1)

# Procesar con pandas
rotacion = pd.read_csv(csv_data)

# Transformaciones
rotacion = rotacion.drop_duplicates().dropna()

# Convertir a Parquet en memoria
parquet_buffer = BytesIO()
rotacion.to_parquet(parquet_buffer, index=False)
parquet_buffer.seek(0)

# Guardar el contenido del buffer en una variable temporal
parquet_data = parquet_buffer.read()

# Subir a MinIO directamente desde memoria
bucket_output = 'processed'
key_output = 'aparcamientos_info_processed.parquet'

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
    with open('data/processed/aparcamientos_info_processed.parquet', 'wb') as f:
        f.write(parquet_data)
    print("Archivo Parquet guardado localmente.")
except Exception as e:
    print(f"Error al guardar el archivo localmente: {e}")
    exit(1)