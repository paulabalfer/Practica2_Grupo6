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
key_input = 'parkings_rotacion_processed.parquet'

try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df_rotacion = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)

# Leer archivo Parquet desde MinIO
bucket_input = 'processed'
key_input = 'aparcamientos_info_processed.parquet'

try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df_ubicacion = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)

def calcular_variaciones_ocupacion(rotacion, info):
    """
    Calcula las variaciones de ocupación y las combina con información de ubicación,
    devolviendo además plazas_libres, plazas_ocupadas y porcentaje_ocupacion por aparcamiento_id.
    """

    # 1. Preparación de los datos de rotación
    rotacion['fecha'] = pd.to_datetime(rotacion['fecha'])
    rotacion['dia_semana'] = rotacion['fecha'].dt.day_name() # Agregar nombre del día
    rotacion['semana_anio'] = rotacion['fecha'].dt.isocalendar().week  # Añadir número de semana

    # 2. Cálculo de la variación de ocupación diaria
    df_diario = rotacion.groupby(['aparcamiento_id', 'fecha'])['porcentaje_ocupacion'].agg(['mean', 'max', 'min'])
    df_diario['variacion_diaria'] = df_diario['max'] - df_diario['min'] # Rango de ocupación diario
    df_diario = df_diario.reset_index()

    # 3. Cálculo de la variación de ocupación semanal 
    df_semanal = rotacion.groupby(['aparcamiento_id', 'semana_anio'])['porcentaje_ocupacion'].agg(['max', 'min'])
    df_semanal['variacion_semanal'] = df_semanal['max'] - df_semanal['min']
    df_semanal = df_semanal.reset_index()

    # 4. Agregación final para obtener la variación promedio por aparcamiento 
    df_variacion_diaria = df_diario.groupby('aparcamiento_id')['variacion_diaria'].mean().reset_index()  # Promedio diario
    df_variacion_semanal = df_semanal.groupby('aparcamiento_id')['variacion_semanal'].mean().reset_index()  # Promedio semanal

    # 5. Combinar todos los resultados
    df_combinado = pd.merge(df_variacion_diaria, df_variacion_semanal, on='aparcamiento_id', how='left')

    return df_combinado

# Transformaciones
prepared = calcular_variaciones_ocupacion(df_rotacion, df_ubicacion)

# Convertir DataFrame a Parquet en memoria
parquet_buffer = BytesIO()
prepared.to_parquet(parquet_buffer, index=False)
parquet_data_bytes = parquet_buffer.getvalue()  # Guardamos copia del contenido
parquet_buffer.close()

# Subir archivo Parquet a MinIO (access)
bucket_output = 'access'
key_output = 'hecho_ocupacion.parquet'

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
    with open("data/access/hecho_ocupacion.parquet", "wb") as f:
        f.write(parquet_data_bytes)
    print("Archivo Parquet de access guardado localmente.")
except Exception as e:
    print(f"Error al guardar el archivo localmente: {e}")
    exit(1)
