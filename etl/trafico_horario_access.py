from pathlib import Path
import pandas as pd
import os
<<<<<<< HEAD

# Configuración de rutas
BASE_DIR = Path(__file__).parent.parent  # Directorio raíz del proyecto
PROCESSED_DATA = BASE_DIR / 'data' / 'processed' / 'trafico_horario_processed.csv'
ACCESS_DATA = BASE_DIR / 'data' / 'access' / 'trafico_horario_access.csv'

try:
    # 1. Lectura de datos
    df = pd.read_csv(PROCESSED_DATA)
    
    # 2. Procesamiento
    # Congestión por hora (usando moda)
    congestion_hora = df.groupby('hora')['nivel_congestion'].agg(lambda x: x.mode()[0])
    
    # Vehículos por hora
    tipos_por_hora = df.groupby('hora')[['coches', 'motos', 'camiones', 'buses']].sum()
    tipos_por_hora['vehiculo_predominante'] = tipos_por_hora[['coches', 'motos', 'camiones', 'buses']].idxmax(axis=1)
    
    # 3. Unión de resultados
    df_unido = pd.merge(
        congestion_hora,
        tipos_por_hora,
        on='hora',
        how='left'
    ).reset_index()
    
    # 4. Guardado seguro
    os.makedirs(ACCESS_DATA.parent, exist_ok=True)
    df_unido.to_csv(ACCESS_DATA, index=True)  # index=False para CSV más limpio
    
except FileNotFoundError as e:
    print(f"Error crítico: Archivo no encontrado en {e.filename}")
except pd.errors.EmptyDataError:
    print("Error: El archivo CSV está vacío")
except KeyError as e:
    print(f"Error: Columna no encontrada - {str(e)}")
except Exception as e:
    print(f"Error inesperado: {str(e)}")
=======
import boto3
from botocore.exceptions import ClientError

# Leer archivo preprocesado
df = pd.read_parquet('data/preprocessed/trafico_horario_preprocessed.parquet')

# Obtener nivel de congestión más frecuente por hora
congestion_hora = df.groupby('hora')['nivel_congestion'].agg(lambda x: x.mode()[0]).reset_index()

# Sumar tipos de vehículos por hora
tipos_por_hora = df.groupby('hora')[['coches', 'motos', 'camiones', 'buses']].sum().reset_index()

# Determinar el vehículo predominante por hora
tipos_por_hora['vehiculo_predominante'] = tipos_por_hora[['coches', 'motos', 'camiones', 'buses']].idxmax(axis=1)

# Unir ambos resultados
df_unido = pd.merge(congestion_hora, tipos_por_hora, on='hora')

# Guardar capa de acceso como archivo Parquet
os.makedirs('data/access', exist_ok=True)
access_path = 'data/access/trafico_horario_access.parquet'
df_unido.to_parquet(access_path, index=False)

# Subir a MinIO
minio = boto3.client(
    's3',
    endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin")
)

bucket = 'trafico-access'

# Crear bucket si no existe
try:
    minio.head_bucket(Bucket=bucket)
except ClientError:
    minio.create_bucket(Bucket=bucket)

# Subir archivo final a MinIO
with open(access_path, 'rb') as f:
    minio.upload_fileobj(f, bucket, 'trafico_horario_access.parquet')
>>>>>>> 32ca4d2f63703b489adbf2191a6c8b59c2fd1dc8
