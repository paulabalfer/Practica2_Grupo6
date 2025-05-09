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

# Archivos a subir
files = [
    ('data/raw/trafico-horario.csv', 'trafico-horario.csv'),
    ('data/raw/avisamadrid.json', 'avisamadrid.json'),
    ('data/raw/ext_aparcamientos_info.csv', 'ext_aparcamientos_info.csv'),
    ('data/raw/parkings-rotacion.csv', 'parkings-rotacion.csv'),
    ('data/raw/bicimad-usos.csv', 'bicimad-usos.csv'),
    ('data/raw/dump-bbdd-municipal.sql', 'dump-bbdd-municipal.sql')
]

# Subir archivos uno por uno
for local_path, object_name in files:
    try:
        with open(local_path, 'rb') as f:
            minio.upload_fileobj(f, bucket, object_name)
        print(f"{object_name} subido correctamente.")
    except FileNotFoundError:
        print(f"ERROR: No se encontró el archivo {local_path}.")
    except ClientError as e:
        print(f"Error subiendo {object_name} a MinIO: {e}")
