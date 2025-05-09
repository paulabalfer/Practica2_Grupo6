import pandas as pd
import boto3
from io import BytesIO

# Conexión a MinIO (ajusta si usas otras credenciales o endpoint)
s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

# Cargar archivo desde MinIO (bucket y clave)
obj = s3.get_object(Bucket='trafico-access', Key='trafico_horario_access.parquet')

# Usar BytesIO para poder leer el archivo en memoria
parquet_data = BytesIO(obj['Body'].read())

# Leer el archivo parquet
df = pd.read_parquet(parquet_data)

# Tu lógica original
mayor_congestion = df[df['nivel_congestion'] == 'Muy Alta']
print('Horarios de mayor congestion de trafico en Madrid y tipos de vehiculos predominantes:')
for _, fila in mayor_congestion.iterrows():
    hora = fila['hora']
    vehiculo = fila['vehiculo_predominante']
    print(f"A las {hora}:00 horas, el vehiculo predominante es el {vehiculo}.")
