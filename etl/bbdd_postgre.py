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

# Leer archivo Parquet desde MinIO
bucket_input = 'access'

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
cur = conn.cursor()

# -- Tabla hechos 1: hecho_bicimad_usos ---

# Definición de la tabla 
create_fact_bicimad = """
CREATE TABLE IF NOT EXISTS fact_bicimad_usos (
    id INT,
    usuario_id INT,
    tipo_usuario VARCHAR(20),
    estacion_origen INT,
    estacion_destino INT,
    fecha DATE, 
    hora_inicio TIME,
    hora_fin TIME,
    duracion_segundos INT,
    distancia_km FLOAT,
    calorias_estimadas INT,
    co2_evitado_gramos INT
);
"""

# Ejecución e importación de datos
key_input = 'hecho_bicimad_usos.parquet'
try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)


cur.execute(create_fact_bicimad)  # Crear la tabla
conn.commit()
print('Tabla creada fact_bicimad_usos.')

# Insertar los datos fila a fila
for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO fact_bicimad_usos (id, usuario_id, tipo_usuario, estacion_origen, estacion_destino, fecha, hora_inicio, hora_fin, duracion_segundos, distancia_km, calorias_estimadas, co2_evitado_gramos) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (row['id'], row['usuario_id'], row['tipo_usuario'], row['estacion_origen'], row['estacion_destino'], row['fecha'], row['hora_inicio'], row['hora_fin'], row['duracion_segundos'], row['distancia_km'], row['calorias_estimadas'], row['co2_evitado_gramos'])
    )
conn.commit()

# 3. (Opcional) Verifica cuántas filas se importaron
cur.execute("SELECT COUNT(*) FROM fact_bicimad_usos;")
print(f"Filas importadas en fact_bicimad_usos: {cur.fetchone()[0]}")

# -- Tabla hechos 2: hecho_ocupacion ---

# Definición de la tabla 
create_fact_ocupacion = """
CREATE TABLE IF NOT EXISTS fact_ocupacion (
    id_aparcamiento INT,
    variacion_diaria FLOAT, 
    variacion_semanal FLOAT
);
"""

# Ejecución e importación de datos 
key_input = 'hecho_ocupacion.parquet'
try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)

cur.execute(create_fact_ocupacion)  # Crear la tabla
conn.commit()
print('Tabla creada fact_ocupacion.')

# Insertar los datos fila a fila
for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO fact_ocupacion (id_aparcamiento, variacion_diaria, variacion_semanal) VALUES (%s, %s, %s)",
        (row['aparcamiento_id'], row['variacion_diaria'], row['variacion_semanal'])
    )
conn.commit()

# 3. (Opcional) Verifica cuántas filas se importaron
cur.execute("SELECT COUNT(*) FROM fact_ocupacion;")
print(f"Filas importadas en fact_ocupacion: {cur.fetchone()[0]}")

# -- Tabla hechos 3: hecho_densidad_y_transportes ---

# Definición de la tabla 
create_fact_densidad_y_transportes = """
CREATE TABLE IF NOT EXISTS fact_densidad_y_transportes (
    distrito_id FLOAT, 
    distrito_nombre TEXT, 
    densidad_poblacion FLOAT, 
    estacion_id FLOAT, 
    linea_id FLOAT 
);
"""

# Ejecución e importación de datos 
key_input = 'hecho_densidad_y_transportes.parquet'

try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)
    

cur.execute(create_fact_densidad_y_transportes)  # Crear la tabla
conn.commit()
print('Tabla creada fact_densidad_y_transportes.')

# Insertar los datos fila a fila
for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO fact_densidad_y_transportes (distrito_id, distrito_nombre, densidad_poblacion, estacion_id, linea_id) VALUES (%s, %s, %s, %s, %s)",
        (row['id_distrito'], row['nombre_distrito'], row['densidad_poblacion'], row['estacion_id'], row['linea_id'])
    )
conn.commit()

# 3. (Opcional) Verifica cuántas filas se importaron
cur.execute("SELECT COUNT(*) FROM fact_densidad_y_transportes;")
print(f"Filas importadas en fact_densidad_y_transportes: {cur.fetchone()[0]}")

# -- Tabla dimensiones 1: dim_usuario ---

# Definición de la tabla 
create_dim_usuario = """
CREATE TABLE IF NOT EXISTS dim_usuario (
    usuario_id INT, 
    tipo_usuario TEXT
);
"""

# Ejecución e importación de datos 
key_input = 'dim_usuario.parquet'

try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)

cur.execute(create_dim_usuario)  # Crear la tabla
conn.commit()
print('Tabla creada dim_usuario.')

# Insertar los datos fila a fila
for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO dim_usuario (usuario_id, tipo_usuario) VALUES (%s, %s)",
        (row['usuario_id'], row['tipo_usuario'])
    )
conn.commit()

# 3. (Opcional) Verifica cuántas filas se importaron
cur.execute("SELECT COUNT(*) FROM dim_usuario;")
print(f"Filas importadas en dim_usuario: {cur.fetchone()[0]}")

# -- Tabla dimensiones 2: dim_estacion---

# Definición de la tabla 
create_dim_estacion = """
CREATE TABLE IF NOT EXISTS dim_estacion (
    estacion_id INT, 
    estacion_nombre TEXT, 
    linea_id INT, 
    distrito_id INT, 
    latitud FLOAT, 
    longitud FLOAT
);
"""

# Ejecución e importación de datos 
key_input= 'dim_estacion.parquet'
try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)

cur.execute(create_dim_estacion)  # Crear la tabla
conn.commit()
print('Tabla creada dim_estacion.')

# Insertar los datos fila a fila
for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO dim_estacion (estacion_id, estacion_nombre, linea_id, distrito_id, latitud, longitud) VALUES (%s, %s, %s, %s, %s, %s)",
        (row['estacion_id'], row['estacion_nombre'], row['linea_id'], row['distrito_id'], row['latitud'], row['longitud'])
    )
conn.commit()

# 3. (Opcional) Verifica cuántas filas se importaron
cur.execute("SELECT COUNT(*) FROM dim_estacion;")
print(f"Filas importadas en dim_estacion: {cur.fetchone()[0]}")

# -- Tabla dimensiones 3: dim_distrito---

# Definición de la tabla 
create_dim_distrito = """
CREATE TABLE IF NOT EXISTS dim_distrito (
    distrito_id INT, 
    distrito_nombre TEXT, 
    densidad_poblacion FLOAT
);
"""

# Ejecución e importación de datos 
key_input= 'dim_distrito.parquet'

try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)

cur.execute(create_dim_distrito)  # Crear la tabla
conn.commit()
print('Tabla creada dim_distrito.')

# Insertar los datos fila a fila
for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO dim_distrito (distrito_id, distrito_nombre, densidad_poblacion) VALUES (%s, %s, %s)",
        (row['distrito_id'], row['distrito_nombre'], row['densidad_poblacion'])
    )
conn.commit()

# 3. (Opcional) Verifica cuántas filas se importaron
cur.execute("SELECT COUNT(*) FROM dim_distrito;")
print(f"Filas importadas en dim_distrito: {cur.fetchone()[0]}")

# -- Tabla dimensiones 4: dim_tiempo ---

# Definición de la tabla 
create_dim_tiempo = """
CREATE TABLE IF NOT EXISTS dim_tiempo (
    fecha DATE, 
    hora_inicio TIME, 
    hora_fin TIME
);
"""

# Ejecución e importación de datos 
key_input = 'dim_tiempo.parquet'

try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)

cur.execute(create_dim_tiempo)  # Crear la tabla
conn.commit()
print('Tabla creada dim_tiempo.')

# Insertar los datos fila a fila
for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO dim_tiempo (fecha, hora_inicio, hora_fin) VALUES (%s, %s, %s)",
        (row['fecha'], row['hora_inicio'], row['hora_fin'])
    )
conn.commit()

# 3. (Opcional) Verifica cuántas filas se importaron
cur.execute("SELECT COUNT(*) FROM dim_tiempo;")
print(f"Filas importadas en dim_tiempo: {cur.fetchone()[0]}")

# -- Tabla dimensiones 5: dim_aparcamiento ---

# Definición de la tabla 
create_dim_aparcamiento = """
CREATE TABLE IF NOT EXISTS dim_aparcamiento (
    aparcamiento_id INT, 
    aparcamiento_nombre TEXT, 
    direccion TEXT, 
    capacidad_total INT, 
    plazas_movilidad_reducida INT, 
    plazas_vehiculos_electricos INT, 
    tarifa_hora_euros FLOAT, 
    horario TEXT, 
    latitud FLOAT, 
    longitud FLOAT
);
"""

# Ejecución e importación de datos 
key_input = 'dim_aparcamiento.parquet'

try:
    response = s3.get_object(Bucket=bucket_input, Key=key_input)
    parquet_data = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_data)
except ClientError as e:
    print(f"Error leyendo el archivo desde MinIO: {e}")
    exit(1)

cur.execute(create_dim_aparcamiento)  # Crear la tabla
conn.commit()
print('Tabla creada dim_aparcamiento.')

# Insertar los datos fila a fila
for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO dim_aparcamiento (aparcamiento_id, aparcamiento_nombre, direccion, capacidad_total, plazas_movilidad_reducida, plazas_vehiculos_electricos, tarifa_hora_euros, horario, latitud, longitud) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (row['aparcamiento_id'], row['nombre'], row['direccion'], row['capacidad_total'], row['plazas_movilidad_reducida'], row['plazas_vehiculos_electricos'], row['tarifa_hora_euros'], row['horario'], row['latitud'], row['longitud'])
    )
conn.commit()

# 3. (Opcional) Verifica cuántas filas se importaron
cur.execute("SELECT COUNT(*) FROM dim_aparcamiento;")
print(f"Filas importadas en dim_aparcamiento: {cur.fetchone()[0]}")

cur.close()
conn.close()