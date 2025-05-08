import psycopg2
import pandas as pd
import os
from pathlib import Path

# Configuración de rutas
BASE_DIR = Path(__file__).parent.parent  # Asume que el script está en proyecto/scripts/
RAW_DATA = BASE_DIR / 'data' / 'raw'
csv_path = os.path.join(RAW_DATA, 'bicimad-usos.csv') # Ruta al archivo CSV

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
cur = conn.cursor() # inicio un cursos


# Lee el CSV con pandas
df = pd.read_csv(csv_path)

# Definición de la tabla 
create_table_sql = """
CREATE TABLE IF NOT EXISTS bicimad_usos (
    id INT PRIMARY KEY,
    usuario_id INT,
    tipo_usuario VARCHAR(20),
    estacion_origen INT,
    estacion_destino INT,
    fecha_hora_inicio TIMESTAMP,
    fecha_hora_fin TIMESTAMP,
    duracion_segundos INT,
    distancia_km FLOAT,
    calorias_estimadas INT,
    co2_evitado_gramos INT
);
"""

# 1. Crear la tabla
cur.execute(create_table_sql)
conn.commit()

# 2. Insertar los datos fila a fila
for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO bicimad_usos (id, usuario_id, tipo_usuario, estacion_origen, estacion_destino, fecha_hora_inicio, fecha_hora_fin, duracion_segundos, distancia_km, calorias_estimadas, co2_evitado_gramos) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (row['id'], row['usuario_id'], row['tipo_usuario'], row['estacion_origen'], row['estacion_destino'], row['fecha_hora_inicio'], row['fecha_hora_fin'], row['duracion_segundos'], row['distancia_km'], row['calorias_estimadas'], row['co2_evitado_gramos'])
    )
conn.commit()

# 3. (Opcional) Verifica cuántas filas se importaron
cur.execute("SELECT COUNT(*) FROM bicimad_usos;")
print(f"Filas importadas: {cur.fetchone()[0]}")

cur.close()
conn.close()
