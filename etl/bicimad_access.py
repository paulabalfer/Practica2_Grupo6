import pandas as pd
import psycopg2
import os
from pathlib import Path

# Configuración de rutas
BASE_DIR = Path(__file__).parent.parent  # Asume que el script está en proyecto/scripts/
PROCESSED_DATA = BASE_DIR / 'data' / 'processed'
ACCESS_DATA = BASE_DIR / 'data' / 'access'
os.makedirs(ACCESS_DATA, exist_ok=True)

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

# --- Consulta 1: Tabla de hechos densidad de población e infraestructuras de transportes ---

consulta_sql_1 = """
SELECT 
	et.distrito_id as id_distrito,
    d.nombre AS nombre_distrito,
    d.densidad_poblacion,
    et.id as estacion_id,
    et.linea_id as linea_id 
FROM distritos d
LEFT JOIN estaciones_transporte et ON d.id = et.distrito_id;
"""

# Ejecución e importación 
output_file_1 = 'hecho_densidad_y_transportes.csv'
df_1 = pd.read_sql_query(consulta_sql_1, conn)
output_path_1 = os.path.join(ACCESS_DATA, output_file_1)
df_1.to_csv(output_path_1, index=False)
print(f"Exportada {output_path_1}")

# --- Consulta 2: Dimensión de usuario para el objetivo 2 ---

consulta_sql_2 = """
SELECT 
	b.usuario_id, 
    b.tipo_usuario
FROM bicimad_usos b ;
"""

# Ejecución e importación 
output_file_2 = 'dim_usuario.csv'
df_2 = pd.read_sql_query(consulta_sql_2, conn)
output_path_2 = os.path.join(ACCESS_DATA, output_file_2)
df_2.to_csv(output_path_2, index=False)
print(f"Exportada {output_path_2}")

# --- Consulta 3: Dimensión de estación para el objetivo 2 ---

consulta_sql_3 = """
SELECT 
	et.id as estacion_id, 
    et.nombre as estacion_nombre, 
    et.linea_id as linea_id, 
    et.distrito_id, 
    et.latitud, 
    et.longitud
FROM estaciones_transporte et ;
"""

# Ejecución e importación 
output_file_3 = 'dim_estacion.csv'
df_3 = pd.read_sql_query(consulta_sql_3, conn)
output_path_3 = os.path.join(ACCESS_DATA, output_file_3)
df_3.to_csv(output_path_3, index=False)
print(f"Exportada {output_path_3}")

# --- Consulta 4: Dimensión de distrito para el objetivo 2 ---

consulta_sql_4 = """
SELECT 
	d.id as distrito_id, 
    d.nombre as distrito_nombre, 
    d.densidad_poblacion
FROM distritos d; 
"""

# Ejecución e importación 
output_file_4 = 'dim_distrito.csv'
df_4 = pd.read_sql_query(consulta_sql_4, conn)
output_path_4 = os.path.join(ACCESS_DATA, output_file_4)
df_4.to_csv(output_path_4, index=False)
print(f"Exportada {output_path_4}")

# --- Consulta 5: Dimensión de tiempo para el objetivo 2 ---

consulta_sql_5 = """
SELECT 
	b.fecha, 
    b.hora_inicio, 
    b.hora_fin
FROM bicimad_usos b; 
"""

# Ejecución e importación 
output_file_5 = 'dim_tiempo.csv'
df_5 = pd.read_sql_query(consulta_sql_5, conn)
output_path_5 = os.path.join(ACCESS_DATA, output_file_5)
df_5.to_csv(output_path_5, index=False)
print(f"Exportada {output_path_5}")

# "Copia" de los csvs para las que las tablas de hechos son fieles a ellos 
file_path_1 = os.path.join(PROCESSED_DATA, 'bicimad_usos_processed.csv')
output_path_6 = os.path.join(ACCESS_DATA, 'hecho_bicimad_usos.csv')
df_6 = pd.read_csv(file_path_1)
df_6.to_csv(output_path_6, index=False)
print(f"Exportada {output_path_6}")

file_path_2 = os.path.join(PROCESSED_DATA, 'aparcamientos_info_processed.csv')
output_path_7 = os.path.join(ACCESS_DATA, 'dim_aparcamiento.csv')
df_7 = pd.read_csv(file_path_2)
df_7.to_csv(output_path_7, index=False)
print(f"Exportada {output_path_7}")

conn.close()
