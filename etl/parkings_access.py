from pathlib import Path
import pandas as pd
import os

# Configuración de rutas
BASE_DIR = Path(__file__).parent.parent  # Asume que el script está en proyecto/scripts/
RAW_PARKINGS_DATA = BASE_DIR / 'data' / 'processed' / 'parkings_rotacion_processed.csv'
RAW_UBICACION_DATA = BASE_DIR / 'data' / 'processed' / 'aparcamientos_info_processed.csv'
ACCESS_DATA = BASE_DIR / 'data' / 'access' / 'parkings_access.csv'

# Función de transformación 
def calcular_variaciones_ocupacion(rotacion, info):
    """
    Calcula las variaciones de ocupación y las combina con información de ubicación.
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
    # Encontrar la MÁXIMA variación de ocupación DENTRO de cada semana
    df_semanal = rotacion.groupby(['aparcamiento_id', 'semana_anio'])['porcentaje_ocupacion'].agg(['max', 'min'])
    df_semanal['variacion_semanal'] = df_semanal['max'] - df_semanal['min']
    df_semanal = df_semanal.reset_index()

    # 4. Agregación final para obtener la variación promedio por aparcamiento 
    df_variacion_diaria = df_diario.groupby('aparcamiento_id')['variacion_diaria'].mean().reset_index()  # Promedio diario
    df_variacion_semanal = df_semanal.groupby('aparcamiento_id')['variacion_semanal'].max().reset_index()  # **MAXIMO** semanal

    # 5. Agregación final para obtener la variación promedio por aparcamiento
    df_variacion_diaria = df_diario.groupby('aparcamiento_id')['variacion_diaria'].mean().reset_index() # Promedio diario
    df_variacion_semanal = df_semanal.groupby('aparcamiento_id')['variacion_semanal'].mean().reset_index() # Promedio semanal

    # 6. Combinar con información de ubicación (usando el archivo ext_aparcamientos_info.csv)
    df_ubicacion = info[['aparcamiento_id', 'nombre', 'latitud', 'longitud']] # Seleccionar columnas relevantes

    df_variacion_diaria = pd.merge(df_variacion_diaria, df_ubicacion, on='aparcamiento_id', how='left') # Combinar diario
    df_variacion_semanal = pd.merge(df_variacion_semanal, df_ubicacion, on='aparcamiento_id', how='left') # Combinar semanal

    df_combinado = pd.merge(df_variacion_diaria, df_variacion_semanal, on='aparcamiento_id', how='left') # Combinar diario y semanal

    return df_combinado


try: 
    # Cargar datos
    rotacion = pd.read_csv(RAW_PARKINGS_DATA)
    ubicacion = pd.read_csv(RAW_UBICACION_DATA)

    # Transformaciones
    prepared = calcular_variaciones_ocupacion(rotacion, ubicacion)
    # Filtro de columnas
    cols = ['aparcamiento_id', 'variacion_diaria', 'variacion_semanal', 'nombre_x', 'latitud_x', 'longitud_x']
    prepared = prepared[cols]

    # Guardar datos 
    os.makedirs(ACCESS_DATA.parent, exist_ok=True)
    prepared.to_csv(ACCESS_DATA, index=True)
    
except FileNotFoundError:
    print(f"Error: Archivo no encontrado.")
except Exception as e:
    print(f"Error inesperado: {str(e)}")