from pathlib import Path
import pandas as pd
import os

# Configuración de rutas
BASE_DIR = Path(__file__).parent.parent  # Asume que el script está en proyecto/scripts/
RAW_PARKINGS_DATA = BASE_DIR / 'data' / 'processed' / 'parkings_rotacion_processed.csv'
RAW_UBICACION_DATA = BASE_DIR / 'data' / 'processed' / 'aparcamientos_info_processed.csv'
ACCESS_DATA = BASE_DIR / 'data' / 'access' / 'hecho_ocupacion.csv'


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


try: 
    # Cargar datos
    rotacion = pd.read_csv(RAW_PARKINGS_DATA)
    ubicacion = pd.read_csv(RAW_UBICACION_DATA)

    # Transformaciones
    prepared = calcular_variaciones_ocupacion(rotacion, ubicacion)

    # Guardar datos 
    os.makedirs(ACCESS_DATA.parent, exist_ok=True)
    prepared.to_csv(ACCESS_DATA, index=True)
    
except FileNotFoundError:
    print(f"Error: Archivo no encontrado.")
except Exception as e:
    print(f"Error inesperado: {str(e)}")