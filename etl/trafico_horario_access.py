from pathlib import Path
import pandas as pd
import os

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