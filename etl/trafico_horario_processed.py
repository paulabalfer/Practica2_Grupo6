from pathlib import Path
import pandas as pd
import os

# Configuración de rutas
BASE_DIR = Path(__file__).parent.parent  # Asume que el script está en proyecto/scripts/
RAW_DATA = BASE_DIR / 'data' / 'raw' / 'trafico-horario.csv'
PROCESSED_DATA = BASE_DIR / 'data' / 'processed' / 'trafico_horario_processed.csv'

# Procesamiento
try:
    df = pd.read_csv(RAW_DATA)
    
    # Transformaciones
    df = df.drop_duplicates().dropna()
    df['fecha_hora'] = pd.to_datetime(df['fecha_hora'])
    df['fecha'] = df['fecha_hora'].dt.date
    df['hora'] = df['fecha_hora'].dt.hour
    df = df.drop(columns=['fecha_hora'])
    
    # Guardado seguro
    os.makedirs(PROCESSED_DATA.parent, exist_ok=True)
    df.to_csv(PROCESSED_DATA, index=True)
    
except FileNotFoundError:
    print(f"Error: Archivo no encontrado en {RAW_DATA}")
except Exception as e:
    print(f"Error inesperado: {str(e)}")
