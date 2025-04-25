from pathlib import Path
import pandas as pd
import os

# Configuración de rutas
BASE_DIR = Path(__file__).parent.parent  # Asume que el script está en proyecto/scripts/
RAW_DATA = BASE_DIR / 'data' / 'raw' / 'parkings-rotacion.csv'
PROCESSED_DATA = BASE_DIR / 'data' / 'processed' / 'parkings_rotacion_processed.csv'

try: 
    # Cargar datos
    rotacion = pd.read_csv(RAW_DATA)

    # Transformaciones
    rotacion = rotacion.drop_duplicates().dropna()
    rotacion['fecha'] = pd.to_datetime(rotacion['fecha'])  # Asegurar formato fecha
    rotacion['hora'] = rotacion['hora'].astype(int) # Asegurar tipo entero

    # Guardar datos 
    os.makedirs(PROCESSED_DATA.parent, exist_ok=True)
    rotacion.to_csv(PROCESSED_DATA, index=True)
    
except FileNotFoundError:
    print(f"Error: Archivo no encontrado en {RAW_DATA}")
except Exception as e:
    print(f"Error inesperado: {str(e)}")


