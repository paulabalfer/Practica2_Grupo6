from pathlib import Path
import pandas as pd
import os

# Configuración de rutas
BASE_DIR = Path(__file__).parent.parent  # Directorio raíz del proyecto
DATA = BASE_DIR / 'data' / 'access' / 'trafico_horario_access.csv'

try: 
    df = pd.read_csv(DATA)

    mayor_congestion = df[df['nivel_congestion'] == 'Muy Alta']

    for _, fila in mayor_congestion.iterrows():
        hora = fila['hora']
        vehiculo = fila['vehiculo_predominante']
        print(f"A las {hora}:00 horas, predominan los {vehiculo}.")

except FileNotFoundError:
    print(f"Error: Archivo no encontrado en {DATA}")
except Exception as e:
    print(f"Error inesperado: {str(e)}")