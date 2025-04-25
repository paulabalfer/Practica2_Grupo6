import pandas as pd

df = pd.read_csv('../data/raw/trafico-horario.csv')
# Eliminar duplicados en caso de que haya
df = df.drop_duplicates()
# Si hay nulos eliminar el registro
df_sin_nulos = df.dropna()
# Asegurar que fecha_hora está en formato datetime
df['fecha_hora'] = pd.to_datetime(df['fecha_hora'])

# Separa en columna fecha y hora
df['fecha'] = df['fecha_hora'].dt.date

df['hora'] = df['fecha_hora'].dt.hour

df = df.drop(columns=['fecha_hora'])

df.to_csv('../data/processed/trafico_horario_processed.csv', index=True)