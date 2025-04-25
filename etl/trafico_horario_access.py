import pandas as pd

# Lectura de datos de la carpeta procesada
df = pd.read_csv('../data/processed/trafico_horario_processed.csv')

# Vistazo a la congestión del tráfico por hora
congestion_hora = df.groupby('hora')['nivel_congestion'].agg(lambda x: x.mode()[0])

# Vistazo a los tipos de coche por hora
tipos_por_hora = df.groupby('hora')[['coches', 'motos', 'camiones', 'buses']].sum()
tipos_por_hora['vehiculo_predominante'] = tipos_por_hora[['coches', 'motos', 'camiones', 'buses']].idxmax(axis=1)

# Unión de ambos vistazos por hora
df_unido = pd.merge(congestion_hora, tipos_por_hora, on='hora')

# Guardar datos en la carpeta de acceso
df_unido.to_csv('../data/access/trafico_horario_access.csv', index=True)