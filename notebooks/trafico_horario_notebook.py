import pandas as pd

df = pd.read_csv('../data/access/trafico_horario_access.csv')

mayor_congestion = df[df['nivel_congestion'] == 'Muy Alta']

for _, fila in mayor_congestion.iterrows():
    hora = fila['hora']
    vehiculo = fila['vehiculo_predominante']
    print(f"A las {hora}:00 horas, predominan los {vehiculo}.")