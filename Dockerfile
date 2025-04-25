FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

# Instalar dependencias desde el archivo requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Instalar las dependencias necesarias para trabajar con Parquet
RUN pip install pyarrow

# Instalar las dependencias necesarias para trabajar con Parquet
RUN pip install fastparquet

# Copiamos datos y scripts
COPY data/ ./data
COPY etl/ ./etl

# Instalamos dependencias
RUN pip install pandas boto3

# Ejecutamos todos los scripts ETL en orden
CMD   python etl/trafico_horario_preprocessed.py && \
    python etl/trafico_horario_access.py && \
    python etl/raw_bucket.py