FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

# Instalar dependencias desde el archivo requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos datos y scripts
COPY data/ ./data
COPY etl/ ./etl

# Ejecutamos todos los scripts ETL en orden
CMD   python etl/trafico_horario_processed.py && \
    python etl/trafico_horario_access.py && \
    python etl/raw_bucket.py && \ 
    python etl/parkings_rotacion_processed.py && \
    python etl/aparcamientos_info_processed.py && \
    python etl/parkings_access.py && \
    python etl/bicimad_processed.py && \
    python etl/create_tabla_bicimad.py && \
    python etl/bicimad_access.py && \
    python etl/bbdd_postgre.py