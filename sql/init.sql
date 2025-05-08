CREATE TABLE hechos_bicimad (
    id SERIAL PRIMARY KEY,
    id_usuario INT,
    id_estacion_origen INT,
    id_estacion_destino INT,
    id_fecha INT,
    duracion INT, -- en segundos
    distancia_km NUMERIC,
    FOREIGN KEY (id_usuario) REFERENCES dim_usuario(id),
    FOREIGN KEY (id_estacion_origen) REFERENCES dim_estacion(id),
    FOREIGN KEY (id_estacion_destino) REFERENCES dim_estacion(id),
    FOREIGN KEY (id_fecha) REFERENCES dim_fecha(id)
);

CREATE TABLE dim_usuario (
    id SERIAL PRIMARY KEY,
    tipo_usuario VARCHAR(20) -- 'abonado', 'ocasional'
);

CREATE TABLE dim_estacion (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100),
    distrito VARCHAR(100),
    latitud NUMERIC,
    longitud NUMERIC
);

CREATE TABLE dim_fecha (
    id SERIAL PRIMARY KEY,
    fecha DATE,
    anio INT,
    mes INT,
    dia INT,
    dia_semana VARCHAR(10),
    es_festivo BOOLEAN
);