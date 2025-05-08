USE madrid_sostenible;

CREATE TABLE bicimad_usos (
    id INT PRIMARY KEY,
    usuario_id INT,
    tipo_usuario VARCHAR(20),
    estacion_origen INT,
    estacion_destino INT,
    fecha_hora_inicio TIMESTAMP,
    fecha_hora_fin TIMESTAMP,
    duracion_segundos INT,
    distancia_km FLOAT,
    calorias_estimadas INT,
    co2_evitado_gramos INT
);
