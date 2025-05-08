USE madrid_sostenible;

SELECT
    estacion_origen,
    estacion_destino,
    tipo_usuario,
    COUNT(*) AS total_viajes
FROM
    bicimad_usos
GROUP BY
    estacion_origen,
    estacion_destino,
    tipo_usuario
ORDER BY
    total_viajes DESC;
    
SELECT 
    estacion_origen,
    estacion_destino,
    COUNT(*) AS total_viajes
FROM bicimad_usos
GROUP BY estacion_origen, estacion_destino
ORDER BY total_viajes DESC
LIMIT 10;

