USE madrid_sostenible;

USE madrid_sostenible;

SELECT 
    tipo_usuario,
    
    -- Media de CO2 evitado
    AVG(co2_evitado_gramos) AS media_co2_evitado,

    -- Media de kilómetros recorridos
    AVG(distancia_km) AS media_km_recorridos,

    -- Estación de origen más popular
    (
        SELECT estacion_origen
        FROM bicimad_usos b2
        WHERE b2.tipo_usuario = b1.tipo_usuario
        GROUP BY estacion_origen
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ) AS estacion_origen_mas_popular,

    -- Estación de destino más popular
    (
        SELECT estacion_destino
        FROM bicimad_usos b2
        WHERE b2.tipo_usuario = b1.tipo_usuario
        GROUP BY estacion_destino
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ) AS estacion_destino_mas_popular,

    -- Duración promedio del viaje (en segundos
    AVG(duracion_segundos) AS duracion_media_segunods,

    -- Media de calorías estimadas
    AVG(calorias_estimadas) AS media_calorias_estimadas,

    -- Cantidad total de viajes
    COUNT(*) AS cantidad_viajes

FROM bicimad_usos b1
GROUP BY tipo_usuario;
