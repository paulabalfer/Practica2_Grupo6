USE madrid_sostenible;

SELECT 
    d.nombre AS distrito,
    d.densidad_poblacion,
    COUNT(DISTINCT et.id) AS num_estaciones_transporte,
    COUNT(DISTINCT et.linea_id) AS num_lineas_distintas
FROM distritos d
LEFT JOIN estaciones_transporte et ON d.id = et.distrito_id
GROUP BY d.id, d.nombre, d.densidad_poblacion
ORDER BY d.densidad_poblacion DESC;
