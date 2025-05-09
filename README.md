# Practica2_Grupo6

### Sara Meco, Laura Martín y Paula Ballesteros. 

**Diagrama de la infraestructura**

La infraestructura de datos que hemos diseñado sigue un enfoque modular y escalable, pensado para abarcar desde el almacenamiento de datos crudos hasta el análisis visual e interpretación por usuarios finales con distintos niveles de conocimiento técnico.

**Almacenamiento inicial – Data Lake (MinIO)**:
Para gestionar grandes volúmenes de datos heterogéneos, utilizamos MinIO como Data Lake. Este sistema de almacenamiento de objetos distribuido permite alojar datos en distintas fases del proceso ETL, organizados en cuatro zonas:

- *Carpeta data/raw*: Contiene los datos en su formato original sin procesar ni transformar, tal como han sido extraídos desde las fuentes primarias (bases de datos SQL, ficheros CSV, JSON, etc.).

- *Carpeta data/processed*: Almacena los datos que han pasado por una primera transformación. Aquí los datos están ya limpios (sin nulos ni duplicados), normalizados y estandarizados, listos para su posterior carga.
Para los datos provenientes de SQL, en este paso se generan los archivos parquet correspondientes.

- *Carpeta data/access*: Contiene los datos modelados, preparados para responder a consultas concretas y ser consumidos por distintas herramientas analíticas y de visualización.

- *Carpeta data/governance*: Incluye documentación, diagramas y pasos de las transformaciones realizadas. 

A través de MinIO, se leen los datos ubicados en el bucket raw, los cuales son procesados y transformados según sea necesario. Posteriormente, estos datos se cargan en el bucket processed. Desde allí, se realizan las transformaciones finales antes de cargar los datos definitivos en el bucket access.
Además, se realiza un guardado local de los datos con el fin de facilitar tareas de recuperación ante posibles errores durante el proceso de carga.

**Procesos ETL – Scripts en Python (carpeta etl/)**:
La lógica de transformación se encuentra implementada en scripts de Python que son esenciales para el proceso de creación del Data Warehouse. Gestionan la transición de los datos en las distintas zonas definidas a través de tres etapas clave:

- *Extracción*: Lectura de los datos desde data/raw y fuentes externas.

- *Transformación*: Limpieza, estandarización, enriquecimiento y modelado (normalización, construcción de tablas de hechos y dimensiones, …).

- *Carga*: Los datos transformados son almacenados en data/process o data/access, y posteriormente cargados en el Data Warehouse.

**Almacenamiento analítico – Data Warehouse (PostgreSQL)**
Los datos organizados se cargan en PostgreSQL, importándolos desde la carpeta access de MinIO, donde se estructura un modelo dimensional basado en un esquema Galaxy (esquema en estrella extendido). Este enfoque optimiza la eficiencia de las consultas analíticas y facilita el análisis cruzado entre distintos dominios, como distritos, estaciones, usuarios, entre otros.
Automatización y orquestación – Docker Compose.
La infraestructura se despliega automáticamente a través de Docker Compose y procesa todos los datos a utilizar, lo que facilita la gestión de contenedores para MinIO, PostgreSQL y otros servicios. 

**Visualización – Apache Superset**.
Para facilitar tanto el análisis exploratorio como el acceso intuitivo a los datos, hemos integrado Apache Superset como herramienta de visualización y consulta. Superset se conecta directamente al Data Warehouse (PostgreSQL) y cumple una doble función:

- *Visualización interactiva*: Permite a usuarios sin conocimientos técnicos acceder a información relevante mediante dashboards intuitivos, con filtros, gráficos y mapas que reflejan dinámicamente los datos procesados.

- *Consultas SQL*: Para usuarios con conocimientos técnicos, como analistas o gestores municipales, Superset ofrece un entorno donde pueden escribir y ejecutar consultas SQL directamente sobre el modelo de datos.

**Modelo de datos diseñado**
Para estructurar el Data Warehouse en PostgreSQL hemos implementado un Galaxy Schema, como hemos mencionado anteriormente, que permite un análisis integrado de distintas áreas urbanas mediante múltiples tablas de hechos que comparten un conjunto común de dimensiones. 
En este contexto y para cumplir los objetivos, hemos diseñado tres tablas de hechos (dos para el objetivo 2 y una para el 3):

- *hecho_bicimad_usos (objetivo 2)*: Recoge los registros del sistema BiciMAD, incluyendo información sobre los viajes realizados: duración, estaciones de origen y destino, tipo de usuario, y fecha/hora del trayecto.

- *hecho_densidad_y_transporte (objetivo 2)*: Contiene indicadores urbanos agregados por distrito, incluyendo la densidad de población y las líneas de transporte público disponibles, permitiendo evaluar la relación entre oferta de transporte y concentración poblacional.

- *hecho_ocupacion (objetivo 3)*: Contiene los ids de los aparcamientos así como la variación diaria y semanal de cada uno. 

Y cinco tablas de dimensiones (cuatro para el objetivo 2 y una para el 3): 

- *dim_usuario (objetivo 2)*: Clasifica el tipo de usuario del sistema BiciMAD (abonado u ocasional), lo que permite segmentar los patrones de movilidad.

- *dim_estacion (objetivo 2)*: Describe las estaciones de BiciMAD (nombre y ubicación geográfica), incluyendo una relación con el distrito correspondiente para facilitar análisis espaciales.

- *dim_distrito (objetivo 2)*: Contiene el identificador y nombre de los distritos de Madrid, así como su densidad poblacional.

- *dim_tiempo (objetivo 2)*: Ofrece un desglose temporal que incluye fecha, hora de inicio y hora de fin del uso de servicios, lo que permite realizar análisis por franjas horarias, días de la semana o estacionalidad.

- *dim_aparcamiento (objetivo 3)*: Contiene toda la información relacionada con los aparcamientos identificados por su id; su nombre, dirección, capacidad, coordenadas, número de plazas especiales o precios entre otras cosas. 

Ambas tablas de hechos del objetivo 2 están conectadas con dim_distrito y dim_tiempo, lo que permite realizar análisis sincronizados por zona geográfica y periodo temporal. Además, hecho_bicimad_usos se relaciona con dim_estacion y dim_usuario, permitiendo un desglose detallado del comportamiento de los usuarios del sistema de bicicletas. Las tablas relacionadas con el objetivo 3 funcionan de manera independiente dado que tratan con datos diferentes y se relacionan entre ellas.

**Procesos de transformación implementados**. 
El pipeline de transformación de datos se basa en un enfoque ETL automatizado mediante scripts de Python, como ya hemos mencionado anteriormente. Tras la extracción desde las fuentes originales (archivos CSV, JSON y dump SQL), los datos son transformados a través de procesos que incluyen:

- *Limpieza y normalización*: estandarización de formatos, eliminación de valores nulos o duplicados, normalización, …

- *Enriquecimiento*: generación de nuevos campos como duración de trayecto, claves foráneas y agregaciones por fecha y distrito.

- *Modelado*: reestructuración de los datos para ajustarse al modelo dimensional definido (tablas de hechos y dimensiones).

- *Validación*: control sobre estructuras, claves y tipos de datos antes de la carga final.

Este proceso permite asegurar que los datos están optimizados para su análisis y visualización por los diferentes perfiles de usuario definidos en el proyecto.

**Guía de puesta en marcha.** 
Para desplegar toda la infraestructura del proyecto y cargar automáticamente los datos transformados hay que, en primer lugar, clonar el repositorio del proyecto y acceder a su directorio de trabajo; ejecutando en terminal:  

**>> git clone https://github.com/paulabalfer/Practica2_Grupo6.git**

**>> cd ruta/del/repositorio**

Tal y como se ha explicado anteriormente, el archivo docker-compose.yml está configurado para levantar todos los servicios (MinIO, PostgreSQL, etc.) y ejecutar automáticamente el procesamiento de los datos mediante los scripts definidos en el Dockerfile que establece el orden de ejecución de los archivos. 
Por ello, basta con ejecutar: 

**>> docker compose up --build**

Al haber lanzado el Docker Compose hemos levantado un servicio de Apache Superset que se apoya en nuestra base de datos PostgreSQL en la que ya están también creadas y cargadas nuestras tablas. 
Ahora bien, el usuario necesita crear la conexión entre ambos servicios y configurar las tablas para poder ejecutar las consultas. 
Vamos a ver cómo realizarlas paso a paso: 

- **Accede a la interfaz de Apache Superset en la url: https://localhost:8088** 

- **Accede con las credenciales: Usuario: admin / Contraseña: admin**

- Conecta tu base de datos siguiendo: 
**‘+’ (esquina superior derecha) > Data > Connect Database**

- Selecciona PostgreSQL e introduce tus datos de conexión según lo que definimos en el Docker Compose al lanzar el servicio: 

    **HOST: postgres**
    
    **PORT: 5432**
    
    **DATABASE NAME: postgres**
    
    **USERNAME: postgres**
    
    **PASSWORD: postgres**
    
    **DISPLAY NAME: a elección del usuario**

- Pulsa **Connect** y en la siguiente pestaña selecciona: 

    *Allow CREATE TABLE AS* 
    
    *Allow DDL and DML* 

- Pulsa **Finish**. El servicio Apache Superset está ya conectado a nuestra base de datos PostgreSQL. 

- Navega ahora hasta la **pestaña Datasets y pulsa +Dataset**. 

- Para cada una de las tablas (8 en total) selecciona: 

    Nombre de la base de datos. 
    
    Esquema **“public”** 
    
    Nombre de tu tabla 

- Pulsa **CREATE DATASET AND CREATE CHART** en la esquina inferior derecha. 

- Selecciona **“Add a dataset”** a la derecha del nombre de la tabla que acabas de añadir en la siguiente pestaña. 

Una vez completados los pasos tienes ya la infraestructura e interfaz preparadas para ejecutar las diferentes consultas que responderán a nuestras preguntas. 

**Ejemplos de uso y soporte a las consultas**.
**Objetivo 1: "¿Cuáles son los horarios de mayor congestión de tráfico en Madrid y qué tipos de vehículos predominan en esas franjas?"**
Este primer objetivo busca una infraestructura destinada a científicos de datos de empresas y organizaciones locales que analizan los datos mediante notebooks de Python; por ello basta con ejecutar desde la terminal un script de python ubicado en la carpeta notebooks, que nos devolverá por esta misma terminal la respuesta que buscamos. 
Ejecutamos entonces los comandos: 

**>> cd notebooks**  # Cambiamos el directorio de trabajo a la carpeta

**>> python trafico_horario_consulta.py**  # Ejecución del script

(NO olvides ejecutar >> cd .. para volver al directorio principal de trabajo antes de ejecutar otros comandos no relacionados con el objetivo)

Para el conjunto de datos propuesto como ejemplo se obtiene el siguiente mensaje por pantalla que nos permite obtener las conclusiones buscada.

**Objetivo 2 (2.1): "¿Qué rutas de BiciMAD son más populares entre los usuarios y cómo varían los patrones de uso entre usuarios abonados y ocasionales?"**.
Dado que el contexto de este objetivo es que gestores municipales con conocimientos avanzados de SQL puedan contestar a las preguntas, se usa la consola de SQL de Apache Superset para implementar las consultas. 
En este primer caso, accedemos a la consola pulsando la **pestaña de nuestra interfaz SQL > SQL Lab** e introducimos las consultas: 

	>> SELECT 
        estacion_origen,
        estacion_destino,
        COUNT(*) AS total_viajes
        FROM bicimad_usos
        GROUP BY estacion_origen, estacion_destino
        ORDER BY total_viajes DESC
        LIMIT 10;

De la que obtendremos las rutas más populares. Además: 

  >> SELECT 
            tipo_usuario,
            AVG(co2_evitado_gramos) AS media_co2_evitado,
            AVG(distancia_km) AS media_km_recorridos,
            (
                SELECT estacion_origen
                FROM bicimad_usos b2
                WHERE b2.tipo_usuario = b1.tipo_usuario
                GROUP BY estacion_origen
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS estacion_origen_mas_popular,
            (
                SELECT estacion_destino
                FROM bicimad_usos b2
                WHERE b2.tipo_usuario = b1.tipo_usuario
                GROUP BY estacion_destino
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS estacion_destino_mas_popular,
            AVG(duracion_segundos) AS duracion_media_segundos,
            AVG(calorias_estimadas) AS media_calorias_estimadas,
            COUNT(*) AS cantidad_viajes
        FROM bicimad_usos b1
        GROUP BY tipo_usuario;

De la que obtendremos los patrones de uso de los diferentes usuarios. 

**Objetivo 2 (2.2):  "¿Cómo se relaciona la densidad de población de los distritos con la presencia de infraestructura de transporte público?"**
De igual manera que el apartado anterior y dado que partimos del mismo contexto, de nuevo en la consola de SQL Lab ejecutamos la consulta: 

  	>> SELECT 
          d.nombre AS distrito,
          d.densidad_poblacion,
          COUNT(DISTINCT et.id) AS num_estaciones_transporte,
          COUNT(DISTINCT et.linea_id) AS num_lineas_distintas
      FROM distritos d
      LEFT JOIN estaciones_transporte et ON d.id = et.distrito_id
      GROUP BY d.id, d.nombre, d.densidad_poblacion
      ORDER BY d.densidad_poblacion DESC;
	
De la que obtendremos la respuesta a la relación entre la densidad de población y las infraestructuras de transporte presentes en cada distrito. 

**Objetivo 3: "¿Qué aparcamientos públicos presentan mayores variaciones de ocupación a lo largo del día y la semana, y cómo se correlacionan con su ubicación en la ciudad?"**
En este caso, partimos de un contexto en el que la infraestructura está destinada a ciudadanos y asociaciones vecinales sin conocimientos técnicos que buscan obtener visualizaciones para entender los datos y contestar a las preguntas. Para ello, la respuesta se obtiene mediante consultas SQL y se muestran algunas visualizaciones que permiten ver las respuestas así como el cómo conseguirlas. 
En primer lugar, para obtener los aparcamientos con mayores variaciones, ejecutamos en la consola SQL Lab la consulta: 

    >> SELECT
          	*
        FROM postgres.public.fact_ocupacion fo
        ORDER BY fo.variacion_diaria DESC, fo.variacion_semanal DESC

De la que obtenemos la tabla resultado que visualizamos como tabla con las diferentes columnas (que aparecerán ordenadas de mayor a menor variación). Para obtenerlo en la consola bastaría con darle a **CREATE CHART** una vez realizada la consulta SQL. 

En segundo y último lugar, para obtener la correlación de las variaciones con la ubicación de los aparcamientos en la ciudad, ejecutamos la consulta (de nuevo en SQL Lab): 

  	>> SELECT
            corr(oc.variacion_diaria, ap.latitud) AS correlacion_variacion_diaria_latitud,
            corr(oc.variacion_diaria, ap.longitud) AS correlacion_variacion_diaria_longitud, 
            corr(oc.variacion_semanal, ap.latitud) AS correlacion_variacion_semanal_latitud, corr(oc.variacion_semanal, ap.longitud) AS correlacion_variacion_semanal_longitud
        FROM
            postgres.public.dim_aparcamiento ap
        JOIN
            postgres.public.fact_ocupacion oc ON id_aparcamiento = oc.id_aparcamiento

De la que obtenemos la correlación de las variaciones con las columnas de latitud y longitud. De esta manera, obtenemos cómo afectan los cambios de ubicación (en latitud y longitud) a las distintas variaciones; una correlación positiva indicaría que a medida que uno de los valores crece el otro también lo hace y una negativa indicaría que a medida que uno crece el otro decrece. Si lo explicamos de manera más específica para el caso:

- Una **correlación variación-latitud positiva** significaría que a medida que nos vamos al norte la variación crece (y por consecuente a medida que vamos al sur baja); mientras que una **negativa** significaría que a medida que nos vamos al norte la variación baja (y al sur crece). 

- Una**correlación variación-longitud positiva** significaría que a medida que nos vamos al este la variación crece (y por tanto al oeste decrece); mientras que una **negativa** nos diría que si nos vamos al este la variación decrece (y al oeste crece).  
