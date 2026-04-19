# Plan de Estudio: Procesamiento de Datos con Apache Spark

**Tecnicatura en Datos | 10 Unidades | 2:30 hs por unidad**
Enfoque práctico y técnico

---

## Unidad 1 — Del origen a Spark: contexto histórico y tecnológico

### Objetivos
Comprender los problemas que dieron origen a las tecnologías de procesamiento distribuido y cómo cada solución evolucionó hacia Apache Spark.

### Contenidos

#### Google File System (GFS)
- El problema: Google necesitaba almacenar y procesar petabytes de datos en hardware commodity
- Arquitectura: nodo master + chunk servers
- Conceptos clave: replicación, tolerancia a fallos, acceso secuencial

#### Apache Hadoop
- Origen: implementación open source inspirada en GFS y MapReduce de Google
- Componentes principales: HDFS (almacenamiento) + MapReduce (procesamiento)
- HDFS: NameNode, DataNode, bloques y replicación

#### MapReduce
- Paradigma de programación distribuida: fase Map y fase Reduce
- Cómo se ejecuta un job en el clúster
- Limitaciones: escritura intermedia en disco, latencia alta, difícil de programar

#### YARN (Yet Another Resource Negotiator)
- Problema que resuelve: en Hadoop 1.x, el JobTracker era un cuello de botella
- Arquitectura: ResourceManager, NodeManager, ApplicationMaster
- YARN como gestor de recursos independiente del motor de procesamiento

#### Apache Spark
- Por qué nace Spark: superar las limitaciones de MapReduce
- Procesamiento en memoria (in-memory computing)
- Velocidad, facilidad de programación y soporte multilenguaje
- Comparativa: Spark vs MapReduce

### Práctica
- Diagrama comparativo de arquitecturas (GFS → Hadoop → Spark)
- Ejercicio de análisis: ¿por qué MapReduce falla con datos iterativos?

---

## Unidad 2 — Arquitectura de Apache Spark

### Objetivos
Comprender los componentes internos de Spark y cómo se distribuye el procesamiento en un clúster.

### Contenidos
- Driver Program y SparkContext / SparkSession
- Executors: workers del clúster
- Cluster Managers: Standalone, YARN, Kubernetes
- Modos de ejecución: local, client, cluster
- DAG (Directed Acyclic Graph): cómo Spark planifica la ejecución
- Jobs, Stages y Tasks

### Práctica
- Instalación de Spark en modo local
- Primer script PySpark: lectura de un archivo y conteo de filas
- Observar el DAG en la Spark UI

---

## Unidad 3 — RDDs: Resilient Distributed Datasets

### Objetivos
Entender el modelo de datos fundamental de Spark y cómo operar sobre colecciones distribuidas.

### Contenidos
- ¿Qué es un RDD? Inmutabilidad, tolerancia a fallos y particionamiento
- Lazy evaluation: transformaciones vs acciones
- Transformaciones principales: `map`, `filter`, `flatMap`, `distinct`, `union`
- Acciones principales: `collect`, `count`, `reduce`, `take`, `saveAsTextFile`
- Lineage y recuperación ante fallos

### Práctica
- Operaciones básicas con RDDs sobre un dataset de texto
- Implementar un WordCount con RDDs
- Analizar el plan de ejecución con `toDebugString`

---

## Unidad 4 — DataFrames y Spark SQL

### Objetivos
Trabajar con datos estructurados usando la API de alto nivel de Spark.

### Contenidos
- Del RDD al DataFrame: ventajas y diferencias
- Schema: inferido vs definido manualmente (`StructType`, `StructField`)
- Tipos de datos en Spark
- Lectura de archivos: CSV, JSON, Parquet
- Operaciones básicas: `select`, `filter`, `withColumn`, `drop`, `rename`
- Spark SQL: registrar vistas temporales y ejecutar consultas SQL

### Práctica
- Análisis exploratorio de un dataset real (ej. transacciones bancarias sintéticas)
- Consultas con DataFrame API y con SQL
- Comparar tiempos de ejecución entre RDD y DataFrame

---

## Unidad 5 — Transformaciones avanzadas

### Objetivos
Dominar operaciones complejas de transformación y agregación sobre datos distribuidos.

### Contenidos
- Agregaciones: `groupBy`, `agg`, funciones de agregación (`sum`, `avg`, `count`, `max`, `min`)
- Joins: inner, left, right, full outer, cross
- Window Functions: `rank`, `dense_rank`, `lag`, `lead`, `row_number`
- Manejo de nulos: `fillna`, `dropna`, `coalesce`
- UDFs (User Defined Functions): definición y uso responsable

### Práctica
- Pipeline de limpieza y transformación sobre datos con calidad deficiente
- Análisis con window functions (ej. ranking de clientes por monto)
- Comparar rendimiento de UDF vs funciones nativas de Spark

---

## Unidad 6 — Optimización y rendimiento

### Objetivos
Entender cómo Spark optimiza la ejecución y cómo mejorar el rendimiento de los jobs.

### Contenidos
- Catalyst Optimizer: reglas de optimización lógica y física
- Tungsten Engine: optimización de memoria y código
- `explain()`: interpretar el plan de ejecución
- Particionamiento: `repartition` vs `coalesce`
- Shuffling: cuándo ocurre y cómo minimizarlo
- Caché y persistencia: `cache()`, `persist()`, niveles de almacenamiento
- Broadcast joins para tablas pequeñas

### Práctica
- Analizar y comparar planes de ejecución con `explain(True)`
- Medir el impacto del caché en jobs iterativos
- Identificar y reducir shuffles innecesarios

---

## Unidad 7 — Lectura y escritura de datos

### Objetivos
Conectar Spark con distintas fuentes y destinos de datos del ecosistema de datos moderno.

### Contenidos
- Formatos de archivo: CSV, JSON, Parquet, ORC, Avro
- ¿Por qué Parquet? Columnar storage y compresión
- Introducción a Delta Lake: ACID en Data Lakes
- Escritura particionada: `partitionBy`
- Conexión a bases de datos relacionales via JDBC
- Opciones de escritura: `overwrite`, `append`, `errorIfExists`, `ignore`

### Práctica
- Leer datos desde una base de datos PostgreSQL o SQLite
- Escribir resultados en formato Parquet particionado por fecha
- Comparar tamaños y tiempos de lectura entre CSV y Parquet

---

## Unidad 8 — Structured Streaming

### Objetivos
Introducir el procesamiento de datos en tiempo real con Spark Structured Streaming.

### Contenidos
- Batch vs Streaming: ¿cuándo usar cada uno?
- Structured Streaming: modelo de tabla infinita
- Fuentes: socket, archivos, Kafka (mención)
- Sinks: consola, archivos, memoria
- Ventanas de tiempo: tumbling y sliding windows
- Watermarks: manejo de datos tardíos

### Práctica
- Pipeline de streaming que lee de un socket y agrega datos por ventana de tiempo
- Simular un flujo de transacciones y detectar anomalías en tiempo real

---

## Unidad 9 — Spark en arquitecturas de datos modernas

### Objetivos
Ubicar Spark dentro del ecosistema de datos empresarial y entender su rol en pipelines productivos.

### Contenidos
- Spark en pipelines ETL/ELT
- Integración con Data Lakes (S3, ADLS, GCS) y Data Warehouses (Snowflake, BigQuery)
- Patrones de arquitectura: Lambda y Kappa
- Orquestación: introducción a Apache Airflow
- Monitoreo de jobs: Spark History Server, logs y métricas
- Buenas prácticas en entornos productivos

### Práctica
- Diseño completo de un pipeline ETL: ingesta → transformación → carga
- Discusión de casos reales del sector bancario y financiero

---

## Unidad 10 — Proyecto Integrador

### Objetivos
Aplicar de forma integrada todos los conceptos del curso en un caso de uso real.

### Descripción del proyecto
Los estudiantes deberán diseñar y desarrollar un pipeline de procesamiento de datos end-to-end que incluya:

1. **Ingesta**: lectura de datos desde al menos dos fuentes (archivo + BD o streaming)
2. **Transformación**: limpieza, enriquecimiento y agregaciones con DataFrames
3. **Optimización**: justificación de decisiones técnicas (particionamiento, caché, formato de salida)
4. **Escritura**: persistencia del resultado en Parquet o Delta Lake
5. **Presentación**: exposición oral del diseño, decisiones técnicas y resultados

### Criterios de evaluación
- Correctitud del pipeline
- Calidad del código PySpark
- Justificación de las decisiones de optimización
- Claridad en la presentación

---

## Herramientas del curso

| Herramienta | Uso |
|-------------|-----|
| Apache Spark 3.x | Motor principal |
| PySpark | API Python |
| Jupyter Notebook | Desarrollo y práctica |
| Docker | Entorno reproducible |
| PostgreSQL / SQLite | Fuente de datos relacional |
| Git | Control de versiones |

---

## Bibliografía recomendada

- *Learning Spark* — Jules Damji et al. (O'Reilly)
- *Spark: The Definitive Guide* — Bill Chambers & Matei Zaharia (O'Reilly)
- Documentación oficial: [spark.apache.org](https://spark.apache.org/docs/latest/)
- Google Research Papers: *The Google File System* (2003), *MapReduce* (2004)
