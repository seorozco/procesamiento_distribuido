# Bibliografía y Recursos del Curso

---

## Libros recomendados

| Libro | Autores | Editorial | Notas |
|-------|---------|-----------|-------|
| *Learning Spark* (2ª ed.) | Jules Damji, Brooke Wenig, Tathagata Das, Denny Lee | O'Reilly | Mejor punto de entrada; cubre Spark 3.x y Delta Lake |
| *Spark: The Definitive Guide* | Bill Chambers & Matei Zaharia | O'Reilly | Referencia exhaustiva; ideal para profundizar |
| *Data Engineering with Python* | Paul Crickard | Packt | Contexto amplio de pipelines ETL/ELT |
| *Fundamentals of Data Engineering* | Joe Reis & Matt Housley | O'Reilly | Arquitecturas modernas: Data Lakes, Lakehouse |
| *Designing Data-Intensive Applications* | Martin Kleppmann | O'Reilly | Fundamentos de sistemas distribuidos y almacenamiento |

---

## Papers fundacionales

| Título | Año | Enlace |
|--------|-----|--------|
| *The Google File System* | 2003 | https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf |
| *MapReduce: Simplified Data Processing on Large Clusters* | 2004 | https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf |
| *Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing* | 2012 | https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf |
| *Apache Spark: A Unified Engine for Big Data Processing* | 2016 | https://dl.acm.org/doi/10.1145/2934664 |

---

## Documentación oficial

### Apache Spark
- Documentación general: https://spark.apache.org/docs/latest/
- Guía de PySpark (API): https://spark.apache.org/docs/latest/api/python/index.html
- Structured Streaming: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Spark SQL: https://spark.apache.org/docs/latest/sql-programming-guide.html
- Tuning y optimización: https://spark.apache.org/docs/latest/tuning.html
- Configuración: https://spark.apache.org/docs/latest/configuration.html

### PySpark
- Referencia de la API: https://spark.apache.org/docs/latest/api/python/reference/index.html
- DataFrame API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
- Funciones SQL (`pyspark.sql.functions`): https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
- Window Functions: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html

### Databricks
- Documentación principal: https://docs.databricks.com/
- Guía de PySpark en Databricks: https://docs.databricks.com/en/languages/python.html
- Delta Lake en Databricks: https://docs.databricks.com/en/delta/index.html
- Databricks SQL: https://docs.databricks.com/en/sql/index.html
- Unity Catalog: https://docs.databricks.com/en/data-governance/unity-catalog/index.html
- Databricks Academy (cursos gratuitos): https://www.databricks.com/learn/training/home

---

## Formatos de archivo

### Apache Parquet
- Sitio oficial: https://parquet.apache.org/
- Documentación: https://parquet.apache.org/docs/
- Especificación del formato: https://github.com/apache/parquet-format
- Parquet en PySpark: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html

### Apache Avro
- Sitio oficial: https://avro.apache.org/
- Documentación y especificación de schemas: https://avro.apache.org/docs/current/
- Avro en PySpark: https://spark.apache.org/docs/latest/sql-data-sources-avro.html
- Repositorio GitHub: https://github.com/apache/avro

### Apache Iceberg
- Sitio oficial: https://iceberg.apache.org/
- Documentación: https://iceberg.apache.org/docs/latest/
- Spark + Iceberg (quickstart): https://iceberg.apache.org/docs/latest/spark-getting-started/
- Repositorio GitHub: https://github.com/apache/iceberg
- Especificación del formato: https://iceberg.apache.org/spec/

### Delta Lake
- Sitio oficial: https://delta.io/
- Documentación: https://docs.delta.io/latest/index.html
- Delta Lake en PySpark: https://docs.delta.io/latest/quick-start.html#python
- Repositorio GitHub: https://github.com/delta-io/delta
- Delta vs Iceberg vs Hudi (comparativa): https://www.databricks.com/blog/2022/11/18/open-sourcing-unity-catalog.html

---

## Herramientas online

### Visualización y edición de JSON
| Herramienta | URL | Descripción |
|-------------|-----|-------------|
| JSON Formatter & Validator | https://jsonformatter.curiousconcept.com/ | Formatea, valida y muestra el árbol JSON |
| JSON Crack | https://jsoncrack.com/ | Visualización gráfica interactiva de JSON |
| JSON Editor Online | https://jsoneditoronline.org/ | Editor con vista árbol y texto en paralelo |
| JQ Play | https://jqplay.org/ | Prueba expresiones `jq` para procesar JSON en línea |

### Expresiones regulares (Regex)
| Herramienta | URL | Descripción |
|-------------|-----|-------------|
| Regex101 | https://regex101.com/ | El más completo; explica cada parte del patrón |
| Regexr | https://regexr.com/ | Visual e interactivo; ideal para aprender |
| Debuggex | https://www.debuggex.com/ | Muestra el diagrama de estados del regex |
| RegexLearn | https://regexlearn.com/ | Curso interactivo para aprender regex desde cero |

### Visualización de archivos Parquet y Avro
| Herramienta | URL | Descripción |
|-------------|-----|-------------|
| Tad (Tabular Data Viewer) | https://www.tadviewer.com/ | Visor de escritorio para Parquet y CSV |
| ParquetViewer (Windows) | https://github.com/mukunku/ParquetViewer | App de escritorio para Windows |
| Apache Avro Tools (CLI) | https://avro.apache.org/releases.html | Herramienta oficial para inspeccionar archivos `.avro` |

---

## Recursos complementarios

### Comunidades y blogs
- Databricks Blog (técnico): https://www.databricks.com/blog
- Apache Spark News & Releases: https://spark.apache.org/news/
- Towards Data Engineering (Medium): https://medium.com/towards-data-engineering
- The Data Engineering Podcast: https://www.dataengineeringpodcast.com/

### Cursos gratuitos
- Databricks Academy (certificaciones gratuitas): https://www.databricks.com/learn/training/home
- Apache Spark en edX (UC Berkeley): https://www.edx.org/school/uc-berkeleyx
- PySpark Tutorial (oficial, GitHub): https://github.com/apache/spark/tree/master/examples/src/main/python
