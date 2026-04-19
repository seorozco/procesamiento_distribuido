# Procesamiento de Datos con Apache Spark

**Tecnicatura en Datos | 10 Unidades | 2:30 hs por unidad**

Curso práctico y técnico sobre Apache Spark orientado al procesamiento distribuido de datos. Cubre desde el contexto histórico hasta la implementación de pipelines de producción, con ejemplos en PySpark en tres niveles de complejidad (simple, medio, avanzado).

---

## Estructura del repositorio

```
spark/
├── README.md
├── plan_estudio_spark.md          # Plan de estudio completo con objetivos y contenidos
├── spark_unidades_1_2.pptx        # Presentación de las unidades 1 y 2
├── documentacion/                 # Apuntes teóricos por unidad (Markdown)
│   ├── unidad_01_contexto_historico.md
│   ├── unidad_02_arquitectura_spark.md
│   ├── unidad_03_rdds.md
│   ├── unidad_04_dataframes_spark_sql.md
│   ├── unidad_05_transformaciones_avanzadas.md
│   ├── unidad_06_optimizacion_rendimiento.md
│   ├── unidad_07_lectura_escritura.md
│   ├── unidad_08_structured_streaming.md
│   ├── unidad_09_spark_arquitecturas.md
│   └── unidad_10_proyecto_integrador.md
└── notebook/                      # Notebooks Databricks-ready por unidad
    ├── unidad_01_contexto_historico.ipynb
    ├── unidad_02_arquitectura_spark.ipynb
    ├── unidad_03_rdds.ipynb
    ├── unidad_04_dataframes_spark_sql.ipynb
    ├── unidad_05_transformaciones_avanzadas.ipynb
    ├── unidad_06_optimizacion_rendimiento.ipynb
    ├── unidad_07_lectura_escritura.ipynb
    ├── unidad_08_structured_streaming.ipynb
    ├── unidad_09_spark_arquitecturas.ipynb
    └── unidad_10_proyecto_integrador.ipynb
```

---

## Unidades del curso

| # | Unidad | Temas principales |
|---|--------|-------------------|
| 1 | [Contexto histórico y tecnológico](documentacion/unidad_01_contexto_historico.md) | GFS, Hadoop, MapReduce, YARN, origen de Spark |
| 2 | [Arquitectura de Apache Spark](documentacion/unidad_02_arquitectura_spark.md) | Driver, Executors, Cluster Managers, DAG, Jobs/Stages/Tasks |
| 3 | [RDDs](documentacion/unidad_03_rdds.md) | Inmutabilidad, lazy evaluation, transformaciones y acciones, lineage |
| 4 | [DataFrames y Spark SQL](documentacion/unidad_04_dataframes_spark_sql.md) | Schema, lectura de archivos, operaciones básicas, vistas temporales |
| 5 | [Transformaciones avanzadas](documentacion/unidad_05_transformaciones_avanzadas.md) | Agregaciones, joins, window functions, nulos, UDFs |
| 6 | [Optimización y rendimiento](documentacion/unidad_06_optimizacion_rendimiento.md) | Catalyst, Tungsten, explain(), particionamiento, caché, broadcast joins |
| 7 | [Lectura y escritura de datos](documentacion/unidad_07_lectura_escritura.md) | CSV, JSON, Parquet, Delta Lake, partitionBy, JDBC, modos de escritura |
| 8 | [Structured Streaming](documentacion/unidad_08_structured_streaming.md) | Modelo tabla infinita, fuentes, sinks, ventanas de tiempo, watermarks |
| 9 | [Spark en arquitecturas modernas](documentacion/unidad_09_spark_arquitecturas.md) | ETL/ELT, Data Lakes, Lambda/Kappa, Airflow, monitoreo |
| 10 | [Proyecto Integrador](documentacion/unidad_10_proyecto_integrador.md) | Pipeline end-to-end: ingesta, transformación, optimización, escritura |

---

## Entorno de trabajo

Los notebooks están preparados para ejecutarse en **Databricks** (la variable `spark` está disponible por defecto, sin necesidad de crear una `SparkSession` manualmente). También pueden ejecutarse en modo local con PySpark instalado.

### Herramientas

| Herramienta | Uso |
|-------------|-----|
| Apache Spark 3.x | Motor principal |
| PySpark | API Python |
| Jupyter Notebook / Databricks | Desarrollo y práctica |
| PostgreSQL / SQLite | Fuente de datos relacional |
| Git | Control de versiones |

---

## Metodología de ejemplos

Cada unidad incluye ejemplos en tres niveles de complejidad:

- **Simple** — concepto aislado, código mínimo
- **Medio** — aplicación con datos más realistas y múltiples operaciones
- **Avanzado** — caso de uso cercano a producción con decisiones de diseño justificadas

Cada ejemplo incluye su **resultado esperado** para verificar la ejecución correcta.

---

## Bibliografía

- *Learning Spark* — Jules Damji et al. (O'Reilly)
- *Spark: The Definitive Guide* — Bill Chambers & Matei Zaharia (O'Reilly)
- Documentación oficial: [spark.apache.org](https://spark.apache.org/docs/latest/)
- Google Research Papers: *The Google File System* (2003), *MapReduce* (2004)
