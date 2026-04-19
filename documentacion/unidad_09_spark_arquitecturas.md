## Unidad 9: Spark en arquitecturas de datos modernas

**Tecnicatura en Datos – Procesamiento con Apache Spark (Databricks)**  
Unidad 9 de 10 — Duración estimada: 2:30 hs

---

## 1. Introducción

Las unidades anteriores enseñaron a usar Spark de forma aislada: crear DataFrames, transformarlos, escribirlos. En la práctica, Spark es **un componente dentro de una arquitectura más grande**.

Esta unidad cubre:
- El rol de Spark en pipelines ETL/ELT modernos
- Arquitecturas de Data Lake, Data Lakehouse y Data Warehouse
- Patrones arquitectónicos: Lambda y Kappa
- Orquestación con Apache Airflow
- Monitoreo, logs y buenas prácticas en producción

---

## 2. ETL vs ELT

### 2.1 Definiciones

**ETL (Extract → Transform → Load):**
1. Extraer datos de la fuente
2. Transformar en un servidor intermedio (ETL engine)
3. Cargar el resultado limpio al destino

**ELT (Extract → Load → Transform):**
1. Extraer datos de la fuente
2. Cargar los datos **crudos** directamente al destino (Data Lake/DWH)
3. Transformar dentro del destino con el poder de cómputo del mismo

### 2.2 ¿Cuándo usar cada uno?

| ETL | ELT |
|-----|-----|
| Datos sensibles (transformar antes de cargar) | Datos grandes que se benefician de cómputo distribuido |
| Destino limitado en recursos | Destino con cómputo elástico (Spark, Snowflake, BigQuery) |
| Transformaciones complejas pre-carga | Exploración analítica iterativa |

> **Spark en ELT:** Es muy común usar Spark para la fase T en un pipeline ELT. Los datos se cargan al Data Lake (S3/ADLS/GCS) y luego Spark lee y transforma.

---

## 3. Arquitecturas de almacenamiento

### 3.1 Data Lake

Un Data Lake almacena datos en su **formato crudo** (CSV, JSON, Parquet) en almacenamiento de objetos barato (S3, ADLS, GCS).

```
Fuentes → [Ingesta] → Data Lake (S3/ADLS/GCS) → [Procesamiento con Spark] → Destino
              ↑                                            ↑
        Scripts de carga                          Transformaciones ELT
```

**Ventajas:** Barato, flexible, escala infinita  
**Desventajas:** Sin transacciones ACID, sin control de schema, puede convertirse en un "pantano" si no se organiza

### 3.2 Data Warehouse

Un DWH almacena datos **estructurados y limpios** optimizados para consultas analíticas.

Ejemplos: Snowflake, Google BigQuery, Amazon Redshift, Azure Synapse

```
[Spark ETL] → Data Warehouse → [BI Tools: Power BI, Tableau, Looker]
```

**Ventajas:** Schema enforcement, SQL estándar, optimizaciones para BI  
**Desventajas:** Caro, rígido, no apto para datos semi-estructurados

### 3.3 Data Lakehouse (el modelo moderno)

Combina lo mejor de ambos mundos usando **Delta Lake** (o Apache Iceberg/Hudi):

```
Data Lake (S3/ADLS) + Delta Lake (ACID) = Data Lakehouse
```

| Característica | Data Lake | DWH | Data Lakehouse |
|----------------|-----------|-----|----------------|
| Costo almacenamiento | Muy bajo | Alto | Muy bajo |
| ACID transactions | ❌ | ✅ | ✅ |
| Schema enforcement | Parcial | ✅ | ✅ |
| Datos no estructurados | ✅ | ❌ | ✅ |
| Performance analítica | Media | Alta | Alta |

---

## 4. Patrón Lambda

La arquitectura Lambda divide el procesamiento en dos capas paralelas:

```
                    ┌─────────────────────────────┐
Fuente de datos ────┤ Speed Layer (Streaming/Spark)├──→ Serving Layer → Consultas
                    ├─────────────────────────────┤    (combinación
                    │ Batch Layer (Spark Batch)   ├──→  de resultados)
                    └─────────────────────────────┘
```

- **Batch Layer**: procesa TODO el histórico, resultados precisos pero lentos (horas de latencia)
- **Speed Layer**: procesa solo los datos recientes, resultados aproximados pero rápidos (segundos)
- **Serving Layer**: combina resultados de ambas capas para responder consultas

**Problema:** Mantener dos sistemas paralelos es complejo y costoso. El mismo código de transformación debe implementarse dos veces.

---

## 5. Patrón Kappa

La arquitectura Kappa simplifica Lambda: **todo es streaming**.

```
Fuente → [Stream Processing (Kafka + Spark Streaming)] → Destino
```

- Solo hay UNA capa de procesamiento
- Los re-procesamientos se hacen reproduciendo el stream desde el origen (Kafka retiene eventos)
- Más simple, más barato de mantener

**Cuándo usar Lambda vs Kappa:**
| Usar Lambda | Usar Kappa |
|-------------|------------|
| Se necesita reprocesamientos exactos del histórico | El histórico puede reproducirse desde el stream |
| Diferente lógica para batch y real-time | La misma lógica sirve para ambos |
| Latencia de segundos es suficiente | Se necesita latencia sub-segundo |

---

## 6. Integración de Spark con el ecosistema

### 6.1 Conexión a S3 (AWS)

```python
# En Databricks: configurar en el cluster o notebook
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get("aws", "access-key"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get("aws", "secret-key"))

df = spark.read.parquet("s3a://mi-bucket/datos/ventas/")
df.write.parquet("s3a://mi-bucket/resultados/ventas_procesadas/")
```

### 6.2 Conexión a Azure Data Lake Storage (ADLS)

```python
spark.conf.set(
    "fs.azure.account.key.micuenta.dfs.core.windows.net",
    dbutils.secrets.get("azure", "storage-key")
)

df = spark.read.parquet("abfss://contenedor@micuenta.dfs.core.windows.net/datos/")
```

### 6.3 Escribir a Snowflake desde Spark

```python
df_resultado.write \
    .format("snowflake") \
    .option("sfUrl",      "mi-cuenta.snowflakecomputing.com") \
    .option("sfDatabase", "MI_DWH") \
    .option("sfSchema",   "PUBLIC") \
    .option("dbtable",    "ventas_procesadas") \
    .option("sfUser",     dbutils.secrets.get("snow", "user")) \
    .option("sfPassword", dbutils.secrets.get("snow", "password")) \
    .mode("overwrite") \
    .save()
```

---

## 7. Orquestación con Apache Airflow

### 7.1 ¿Qué es Airflow?

Airflow es una plataforma de orquestación de workflows. Permite definir **DAGs** (Directed Acyclic Graphs) que describen tareas y sus dependencias.

```
DAG: pipeline_ventas_diario
│
├── task_01: extraer_datos_fuente  (00:00)
│       ↓
├── task_02: limpiar_datos          (00:15)
│       ↓
├── task_03: transformar_spark      (00:30)  ← llama a un job de Spark
│       ↓
├── task_04: cargar_a_dwh           (01:30)
│       ↓
└── task_05: enviar_reporte         (01:45)
```

### 7.2 DAG básico que ejecuta un job de Spark

```python
# archivo: dags/pipeline_ventas.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    "pipeline_ventas_diario",
    default_args={
        "owner":          "equipo_datos",
        "retries":        2,
        "retry_delay":    timedelta(minutes=5),
        "email_on_failure": True,
        "email":          ["datos@empresa.com"],
    },
    description="Pipeline diario de procesamiento de ventas",
    schedule_interval="0 1 * * *",   # Cada día a la 1:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Task 1: limpiar datos con Spark
limpiar = SparkSubmitOperator(
    task_id="limpiar_datos_spark",
    application="/jobs/limpiar_ventas.py",
    conn_id="spark_default",
    dag=dag,
)

# Task 2: transformar datos con Spark
transformar = SparkSubmitOperator(
    task_id="transformar_datos_spark",
    application="/jobs/transformar_ventas.py",
    conn_id="spark_default",
    dag=dag,
)

# Task 3: validación en Python
def validar_resultados(**context):
    # Verificar que el output existe y tiene filas
    pass

validar = PythonOperator(
    task_id="validar_resultados",
    python_callable=validar_resultados,
    dag=dag,
)

# Definir orden de ejecución
limpiar >> transformar >> validar
```

---

## 8. Monitoreo y observabilidad

### 8.1 Spark History Server

El History Server guarda los eventos de todos los jobs Spark ejecutados. Permite revisar:
- DAG de stages del job
- Duración de cada stage y task
- Tiempo perdido en shuffle
- Datos leídos/escritos por stage

```python
# Ver el application ID del job actual (en Databricks)
print(spark.sparkContext.applicationId)
# spark_application_1705123456789_0001

# URL del History Server (en Databricks: en el panel del cluster)
# http://driver-node:18080
```

### 8.2 Logs en Databricks

```python
import logging

# Configurar logger
log = logging.getLogger("pipeline.ventas")
log.setLevel(logging.INFO)

def procesar(df):
    log.info(f"Iniciando procesamiento. Filas de entrada: {df.count()}")
    
    df_limpio = df.dropna()
    filas_eliminadas = df.count() - df_limpio.count()
    log.warning(f"Se eliminaron {filas_eliminadas} filas nulas")
    
    log.info("Procesamiento completado")
    return df_limpio
```

### 8.3 Métricas de acumuladores

```python
# Acumuladores: contadores distribuidos para métricas
filas_procesadas  = spark.sparkContext.accumulator(0)
filas_con_errores = spark.sparkContext.accumulator(0)

from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType

@udf(StringType())
def clasificar_con_log(monto):
    filas_procesadas.add(1)
    if monto is None or monto < 0:
        filas_con_errores.add(1)
        return "error"
    elif monto < 500:
        return "bajo"
    else:
        return "alto"

df_resultado = df.withColumn("categoria", clasificar_con_log(col("monto")))
df_resultado.collect()  # Trigger para ejecutar el UDF

print(f"Filas procesadas: {filas_procesadas.value}")
print(f"Filas con errores: {filas_con_errores.value}")
```

---

## 9. Buenas prácticas en producción

### 9.1 Estructura de un pipeline productivo

```python
# jobs/procesar_ventas.py — estructura recomendada

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

log = logging.getLogger(__name__)

def extraer(spark, ruta_entrada: str):
    """Lee datos crudos desde el Data Lake."""
    log.info(f"Leyendo datos desde: {ruta_entrada}")
    return spark.read.format("delta").load(ruta_entrada)

def transformar(df):
    """Aplica transformaciones de negocio."""
    return (df
        .filter(col("monto") > 0)
        .withColumn("categoria",
            when(col("monto") < 500, "bajo")
            .when(col("monto") < 2000, "medio")
            .otherwise("alto"))
        .withColumn("procesado_en", current_timestamp())
    )

def cargar(df, ruta_salida: str):
    """Escribe resultados particionados."""
    (df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("año", "mes")
        .save(ruta_salida)
    )
    log.info(f"Datos escritos en: {ruta_salida}")

def run(ruta_entrada, ruta_salida):
    # En Databricks: spark ya disponible. Para ejecución standalone:
    spark = SparkSession.builder.appName("pipeline-ventas").getOrCreate()
    
    df_raw        = extraer(spark, ruta_entrada)
    df_procesado  = transformar(df_raw)
    cargar(df_procesado, ruta_salida)

if __name__ == "__main__":
    import sys
    run(sys.argv[1], sys.argv[2])
```

### 9.2 Checklist de producción

- [ ] **Schema explícito** en lectura (no `inferSchema=True`)
- [ ] **Checkpoint** definido para streams
- [ ] **Secrets** manejados con Databricks Secrets o Vault (no hardcodeados)
- [ ] **Particionamiento** apropiado para el volumen de datos
- [ ] **Logging** estructurado en todos los pasos clave
- [ ] **Reintentos** configurados en el orquestador
- [ ] **Monitoreo** de métricas: duración, filas procesadas, errores
- [ ] **Pruebas** con datos de muestra antes de producción
- [ ] **Documentación** del job: entradas, salidas, dependencias

---

## 10. Práctica guiada

### Ejercicio 1 — Simple: pipeline ETL básico modularizado

```python
# En Databricks
from pyspark.sql.functions import col, when, current_timestamp

# --- EXTRACT ---
def extract():
    data = [
        (1, "Ana",    1500.0, "AR", "2024-01-10"),
        (2, "Luis",   -50.0,  "MX", "2024-01-11"),  # monto negativo (error)
        (3, "Marta",  2200.0, "CL", "2024-01-12"),
        (4, "Pedro",  None,   "AR", "2024-01-13"),   # nulo (error)
        (5, "Sofía",  800.0,  "MX", "2024-01-14"),
    ]
    return spark.createDataFrame(data, ["id", "cliente", "monto", "pais", "fecha"])

# --- TRANSFORM ---
def transform(df):
    return (df
        .filter(col("monto").isNotNull() & (col("monto") > 0))
        .withColumn("categoria",
            when(col("monto") < 1000, "bajo")
            .otherwise("alto"))
        .withColumn("procesado_en", current_timestamp())
    )

# --- LOAD ---
def load(df, ruta):
    df.write.mode("overwrite").parquet(ruta)
    print(f"Escrito en {ruta}: {df.count()} filas")

# Ejecutar pipeline
df_raw        = extract()
df_procesado  = transform(df_raw)
load(df_procesado, "/tmp/ventas_pipeline/")

# Verificar
spark.read.parquet("/tmp/ventas_pipeline/").show()
```

**Resultado esperado:**
```
+---+-------+------+----+----------+---------+-------------------+
| id|cliente| monto|pais|     fecha|categoria|       procesado_en|
+---+-------+------+----+----------+---------+-------------------+
|  1|    Ana|1500.0|  AR|2024-01-10|     alto|2024-01-15 10:23:...|
|  3|  Marta|2200.0|  CL|2024-01-12|     alto|2024-01-15 10:23:...|
|  5|  Sofía| 800.0|  MX|2024-01-14|     bajo|2024-01-15 10:23:...|
+---+-------+------+----+----------+---------+-------------------+
```

### Ejercicio 2 — Avanzado: pipeline con validación y métricas

```python
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

def validar_y_separar(df: DataFrame):
    """
    Separa filas válidas de inválidas.
    Retorna: (df_valido, df_invalido)
    """
    condicion_valida = (
        col("monto").isNotNull() & 
        (col("monto") > 0) & 
        col("cliente").isNotNull() &
        col("pais").isin("AR", "MX", "CL", "BR")
    )
    df_valido   = df.filter(condicion_valida)
    df_invalido = df.filter(~condicion_valida) \
                    .withColumn("motivo_rechazo", 
                        when(col("monto").isNull(),   "monto_nulo")
                        .when(col("monto") <= 0,      "monto_negativo")
                        .when(col("cliente").isNull(), "cliente_nulo")
                        .otherwise("pais_invalido"))
    return df_valido, df_invalido

df_raw = extract()
df_valido, df_invalido = validar_y_separar(df_raw)

total    = df_raw.count()
validos  = df_valido.count()
invalidos = df_invalido.count()

print(f"=== Reporte de calidad ===")
print(f"Total filas:    {total}")
print(f"Válidas:        {validos} ({validos/total*100:.1f}%)")
print(f"Inválidas:      {invalidos} ({invalidos/total*100:.1f}%)")
print()
df_invalido.select("id", "cliente", "monto", "pais", "motivo_rechazo").show()
```

---

## 11. Preguntas de revisión

1. ¿Cuál es la diferencia entre ETL y ELT? ¿Cuándo conviene cada uno?
2. ¿Qué problema resuelve Delta Lake que un Data Lake tradicional no puede?
3. ¿Cuál es la principal desventaja de la arquitectura Lambda?
4. ¿Cuándo elegirías Kappa sobre Lambda?
5. ¿Para qué sirve Apache Airflow en un ecosistema de datos?
6. ¿Qué son los acumuladores de Spark y para qué se usan?

---

**Próxima unidad:** Proyecto Integrador — pipeline end-to-end completo
