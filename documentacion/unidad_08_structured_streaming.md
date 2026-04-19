## Unidad 8: Structured Streaming

**Tecnicatura en Datos – Procesamiento con Apache Spark (Databricks)**  
Unidad 8 de 10 — Duración estimada: 2:30 hs

---

## 1. Introducción

Hasta ahora procesamos **datos históricos**: archivos que ya existen, con un número finito de filas. Esto se llama procesamiento **batch** (por lotes).

En la realidad de producción, muchos sistemas generan datos **continuamente**: clicks de usuarios, transacciones bancarias en tiempo real, lecturas de sensores IoT, logs de aplicaciones. Para estos casos existe el procesamiento **streaming**.

**Structured Streaming** es el motor de streaming de Spark. Su gran ventaja sobre otros sistemas es que:
- Usa **la misma API que los DataFrames** (no hay que aprender una API nueva)
- El desarrollador describe el resultado deseado, Spark se encarga de procesar los datos incrementalmente
- Garantías de **exactly-once** (cada registro se procesa exactamente una vez)

---

## 2. Batch vs Streaming

### 2.1 Diferencias clave

| Aspecto | Batch | Streaming |
|---------|-------|-----------|
| Datos | Finitos, ya existen | Infinitos, llegan continuamente |
| Trigger | Programado (cron) | Continuo o micro-lotes |
| Latencia | Minutos a horas | Milisegundos a segundos |
| Casos de uso | Reportes diarios, ETL nocturno | Alertas, dashboards en tiempo real |
| Complejidad | Menor | Mayor (estado, late data, checkpoints) |

### 2.2 El modelo de tabla infinita (tabla unbounded)

Structured Streaming modela el stream como una **tabla que crece infinitamente**:

```
Tiempo →
┌─────────────────────────────────────────────
│ t=1: fila nueva → la tabla crece
│ t=2: fila nueva → la tabla crece
│ t=3: fila nueva → la tabla crece
└─────────────────────────────────────────────
```

Escribís consultas sobre esa tabla usando la API de DataFrames normal. Spark se encarga de ejecutarlas de forma incremental conforme llegan nuevos datos.

---

## 3. Fuentes (Sources) y Destinos (Sinks)

### 3.1 Fuentes soportadas

| Fuente | Descripción | Uso |
|--------|-------------|-----|
| `socket` | Lee de un socket TCP | Solo desarrollo/pruebas |
| `file` | Lee archivos nuevos en una carpeta | Ingesta de archivos, simple |
| `kafka` | Lee tópicos de Kafka | Producción, alta escala |
| `rate` | Genera filas a una tasa configurable | Pruebas de carga |
| `delta` | Lee cambios de una tabla Delta | Databricks, producción |

### 3.2 Destinos soportados (Sinks)

| Sink | Descripción |
|------|-------------|
| `console` | Imprime en pantalla (solo desarrollo) |
| `memory` | Guarda en tabla en memoria (solo desarrollo) |
| `parquet` / `delta` | Escribe a disco (producción) |
| `kafka` | Publica a tópicos Kafka |
| `foreach` / `foreachBatch` | Lógica personalizada (base de datos, API) |

---

## 4. Primer streaming: fuente socket

#### Ejemplo 1 — Simple: leer de socket y contar palabras en tiempo real

```python
# En una terminal aparte: nc -lk 9999 (para enviar texto)
# En Databricks: usar readStream sobre Delta o Rate en su lugar

from pyspark.sql.functions import split, explode, col

# Leer stream desde socket (solo para desarrollo local)
lineas = (spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# Transformar: word count en tiempo real
palabras = lineas.select(explode(split(col("value"), " ")).alias("palabra"))
conteo = palabras.groupBy("palabra").count()

# Escribir resultado en consola
query = (conteo.writeStream
    .outputMode("complete")   # "complete" = muestra todo el resultado acumulado
    .format("console")
    .start()
)
query.awaitTermination()
```

**Resultado esperado (en consola, actualizado con cada micro-lote):**
```
-------------------------------------------
Batch: 1
-------------------------------------------
+--------+-----+
| palabra|count|
+--------+-----+
|   spark|    2|
|   hola |    1|
+--------+-----+
```

> `outputMode("complete")` re-escribe el resultado completo en cada micro-lote. Para streams con agregación, es la única opción correcta con fuentes sin estado previo.

---

## 5. Fuente Rate: generador de datos para pruebas

#### Ejemplo 2 — Medio: stream con rate source y ventanas de tiempo

```python
from pyspark.sql.functions import window, col, current_timestamp

# Genera filas sintéticas a 10 por segundo
stream_rate = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
)

# stream_rate tiene columnas: timestamp, value
stream_rate.printSchema()
# root
#  |-- timestamp: timestamp (nullable = true)
#  |-- value: long (nullable = true)

# Agregar por ventana de 10 segundos (tumbling window)
conteo_por_ventana = (stream_rate
    .groupBy(
        window(col("timestamp"), "10 seconds")   # ventanas de 10s
    )
    .count()
    .select(
        col("window.start").alias("inicio"),
        col("window.end").alias("fin"),
        col("count")
    )
)

query = (conteo_por_ventana.writeStream
    .outputMode("update")   # solo emite ventanas actualizadas
    .format("console")
    .option("truncate", False)
    .start()
)
query.awaitTermination()
```

**Resultado esperado:**
```
-------------------------------------------
Batch: 1
-------------------------------------------
+-------------------+-------------------+-----+
|             inicio|                fin|count|
+-------------------+-------------------+-----+
|2024-01-15 10:00:00|2024-01-15 10:00:10|   10|
+-------------------+-------------------+-----+

Batch: 2
-------------------------------------------
|2024-01-15 10:00:10|2024-01-15 10:00:20|   10|
```

---

## 6. Fuente File: leer archivos nuevos

#### Ejemplo 3 — Avanzado: pipeline de ingesta de archivos nuevos con foreachBatch

```python
from pyspark.sql.functions import col, current_timestamp, lit, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema_tx = StructType([
    StructField("id",         IntegerType(), False),
    StructField("cliente_id", IntegerType(), True),
    StructField("monto",      DoubleType(),  True),
    StructField("pais",       StringType(),  True),
])

# Lee archivos CSV nuevos que aparezcan en la carpeta de entrada
stream_archivos = (spark.readStream
    .schema(schema_tx)
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)   # procesa de a 1 archivo por micro-lote
    .csv("/mnt/datalake/ingesta_entrada/")
)

def procesar_lote(df_lote, id_lote):
    """Función que procesa cada micro-lote."""
    df_enriquecido = (df_lote
        .withColumn("categoria",
            when(col("monto") < 500,  "bajo")
            .when(col("monto") < 2000, "medio")
            .otherwise("alto")
        )
        .withColumn("timestamp_proceso", current_timestamp())
        .withColumn("lote_id", lit(id_lote))
    )
    # Escribir al destino (Delta)
    df_enriquecido.write \
        .format("delta") \
        .mode("append") \
        .save("/mnt/datalake/transacciones_procesadas/")
    print(f"Lote {id_lote}: {df_lote.count()} filas procesadas")

query = (stream_archivos.writeStream
    .foreachBatch(procesar_lote)
    .option("checkpointLocation", "/mnt/datalake/checkpoints/ingesta/")
    .trigger(processingTime="30 seconds")   # ejecuta cada 30 segundos
    .start()
)
query.awaitTermination()
```

**Resultado esperado (logs):**
```
Lote 0: 1500 filas procesadas
Lote 1: 2300 filas procesadas
Lote 2: 980 filas procesadas
```

> `checkpointLocation` es **obligatorio en producción**: guarda el estado del stream para poder reiniciarlo sin reprocesar datos ya procesados.

---

## 7. Tipos de Output Mode

| Modo | Cuándo usar |
|------|-------------|
| `append` | Sin agregaciones, o con watermark. Solo emite filas nuevas |
| `complete` | Con agregaciones SIN watermark. Re-emite toda la tabla |
| `update` | Con agregaciones. Solo emite filas actualizadas |

```python
# append: adecuado para streams sin groupBy
stream.writeStream.outputMode("append")

# complete: necesario para wordcount sin watermark
stream.groupBy("palabra").count().writeStream.outputMode("complete")

# update: más eficiente que complete para tablas grandes
stream.groupBy("pais").count().writeStream.outputMode("update")
```

---

## 8. Ventanas de tiempo

### 8.1 Tumbling Windows (ventanas fijas, sin solapamiento)

```python
from pyspark.sql.functions import window, col

# Contar eventos por ventana de 5 minutos (sin solapamiento)
df_ventana = (stream
    .groupBy(window(col("timestamp"), "5 minutes"))
    .count()
)
```

```
|-----5min-----|-----5min-----|-----5min-----|
 w1             w2             w3
```

### 8.2 Sliding Windows (ventanas deslizantes, con solapamiento)

```python
# Ventana de 10 minutos que avanza cada 5 minutos (solapamiento del 50%)
df_sliding = (stream
    .groupBy(window(col("timestamp"), "10 minutes", "5 minutes"))
    .count()
)
```

```
|---10min---|
      |---10min---|
            |---10min---|
```

---

## 9. Watermarks: manejo de datos tardíos

En sistemas distribuidos, los datos **no siempre llegan en orden**. Un evento de las 10:00 puede llegar a las 10:15 por latencia de red.

Un **watermark** le dice a Spark: "espera hasta X tiempo después del evento más reciente antes de cerrar una ventana".

#### Ejemplo: watermark de 10 minutos

```python
from pyspark.sql.functions import window, col

stream_con_watermark = (stream
    .withWatermark("timestamp", "10 minutes")   # espera hasta 10min de retraso
    .groupBy(
        window(col("timestamp"), "5 minutes")
    )
    .count()
)

query = (stream_con_watermark.writeStream
    .outputMode("update")   # con watermark, update es posible
    .format("console")
    .start()
)
```

**Comportamiento:**
```
Evento llegó con timestamp 10:00 → va a ventana 09:55-10:00
Evento llegó con timestamp 09:50 (tardío, llegó a las 10:12)
  → Watermark actual: 10:12 - 10min = 10:02
  → La ventana 09:45-09:50 ya cerró (10:02 > 09:50 + 10min)
  → El evento tardío se descarta
```

---

## 10. Monitoreo de queries de streaming

```python
# Ver el estado de un query activo
query.status
# {'message': 'Processing new data', 'isDataAvailable': True, 'isTriggerActive': True}

# Ver métricas del último micro-lote
query.lastProgress
# {
#   'numInputRows': 150,
#   'inputRowsPerSecond': 10.0,
#   'processedRowsPerSecond': 245.8,
#   'durationMs': {'addBatch': 120, 'getBatch': 8, 'queryPlanning': 45, ...}
# }

# Listar todos los queries activos
spark.streams.active

# Detener un query
query.stop()
```

---

## 11. Práctica guiada

### Ejercicio 1 — Simple: simular un stream con Rate y escribir a Delta

```python
from pyspark.sql.functions import col, when

stream_sim = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumn("pais",  when(col("value") % 3 == 0, "AR")
                         .when(col("value") % 3 == 1, "MX")
                         .otherwise("CL"))
    .withColumn("monto", (col("value") * 37.5).cast("double"))
)

# Destino: tabla Delta en modo append
query = (stream_sim.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoint_ejercicio1/")
    .start("/tmp/stream_delta_ejercicio1/")
)

import time
time.sleep(15)   # dejar correr 15 segundos
query.stop()

# Ver cuántas filas se escribieron
df_resultado = spark.read.format("delta").load("/tmp/stream_delta_ejercicio1/")
print(f"Filas escritas: {df_resultado.count()}")
df_resultado.groupBy("pais").count().show()
```

**Resultado esperado:**
```
Filas escritas: 75   (5 filas/seg × 15 seg)

+----+-----+
|pais|count|
+----+-----+
|  AR|   25|
|  MX|   25|
|  CL|   25|
+----+-----+
```

### Ejercicio 2 — Avanzado: alerta en tiempo real sobre umbrales

```python
from pyspark.sql.functions import col, when, current_timestamp

stream_tx = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .load()
    .withColumn("monto", ((col("value") * 73.1) % 10000).cast("double"))
    .withColumn("cliente_id", (col("value") % 100).cast("int"))
)

# Detectar transacciones de alto riesgo (monto > $5000)
stream_alertas = (stream_tx
    .filter(col("monto") > 5000)
    .withColumn("nivel_alerta", when(col("monto") > 8000, "CRÍTICO").otherwise("ALTO"))
    .withColumn("timestamp_alerta", current_timestamp())
    .select("timestamp", "cliente_id", "monto", "nivel_alerta", "timestamp_alerta")
)

query_alertas = (stream_alertas.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

import time
time.sleep(10)
query_alertas.stop()
```

---

## 12. Preguntas de revisión

1. ¿Cuál es la diferencia entre batch y streaming?
2. ¿Qué es una tumbling window y cómo difiere de una sliding window?
3. ¿Para qué sirve el watermark en un streaming job?
4. ¿Qué es el `checkpointLocation` y por qué es obligatorio?
5. ¿En qué casos usarías `outputMode("complete")` vs `outputMode("append")`?
6. ¿Qué ventaja tiene `foreachBatch` sobre un sink estándar?

---

**Próxima unidad:** Spark en arquitecturas de datos modernas
