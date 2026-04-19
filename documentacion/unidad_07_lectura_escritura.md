## Unidad 7: Lectura y escritura de datos

**Tecnicatura en Datos – Procesamiento con Apache Spark (Databricks)**  
Unidad 7 de 10 — Duración estimada: 2:30 hs

---

## 1. Introducción

En las unidades anteriores trabajamos siempre con datos creados directamente en memoria (`createDataFrame`). En la práctica real, los datos vienen de fuentes externas: archivos en un Data Lake, bases de datos relacionales, APIs o sistemas de streaming.

Esta unidad cubre el **ciclo completo de I/O** en Spark:
- Leer datos desde distintas fuentes y formatos
- Transformar y procesar esos datos
- Escribir los resultados de forma eficiente

El formato de almacenamiento elegido tiene un **impacto directo en el rendimiento** de los jobs. Elegir mal el formato puede hacer que un job tome horas en lugar de minutos.

---

## 2. Formatos de archivo en Spark

### 2.1 Resumen de formatos soportados

| Formato | Estructura | Compresión | Lectura random | Uso recomendado |
|---------|------------|------------|----------------|-----------------|
| CSV     | Fila       | Opcional   | No             | Intercambio con sistemas externos |
| JSON    | Fila       | Opcional   | No             | APIs, datos semi-estructurados |
| Parquet | Columnar   | Sí (nativa)| Sí (por columna)| Producción, análisis |
| ORC     | Columnar   | Sí (nativa)| Sí             | Ecosistema Hive/Hadoop |
| Avro    | Fila       | Sí         | No             | Streaming, Kafka, evolución de schema |
| Delta   | Columnar   | Sí         | Sí + ACID      | Data Lakehouse (Databricks) |

---

## 3. CSV: lectura y escritura

### 3.1 Lectura de CSV

#### Ejemplo 1 — Simple: leer un CSV básico

```python
# En Databricks: spark ya está disponible
df = spark.read.csv("/mnt/datalake/ventas.csv", header=True, inferSchema=True)

df.printSchema()
df.show(5)
```

**Resultado esperado:**
```
root
 |-- id: integer (nullable = true)
 |-- cliente: string (nullable = true)
 |-- monto: double (nullable = true)
 |-- fecha: string (nullable = true)

+---+-------+------+----------+
| id|cliente| monto|     fecha|
+---+-------+------+----------+
|  1|    Ana|1500.0|2024-01-10|
|  2|   Luis| 800.0|2024-01-11|
+---+-------+------+----------+
```

> `header=True` usa la primera fila como nombres de columna. `inferSchema=True` escanea el archivo para deducir tipos (más lento, no recomendado en producción).

---

#### Ejemplo 2 — Medio: CSV con opciones avanzadas y schema explícito

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

schema = StructType([
    StructField("id",      IntegerType(), False),
    StructField("cliente", StringType(),  True),
    StructField("monto",   DoubleType(),  True),
    StructField("fecha",   DateType(),    True),
])

df = (spark.read
    .schema(schema)
    .option("header", "true")
    .option("sep", ";")                  # separador punto y coma
    .option("dateFormat", "yyyy-MM-dd")  # formato de fecha
    .option("nullValue", "N/A")          # qué interpretar como nulo
    .option("mode", "DROPMALFORMED")     # descartar filas con errores
    .csv("/mnt/datalake/ventas_raw.csv")
)
df.show(5)
```

**Resultado esperado:**
```
+---+-------+------+----------+
| id|cliente| monto|     fecha|
+---+-------+------+----------+
|  1|    Ana|1500.0|2024-01-10|
|  2|   Luis| 800.0|2024-01-11|
|  3|   null|2200.0|2024-01-12|   ← "N/A" → null
+---+-------+------+----------+
```

> Modos de lectura: `PERMISSIVE` (default, null en campos malos), `DROPMALFORMED` (descarta filas), `FAILFAST` (lanza error).

---

#### Ejemplo 3 — Avanzado: leer múltiples CSV y unirlos con metadata de origen

```python
from pyspark.sql.functions import input_file_name, regexp_extract, col

# Lee todos los CSV de la carpeta de una vez
df_todos = (spark.read
    .schema(schema)
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd")
    .csv("/mnt/datalake/ventas/año=2024/")
)

# Agregar columna con el nombre del archivo fuente
df_con_origen = df_todos.withColumn("archivo_origen", input_file_name())

# Extraer el mes del nombre del archivo (ej: ventas_2024_01.csv → 01)
df_con_mes = df_con_origen.withColumn(
    "mes",
    regexp_extract(col("archivo_origen"), r"_(\d{4})_(\d{2})\.csv", 2)
)

df_con_mes.select("id", "cliente", "monto", "mes").show(5)
```

**Resultado esperado:**
```
+---+-------+------+---+
| id|cliente| monto|mes|
+---+-------+------+---+
|  1|    Ana|1500.0| 01|
|  2|   Luis| 800.0| 01|
|  3|  Marta|2200.0| 02|
+---+-------+------+---+
```

> `input_file_name()` devuelve la ruta completa del archivo que originó cada fila. Esencial para trazabilidad en pipelines de ingesta.

---

## 4. JSON

#### Ejemplo 1 — Simple: leer JSON plano

```python
df_json = spark.read.json("/mnt/datalake/clientes.json")
df_json.printSchema()
df_json.show()
```

---

#### Ejemplo 2 — Medio: JSON anidado con acceso a campos internos

```python
from pyspark.sql.functions import col

df_pedidos = spark.read.json("/mnt/datalake/pedidos.json")

# Acceder a campos anidados con notación de punto
df_plano = df_pedidos.select(
    col("pedido_id"),
    col("cliente.nombre").alias("cliente"),
    col("cliente.pais").alias("pais"),
    col("total"),
)
df_plano.show()
```

**Resultado esperado:**
```
+---------+-------+----+-------+
|pedido_id|cliente|pais|  total|
+---------+-------+----+-------+
|        1|    Ana|  AR|1225.0|
|        2| Carlos|  MX|  80.0|
+---------+-------+----+-------+
```

---

#### Ejemplo 3 — Avanzado: explode de arrays JSON

```python
from pyspark.sql.functions import explode, col

df_raw = spark.read.json("/mnt/datalake/pedidos.json")

# Explotar el array de items: cada item se convierte en una fila separada
df_items = (df_raw
    .select("pedido_id", "cliente.nombre", explode("items").alias("item"))
    .select(
        col("pedido_id"),
        col("nombre").alias("cliente"),
        col("item.producto").alias("producto"),
        col("item.cantidad").alias("cantidad"),
        col("item.precio").alias("precio"),
    )
)
df_items.show()
```

**Resultado esperado:**
```
+---------+-------+---------+--------+-------+
|pedido_id|cliente| producto|cantidad| precio|
+---------+-------+---------+--------+-------+
|        1|    Ana|   laptop|       1|1200.0|
|        1|    Ana|    mouse|       2|  25.0|
|        2| Carlos|  teclado|       1|  80.0|
+---------+-------+---------+--------+-------+
```

> `explode()` convierte un array en múltiples filas. Cada elemento del array genera una fila nueva, replicando el resto de las columnas.

---

## 5. Parquet: el formato estrella de producción

### 5.1 ¿Por qué Parquet?

Parquet es un formato **columnar**: en lugar de almacenar los datos fila por fila, los almacena columna por columna.

```
CSV (por filas):        Parquet (por columnas):
id, nombre, monto       id: [1, 2, 3, 4]
1,  Ana,    1500        nombre: [Ana, Luis, Marta, Pedro]
2,  Luis,   800         monto: [1500, 800, 2200, 450]
3,  Marta,  2200
```

Ventajas:
- **Compresión nativa**: columnas del mismo tipo comprimen mucho mejor
- **Predicado pushdown**: si solo necesitás columna `monto`, Spark no lee `nombre`
- **Evolución de schema**: podés agregar columnas sin reescribir todo
- **Splittable**: se puede leer en paralelo sin descompresión previa

---

#### Ejemplo 1 — Simple: leer y escribir Parquet

```python
# Escribir
df.write.mode("overwrite").parquet("/mnt/datalake/ventas_parquet/")

# Leer (Spark detecta el schema automáticamente desde los metadatos)
df_parquet = spark.read.parquet("/mnt/datalake/ventas_parquet/")
df_parquet.printSchema()
df_parquet.show(5)
```

**Resultado esperado:**
```
root
 |-- id: integer (nullable = true)
 |-- cliente: string (nullable = true)
 |-- monto: double (nullable = true)
 |-- fecha: date (nullable = true)    ← los tipos se preservan exactamente
```

> A diferencia del CSV, Parquet preserva los tipos de datos en sus metadatos. No necesitás especificar schema al leer.

---

#### Ejemplo 2 — Medio: escritura particionada con `partitionBy`

```python
from pyspark.sql.functions import year, month

df_con_particion = df.withColumn("año",  year("fecha")) \
                     .withColumn("mes",  month("fecha"))

# Escribir particionado por año y mes
df_con_particion.write \
    .mode("overwrite") \
    .partitionBy("año", "mes") \
    .parquet("/mnt/datalake/ventas_particionado/")

# La estructura en disco queda:
# ventas_particionado/
#   año=2024/
#     mes=1/
#       part-00000.parquet
#     mes=2/
#       part-00000.parquet
```

**Resultado al leer con filtro de partición:**
```python
# Spark solo lee la partición necesaria (partition pruning)
df_enero = spark.read.parquet("/mnt/datalake/ventas_particionado/") \
    .filter("año = 2024 AND mes = 1")

# Spark UI mostrará: Files read: 1 (no escanea otros meses)
```

> El `partition pruning` puede reducir el I/O en un 90%+ en datasets grandes. Es la optimización más impactante en lectura de datos analíticos.

---

#### Ejemplo 3 — Avanzado: comparar tiempos CSV vs Parquet

```python
import time

# Generar datos de prueba (1 millón de filas)
from pyspark.sql.functions import rand, randn
df_test = spark.range(1_000_000) \
    .withColumn("cliente", (col("id") % 1000).cast("string")) \
    .withColumn("monto",   (rand() * 5000).cast("double")) \
    .withColumn("pais",    when(col("id") % 3 == 0, "AR")
                           .when(col("id") % 3 == 1, "MX")
                           .otherwise("CL"))

from pyspark.sql.functions import col, when

# Escribir CSV
df_test.write.mode("overwrite").option("header", "true").csv("/mnt/datalake/test_csv/")

# Escribir Parquet
df_test.write.mode("overwrite").parquet("/mnt/datalake/test_parquet/")

# Leer y medir tiempo — solo columna monto (donde Parquet brilla)
t0 = time.time()
spark.read.option("header","true").option("inferSchema","true").csv("/mnt/datalake/test_csv/") \
    .agg({"monto": "avg"}).collect()
t1 = time.time()
print(f"CSV   avg(monto): {t1-t0:.2f}s")

t2 = time.time()
spark.read.parquet("/mnt/datalake/test_parquet/") \
    .agg({"monto": "avg"}).collect()
t3 = time.time()
print(f"Parquet avg(monto): {t3-t2:.2f}s")
```

**Resultado esperado (orientativo):**
```
CSV     avg(monto): 8.43s
Parquet avg(monto): 0.91s   ← ~9x más rápido, solo lee la columna necesaria
```

---

## 6. Delta Lake

Delta Lake es una capa de almacenamiento sobre Parquet que agrega capacidades **ACID** a los Data Lakes.

### 6.1 ¿Qué agrega Delta sobre Parquet?

| Capacidad | Parquet | Delta Lake |
|-----------|---------|------------|
| ACID transactions | ❌ | ✅ |
| Time travel (versiones) | ❌ | ✅ |
| Schema enforcement | Parcial | ✅ Estricto |
| Upsert (MERGE INTO) | ❌ | ✅ |
| Lectura incremental | ❌ | ✅ |
| Vacuum (limpieza) | Manual | ✅ Automático |

---

#### Ejemplo 1 — Simple: escribir y leer Delta

```python
# En Databricks, Delta es el formato por defecto
df.write.mode("overwrite").format("delta").save("/mnt/datalake/ventas_delta/")

# Leer
df_delta = spark.read.format("delta").load("/mnt/datalake/ventas_delta/")
df_delta.show(5)

# También como tabla SQL
spark.sql("CREATE TABLE IF NOT EXISTS ventas USING DELTA LOCATION '/mnt/datalake/ventas_delta/'")
spark.sql("SELECT * FROM ventas LIMIT 5").show()
```

---

#### Ejemplo 2 — Medio: Time Travel (viajar en el tiempo)

```python
# Ver el historial de versiones
from delta.tables import DeltaTable
tabla_delta = DeltaTable.forPath(spark, "/mnt/datalake/ventas_delta/")
tabla_delta.history().show(truncate=False)

# Leer una versión anterior
df_version_0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/mnt/datalake/ventas_delta/")

# O por timestamp
df_ayer = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-15") \
    .load("/mnt/datalake/ventas_delta/")
```

---

#### Ejemplo 3 — Avanzado: UPSERT con MERGE INTO

```python
from delta.tables import DeltaTable

# Tabla destino (Delta)
tabla_destino = DeltaTable.forPath(spark, "/mnt/datalake/ventas_delta/")

# Nuevos datos (pueden ser actualizaciones o inserciones)
df_nuevos = spark.createDataFrame([
    (1, "Ana",   2000.0, "2024-02-01"),  # actualización
    (5, "Sofía", 1800.0, "2024-02-01"),  # inserción nueva
], ["id", "cliente", "monto", "fecha"])

# MERGE: si existe → actualizar; si no existe → insertar
tabla_destino.alias("destino").merge(
    df_nuevos.alias("nuevos"),
    "destino.id = nuevos.id"
).whenMatchedUpdate(set={
    "monto": "nuevos.monto",
    "fecha": "nuevos.fecha",
}).whenNotMatchedInsert(values={
    "id":      "nuevos.id",
    "cliente": "nuevos.cliente",
    "monto":   "nuevos.monto",
    "fecha":   "nuevos.fecha",
}).execute()

tabla_destino.toDF().show()
```

**Resultado esperado:**
```
+---+-------+------+----------+
| id|cliente| monto|     fecha|
+---+-------+------+----------+
|  1|    Ana|2000.0|2024-02-01|  ← actualizado
|  2|   Luis| 800.0|2024-01-11|  ← sin cambios
|  5|  Sofía|1800.0|2024-02-01|  ← nuevo
+---+-------+------+----------+
```

---

## 7. Conexión a bases de datos vía JDBC

#### Ejemplo 1 — Simple: leer desde PostgreSQL

```python
df_clientes = (spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://host:5432/mi_base")
    .option("dbtable", "clientes")
    .option("user", "usuario")
    .option("password", "password")  # En producción: usar Databricks secrets
    .option("driver", "org.postgresql.Driver")
    .load()
)
df_clientes.show()
```

---

#### Ejemplo 2 — Medio: leer con query SQL personalizada y particionamiento JDBC

```python
df_tx = (spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://host:5432/mi_base")
    .option("query", "SELECT * FROM transacciones WHERE monto > 1000")
    .option("user", "usuario")
    .option("password", dbutils.secrets.get("scope", "db-password"))
    .option("numPartitions", "8")        # leer en 8 particiones paralelas
    .option("partitionColumn", "id")     # columna numérica para partir
    .option("lowerBound", "1")
    .option("upperBound", "1000000")
    .load()
)
print(f"Particiones: {df_tx.rdd.getNumPartitions()}")
df_tx.show(5)
```

> Con `numPartitions`, Spark lanza N conexiones paralelas a la BD. El `partitionColumn` debe ser una columna numérica con distribución uniforme.

---

#### Ejemplo 3 — Avanzado: escribir resultados a PostgreSQL

```python
resumen_por_cliente = df_tx.groupBy("cliente_id") \
    .agg({"monto": "sum", "id": "count"}) \
    .withColumnRenamed("sum(monto)", "total_monto") \
    .withColumnRenamed("count(id)", "num_transacciones")

(resumen_por_cliente.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://host:5432/mi_base")
    .option("dbtable", "resumen_clientes")
    .option("user", "usuario")
    .option("password", dbutils.secrets.get("scope", "db-password"))
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")   # overwrite, append, ignore, errorIfExists
    .save()
)
print("Escritura en PostgreSQL completada")
```

---

## 8. Opciones de escritura (saveMode)

| Modo | Comportamiento |
|------|----------------|
| `overwrite` | Reemplaza todo el destino |
| `append` | Agrega al destino existente |
| `ignore` | No hace nada si el destino existe |
| `errorIfExists` | Lanza error si el destino existe (default) |

```python
# Patrón más seguro para un pipeline de carga diaria
df_resultado.write \
    .mode("append") \
    .partitionBy("año", "mes", "dia") \
    .parquet("/mnt/datalake/resultado/")
```

---

## 9. Práctica guiada

### Ejercicio 1 — Simple: leer CSV y guardar como Parquet

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("id",       IntegerType(), False),
    StructField("producto", StringType(),  True),
    StructField("monto",    DoubleType(),  True),
    StructField("pais",     StringType(),  True),
])

# Simular datos en memoria (en producción: leer desde archivo real)
data = [(i, f"prod_{i%10}", float(i*50), ["AR","MX","CL"][i%3]) for i in range(1, 101)]
df = spark.createDataFrame(data, schema)

# Guardar como Parquet
df.write.mode("overwrite").parquet("/tmp/productos_parquet/")

# Verificar la lectura
df_verificacion = spark.read.parquet("/tmp/productos_parquet/")
print(f"Filas leídas: {df_verificacion.count()}")
df_verificacion.show(5)
```

**Resultado esperado:**
```
Filas leídas: 100
+---+---------+------+----+
| id|  producto| monto|pais|
+---+---------+------+----+
|  1|   prod_1|  50.0|  MX|
|  2|   prod_2| 100.0|  CL|
...
```

### Ejercicio 2 — Avanzado: pipeline particionado con auditoría

```python
from pyspark.sql.functions import col, current_timestamp, lit

df_input = spark.read.parquet("/tmp/productos_parquet/")

df_procesado = (df_input
    .filter(col("monto") > 100)
    .withColumn("categoria", 
        when(col("monto") < 300, "bajo")
        .when(col("monto") < 600, "medio")
        .otherwise("alto"))
    .withColumn("fecha_proceso", current_timestamp())
    .withColumn("pipeline_version", lit("v1.0"))
)

# Escribir particionado por país
df_procesado.write \
    .mode("overwrite") \
    .partitionBy("pais") \
    .parquet("/tmp/productos_procesados/")

# Verificar particiones generadas
from pyspark.sql.functions import spark_partition_id
spark.read.parquet("/tmp/productos_procesados/") \
    .groupBy("pais").count().show()
```

---

## 10. Preguntas de revisión

1. ¿Por qué Parquet es más eficiente que CSV para análisis analíticos?
2. ¿Cuál es la diferencia entre `repartition` y `partitionBy` al escribir?
3. ¿Qué es el partition pruning y cómo lo aprovecha Spark?
4. ¿Qué agrega Delta Lake sobre Parquet? ¿Cuándo usarlo?
5. ¿Qué riesgo tiene usar `inferSchema=True` en producción?
6. ¿Para qué se usa `numPartitions` en la lectura JDBC?

---

**Próxima unidad:** Structured Streaming — procesamiento en tiempo real
