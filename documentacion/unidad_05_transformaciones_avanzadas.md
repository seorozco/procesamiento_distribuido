## Unidad 5: Transformaciones avanzadas

**Tecnicatura en Datos – Procesamiento con Apache Spark (Databricks)**  
Unidad 5 de 10 — Duración estimada: 2:30 hs

---

## 1. Introducción

En esta unidad profundizamos en las **transformaciones avanzadas con DataFrames**, que son el núcleo del trabajo diario de un/a **Data Engineer**.

Aquí ya no se trata solo de filtrar o seleccionar columnas, sino de:
- Agregar información
- Combinar múltiples datasets
- Analizar datos en contexto (ventanas)
- Limpiar datos con problemas reales

Todo el contenido está pensado para **PySpark en Databricks** y alineado con escenarios reales de producción.

---

## 2. Agregaciones

Las agregaciones permiten **resumir grandes volúmenes de datos**.

### 2.1 groupBy y agg

---

#### Ejemplo 1 — Simple: total y promedio por categoría

Agrupamos ventas por país y calculamos el monto total y el número de operaciones.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count

spark = SparkSession.builder.appName("Agg_Demo").getOrCreate()

data = [
    ("AR", "laptop",  1200.0),
    ("AR", "mouse",     80.0),
    ("MX", "teclado",  150.0),
    ("MX", "monitor",  400.0),
    ("AR", "webcam",   120.0),
    ("CL", "laptop",  1100.0),
]
df = spark.createDataFrame(data, ["pais", "producto", "monto"])

resumen = (df
    .groupBy("pais")
    .agg(
        sum("monto").alias("monto_total"),
        avg("monto").alias("monto_promedio"),
        count("producto").alias("cant_operaciones"),
    )
    .orderBy("monto_total", ascending=False)
)
resumen.show()
```

**Resultado esperado:**
```
+----+-----------+--------------+----------------+
|pais|monto_total|monto_promedio|cant_operaciones|
+----+-----------+--------------+----------------+
|  AR|     1400.0|    466.666...|               3|
|  CL|     1100.0|        1100.0|               1|
|  MX|      550.0|         275.0|               2|
+----+-----------+--------------+----------------+
```

> Nombramos las columnas con `.alias()` para que el resultado sea legible y previsible en pipes siguientes.

---

#### Ejemplo 2 — Medio: agregación con múltiples columnas y filtro post-agg

Calculamos métricas por cliente y filtramos solo los que tienen más de una operación y monto total superior a $500.

```python
from pyspark.sql.functions import sum, count, max, min, col

data_tx = [
    (1, "Ana",   "AR", 1500.0),
    (2, "Luis",  "MX",  800.0),
    (3, "Ana",   "AR",  200.0),
    (4, "Marta", "AR", 2200.0),
    (5, "Luis",  "MX",  300.0),
    (6, "Pedro", "CL",  450.0),
]
df_tx = spark.createDataFrame(data_tx, ["id", "cliente", "pais", "monto"])

metrics = (df_tx
    .groupBy("cliente", "pais")
    .agg(
        sum("monto").alias("total"),
        count("id").alias("operaciones"),
        max("monto").alias("monto_max"),
        min("monto").alias("monto_min"),
    )
    .filter((col("operaciones") > 1) & (col("total") > 500))
    .orderBy("total", ascending=False)
)
metrics.show()
```

**Resultado esperado:**
```
+-------+----+------+-----------+---------+---------+
|cliente|pais| total|operaciones|monto_max|monto_min|
+-------+----+------+-----------+---------+---------+
|    Ana|  AR|1700.0|          2|   1500.0|    200.0|
|   Luis|  MX|1100.0|          2|    800.0|    300.0|
+-------+----+------+-----------+---------+---------+
```

> Pedro y Marta quedan excluidos: Pedro tiene una sola operación y Marta aunque supera $500, también tiene solo una.

---

#### Ejemplo 3 — Avanzado: `pivot` para convertir filas en columnas

Convertimos el resumen mensual de ventas por país en una tabla donde cada país es una columna.

```python
from pyspark.sql.functions import sum, col

data_pivot = [
    ("enero",  "AR", 3000.0),
    ("enero",  "MX", 1500.0),
    ("enero",  "CL",  800.0),
    ("febrero","AR", 4200.0),
    ("febrero","MX", 2100.0),
    ("febrero","CL", 1100.0),
]
df_pivot = spark.createDataFrame(data_pivot, ["mes", "pais", "ventas"])

tabla = (df_pivot
    .groupBy("mes")
    .pivot("pais", ["AR", "CL", "MX"])  # columnas esperadas
    .agg(sum("ventas"))
    .orderBy("mes")
)
tabla.show()
```

**Resultado esperado:**
```
+-------+------+------+------+
|    mes|    AR|    CL|    MX|
+-------+------+------+------+
|  enero|3000.0| 800.0|1500.0|
|febrero|4200.0|1100.0|2100.0|
+-------+------+------+------+
```

> `pivot` con lista explícita de valores es más eficiente que sin ella, porque Spark no necesita hacer un escaneo previo para descubrir las categorías.

---

## 3. Joins

Los joins permiten **enriquecer información** combinando datasets.

### 3.1 Tipos de joins soportados

- `inner` — solo filas con coincidencia en ambas tablas
- `left` (left_outer) — todas las filas de la izquierda
- `right` (right_outer) — todas las filas de la derecha
- `full` (full_outer) — todas las filas de ambas tablas
- `cross` — producto cartesiano (uso muy excepcional)

---

#### Ejemplo 1 — Simple: `inner join` entre transacciones y clientes

Enriquecemos las transacciones con el nombre y país de cada cliente.

```python
transacciones = spark.createDataFrame([
    (101, 1, 1500.0),
    (102, 2,  800.0),
    (103, 1, 2200.0),
    (104, 3,  450.0),
], ["tx_id", "cliente_id", "monto"])

clientes = spark.createDataFrame([
    (1, "Ana",   "AR"),
    (2, "Luis",  "MX"),
    (3, "Marta", "AR"),
], ["cliente_id", "nombre", "pais"])

resultado = transacciones.join(clientes, "cliente_id", "inner")
resultado.show()
```

**Resultado esperado:**
```
+----------+------+------+------+----+
|cliente_id| tx_id| monto|nombre|pais|
+----------+------+------+------+----+
|         1|   101|1500.0|   Ana|  AR|
|         1|   103|2200.0|   Ana|  AR|
|         2|   102| 800.0|  Luis|  MX|
|         3|   104| 450.0| Marta|  AR|
+----------+------+------+------+----+
```

> `inner` es el join por defecto. Solo aparecen los `cliente_id` que existen en **ambas** tablas.

---

#### Ejemplo 2 — Medio: `left join` para detectar registros sin coincidencia

Identificamos transacciones cuyos `cliente_id` no tienen entrada en la tabla de clientes (datos huérfanos).

```python
from pyspark.sql.functions import col

transacciones_ext = spark.createDataFrame([
    (101, 1, 1500.0),
    (102, 2,  800.0),
    (103, 99, 2200.0),  # cliente 99 no existe
    (104, 3,  450.0),
], ["tx_id", "cliente_id", "monto"])

resultado = (transacciones_ext
    .join(clientes, "cliente_id", "left")
    .select("tx_id", "cliente_id", "monto", "nombre", "pais")
)
resultado.show()

# Filtrar solo los huérfanos
huerfanos = resultado.filter(col("nombre").isNull())
print("Transacciones sin cliente:")
huerfanos.show()
```

**Resultado esperado:**
```
+------+----------+------+------+----+
| tx_id|cliente_id| monto|nombre|pais|
+------+----------+------+------+----+
|   101|         1|1500.0|   Ana|  AR|
|   102|         2| 800.0|  Luis|  MX|
|   103|        99|2200.0|  null|null|  ← huérfano
|   104|         3| 450.0| Marta|  AR|
+------+----------+------+------+----+

Transacciones sin cliente:
+------+----------+------+------+----+
| tx_id|cliente_id| monto|nombre|pais|
+------+----------+------+------+----+
|   103|        99|2200.0|  null|null|
+------+----------+------+------+----+
```

> El `left join` es la herramienta estándar para auditar integridad referencial en pipelines de datos.

---

#### Ejemplo 3 — Avanzado: join con condición compuesta y resolución de ambigüedad de columnas

Hacemos un join entre transacciones y una tabla de límites de crédito por cliente y país, luego marcamos cuáles superan su límite.

```python
from pyspark.sql.functions import col, when

limites = spark.createDataFrame([
    (1, "AR", 2000.0),
    (2, "MX", 1000.0),
    (3, "AR", 1500.0),
], ["cliente_id", "pais", "limite_credito"])

df_tx_con_pais = spark.createDataFrame([
    (101, 1, "AR", 1500.0),
    (102, 2, "MX",  800.0),
    (103, 1, "AR", 2500.0),  # supera límite
    (104, 3, "AR",  450.0),
    (105, 2, "MX", 1200.0),  # supera límite
], ["tx_id", "cliente_id", "pais", "monto"])

# Join con condición compuesta (cliente_id Y pais)
resultado = (df_tx_con_pais
    .join(limites, on=["cliente_id", "pais"], how="inner")
    .withColumn("supera_limite",
        when(col("monto") > col("limite_credito"), "SÍ").otherwise("NO")
    )
    .select("tx_id", "cliente_id", "pais", "monto", "limite_credito", "supera_limite")
    .orderBy("tx_id")
)
resultado.show()
```

**Resultado esperado:**
```
+------+----------+----+------+--------------+-------------+
| tx_id|cliente_id|pais| monto|limite_credito|supera_limite|
+------+----------+----+------+--------------+-------------+
|   101|         1|  AR|1500.0|        2000.0|           NO|
|   102|         2|  MX| 800.0|        1000.0|           NO|
|   103|         1|  AR|2500.0|        2000.0|           SÍ|
|   104|         3|  AR| 450.0|        1500.0|           NO|
|   105|         2|  MX|1200.0|        1000.0|           SÍ|
+------+----------+----+------+--------------+-------------+
```

> Pasar `on=["cliente_id", "pais"]` como lista resuelve automáticamente la ambigüedad de columnas duplicadas en el resultado.

---

### 3.2 Buenas prácticas en joins

- Filtrar **antes** del join para reducir el volumen de datos movido
- Elegir correctamente la tabla base (la más grande, a la izquierda)
- Preferir `broadcast join` cuando una tabla es pequeña (ver Unidad 6)
- En Databricks, los joins son uno de los **principales puntos de impacto en performance**

---

## 4. Window Functions

Las **window functions** permiten analizar datos **en contexto**, sin colapsar filas como en una agregación clásica.

### 4.1 Definición de ventana

Una ventana tiene tres partes:
- `partitionBy`: agrupa filas (como el GROUP BY, pero sin colapsar)
- `orderBy`: define el orden dentro del grupo
- frame: rango opcional de filas (para `lag`, `lead`, sumas acumuladas)

---

#### Ejemplo 1 — Simple: `row_number` para numerar transacciones por cliente

Asignamos un número de orden a cada transacción dentro de cada cliente, ordenadas por fecha.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

data = [
    (1, "Ana",  "2024-01-05", 1500.0),
    (2, "Ana",  "2024-01-10",  200.0),
    (3, "Luis", "2024-01-03",  800.0),
    (4, "Ana",  "2024-01-15", 2200.0),
    (5, "Luis", "2024-01-08",  300.0),
]
df = spark.createDataFrame(data, ["id", "cliente", "fecha", "monto"])

ventana = Window.partitionBy("cliente").orderBy("fecha")

df.withColumn("orden", row_number().over(ventana)).show()
```

**Resultado esperado:**
```
+---+-------+----------+------+-----+
| id|cliente|     fecha| monto|orden|
+---+-------+----------+------+-----+
|  1|    Ana|2024-01-05|1500.0|    1|
|  2|    Ana|2024-01-10| 200.0|    2|
|  4|    Ana|2024-01-15|2200.0|    3|
|  3|   Luis|2024-01-03| 800.0|    1|
|  5|   Luis|2024-01-08| 300.0|    2|
+---+-------+----------+------+-----+
```

> El numerado reinicia en 1 para cada cliente (`partitionBy`). A diferencia de `rank`, `row_number` siempre asigna números consecutivos únicos.

---

#### Ejemplo 2 — Medio: `rank` y `dense_rank` para ranking por monto

Rankeamos las transacciones de cada cliente por monto descendente y mostramos la diferencia entre `rank` y `dense_rank` cuando hay empates.

```python
from pyspark.sql.functions import rank, dense_rank

data_rank = [
    ("Ana",  1500.0),
    ("Ana",  1500.0),  # empate
    ("Ana",   200.0),
    ("Luis",  800.0),
    ("Luis",  800.0),  # empate
    ("Luis",  300.0),
]
df_r = spark.createDataFrame(data_rank, ["cliente", "monto"])

ventana_monto = Window.partitionBy("cliente").orderBy(col("monto").desc())

df_r.withColumn("rank",       rank().over(ventana_monto)) \
    .withColumn("dense_rank", dense_rank().over(ventana_monto)) \
    .show()
```

**Resultado esperado:**
```
+-------+------+----+----------+
|cliente| monto|rank|dense_rank|
+-------+------+----+----------+
|    Ana|1500.0|   1|         1|
|    Ana|1500.0|   1|         1|
|    Ana| 200.0|   3|         2|  ← rank salta a 3; dense_rank es 2
|   Luis| 800.0|   1|         1|
|   Luis| 800.0|   1|         1|
|   Luis| 300.0|   3|         2|  ← mismo comportamiento
+-------+------+----+----------+
```

> `rank` deja huecos tras un empate; `dense_rank` no. Elegir el correcto según la regla de negocio.

---

#### Ejemplo 3 — Avanzado: `lag`, `lead` y suma acumulada para análisis de tendencia

Calculamos para cada transacción el monto anterior (`lag`), el siguiente (`lead`) y el total acumulado hasta ese momento.

```python
from pyspark.sql.functions import lag, lead, sum as spark_sum, col
from pyspark.sql.window import Window

data_tend = [
    ("Ana", "2024-01-05",  500.0),
    ("Ana", "2024-01-10", 1500.0),
    ("Ana", "2024-01-15",  200.0),
    ("Ana", "2024-01-20", 2200.0),
]
df_tend = spark.createDataFrame(data_tend, ["cliente", "fecha", "monto"])

ventana_hist = Window.partitionBy("cliente").orderBy("fecha")
ventana_acum = Window.partitionBy("cliente").orderBy("fecha").rowsBetween(Window.unboundedPreceding, 0)

df_tend \
    .withColumn("monto_anterior", lag("monto", 1).over(ventana_hist)) \
    .withColumn("monto_siguiente", lead("monto", 1).over(ventana_hist)) \
    .withColumn("acumulado", spark_sum("monto").over(ventana_acum)) \
    .show()
```

**Resultado esperado:**
```
+-------+----------+------+--------------+---------------+---------+
|cliente|     fecha| monto|monto_anterior|monto_siguiente|acumulado|
+-------+----------+------+--------------+---------------+---------+
|    Ana|2024-01-05| 500.0|          null|         1500.0|    500.0|
|    Ana|2024-01-10|1500.0|         500.0|          200.0|   2000.0|
|    Ana|2024-01-15| 200.0|        1500.0|         2200.0|   2200.0|
|    Ana|2024-01-20|2200.0|         200.0|           null|   4400.0|
+-------+----------+------+--------------+---------------+---------+
```

> `lag` y `lead` devuelven `null` en los extremos (no hay fila anterior/siguiente). `rowsBetween(unboundedPreceding, 0)` define la ventana acumulada desde el inicio hasta la fila actual.

---

### 4.2 Funciones comunes

| Función | Uso |
|---|---|
| `row_number()` | Número de fila único por partición |
| `rank()` | Ranking con saltos en empates |
| `dense_rank()` | Ranking sin saltos en empates |
| `lag(col, n)` | Valor N filas atrás |
| `lead(col, n)` | Valor N filas adelante |
| `sum().over(...)` | Suma acumulada o por ventana |

---

## 5. Manejo de valores nulos

Los datos reales **siempre** tienen nulos.

### 5.1 Eliminación

```python
df.dropna()
df.dropna(subset=["monto"])
```

---

### 5.2 Reemplazo

```python
df.fillna({"monto": 0, "pais": "AR"})
```

---

### 5.3 Coalesce

```python
from pyspark.sql.functions import coalesce, lit

df.withColumn("score", coalesce("score", lit(0)))
```

---

## 6. Columnas derivadas

Crear columnas nuevas a partir de otras es una operación central.

```python
from pyspark.sql.functions import col

df.withColumn("monto_usd", col("monto") / col("tipo_cambio"))
```

---

## 7. UDFs (User Defined Functions)

Las UDFs permiten ejecutar lógica Python sobre los datos.

### 7.1 Ejemplo de UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def clasificar_monto(monto):
    if monto < 1000:
        return "bajo"
    elif monto < 5000:
        return "medio"
    else:
        return "alto"
```

---

### 7.2 Uso responsable de UDFs (MUY IMPORTANTE)

❌ Las UDFs:
- No usan Catalyst Optimizer
- Son más lentas

✅ Preferir siempre:
- Funciones nativas de Spark

UDFs solo cuando **no exista alternativa nativa**.

---

## 8. Caso de uso real (banca)

### Escenario

- Dataset de transacciones
- Dataset de clientes

### Objetivo

- Calcular monto total por cliente
- Rankear operaciones
- Manejar datos incompletos

Esto representa un **pipeline típico de capa Silver** en Databricks.

---

## 9. Práctica guiada

### Ejercicio 1 — Simple: agrupar transacciones por cliente y calcular métricas básicas

**Consigna:** Dado un dataset de transacciones, calcular para cada cliente: monto total, cantidad de operaciones y monto promedio.

```python
from pyspark.sql.functions import sum, count, avg, col

transacciones = spark.createDataFrame([
    (1, "Ana",   1500.0),
    (2, "Luis",   800.0),
    (3, "Ana",    200.0),
    (4, "Marta", 2200.0),
    (5, "Luis",   300.0),
    (6, "Ana",    900.0),
], ["id", "cliente", "monto"])

resumen = (transacciones
    .groupBy("cliente")
    .agg(
        sum("monto").alias("total"),
        count("id").alias("operaciones"),
        avg("monto").alias("promedio"),
    )
    .orderBy(col("total").desc())
)
resumen.show()
```

**Resultado esperado:**
```
+-------+------+-----------+--------+
|cliente| total|operaciones|promedio|
+-------+------+-----------+--------+
|    Ana|2600.0|          3|  866.67|
|  Marta|2200.0|          1| 2200.00|
|   Luis|1100.0|          2|  550.00|
+-------+------+-----------+--------+
```

---

### Ejercicio 2 — Medio: join de transacciones con clientes y ranking por monto

**Consigna:** Combinar la tabla de transacciones con la de clientes, calcular el monto total por cliente y asignar un ranking global por total.

```python
from pyspark.sql.functions import sum, rank, col
from pyspark.sql.window import Window

clientes = spark.createDataFrame([
    (1, "Ana",   "AR", "VIP"),
    (2, "Luis",  "MX", "regular"),
    (3, "Marta", "AR", "VIP"),
], ["cliente_id", "nombre", "pais", "segmento"])

tx = spark.createDataFrame([
    (101, 1, 1500.0),
    (102, 2,  800.0),
    (103, 1,  200.0),
    (104, 3, 2200.0),
    (105, 2,  300.0),
    (106, 1,  900.0),
], ["tx_id", "cliente_id", "monto"])

resumen_clientes = (tx
    .groupBy("cliente_id")
    .agg(sum("monto").alias("total_monto"))
    .join(clientes, "cliente_id", "inner")
    .withColumn("ranking_global",
        rank().over(Window.orderBy(col("total_monto").desc()))
    )
    .select("ranking_global", "nombre", "pais", "segmento", "total_monto")
    .orderBy("ranking_global")
)
resumen_clientes.show()
```

**Resultado esperado:**
```
+--------------+------+----+--------+-----------+
|ranking_global|nombre|pais|segmento|total_monto|
+--------------+------+----+--------+-----------+
|             1|   Ana|  AR|     VIP|     2600.0|
|             2| Marta|  AR|     VIP|     2200.0|
|             3|  Luis|  MX| regular|     1100.0|
+--------------+------+----+--------+-----------+
```

---

## 10. Preguntas de revisión

- Diferencia entre aggregation y window function
- Cuándo usar UDFs
- Impacto de los joins en performance

---

## 11. Resumen

- groupBy y agg para resumen
- joins para enriquecimiento
- window functions para análisis contextual
- manejo de nulos esencial
- UDFs: último recurso

---

**Próxima unidad**: Optimización y rendimiento en Spark
