## Unidad 6: Optimización y rendimiento en Spark

**Tecnicatura en Datos – Procesamiento con Apache Spark (Databricks)**  
Unidad 6 de 10 — Duración estimada: 2:30 hs

---

## 1. Introducción

A partir de esta unidad entramos en uno de los temas **más importantes y más mal entendidos** de Spark: **el rendimiento**.

Muchas aplicaciones Spark:
- Funcionan correctamente
- Devuelven resultados válidos
- Pero **son lentas, caras o inestables**

La diferencia entre un/a usuario/a de Spark y un/a **Data Engineer profesional** está en **entender cómo Spark ejecuta y optimiza los jobs**.

---

## 2. Cómo optimiza Spark una consulta

Spark no ejecuta los DataFrames "tal cual" se escriben. Antes de ejecutar, aplica varias capas de optimización.

Las dos piezas clave son:
- **Catalyst Optimizer** (optimización lógica y física)
- **Tungsten Engine** (optimización de memoria y CPU)

---

## 3. Catalyst Optimizer

Catalyst es el motor que analiza y transforma las consultas.

Actúa en varias fases:

### 3.1 Plan lógico

- Representa **qué quiere hacer el usuario**
- Aún no decide *cómo* hacerlo

Ejemplo:
```text
filter → select → aggregate
```

---

### 3.2 Optimizaciones lógicas

Catalyst aplica reglas como:
- Predicate pushdown
- Eliminación de columnas no usadas
- Reordenamiento de filtros

Ejemplo:
```python
df.filter(df.monto > 1000).select("cliente", "monto")
```

El filtro puede ejecutarse **antes** del select para procesar menos datos.

---

### 3.3 Plan físico

Aquí Spark decide:
- Estrategia de join
- Número de etapas
- Uso de shuffle

Esto genera el plan **final ejecutable**.

---

## 4. Tungsten Engine

Tungsten optimiza **cómo** se ejecuta el plan físico.

Aporta:
- Gestión eficiente de memoria off-heap
- Código generado (bytecode)
- Menos objetos Java/Python

Resultado:
> Más velocidad y menor consumo de recursos

---

## 5. Analizando planes de ejecución: explain()

La herramienta principal de un Data Engineer es `explain()`. Permite ver cómo Spark **realmente** ejecuta una consulta, antes o después de que Catalyst la optimice.

---

#### Ejemplo 1 — Simple: leer el plan físico básico

Hacemos un filtro y un select, luego comparamos el plan sin y con optimización.

```python
from pyspark.sql.functions import col

data = [(i, "producto_%d" % i, float(i * 100)) for i in range(1, 11)]
df = spark.createDataFrame(data, ["id", "producto", "monto"])

operacion = df.filter(col("monto") > 500).select("producto", "monto")

# Plan físico simple (solo el último paso)
operacion.explain()
```

**Resultado esperado** (simplificado):
```
== Physical Plan ==
*(1) Project [producto#12, monto#13]
+- *(1) Filter (monto#13 > 500.0)
   +- *(1) Scan ExistingRDD
```

> El `*` indica que el paso se ejecuta con **Whole-Stage Code Generation** (Tungsten). `Filter` aparece antes que `Project` porque Catalyst aplica predicate pushdown automáticamente.

---

#### Ejemplo 2 — Medio: ver el plan completo con `explain(True)` y detectar shuffles

Hacemos un `groupBy` y observamos qué etapas genera Spark en el plan físico.

```python
from pyspark.sql.functions import sum, col

df_ventas = spark.createDataFrame([
    ("AR", 1500.0), ("MX", 800.0), ("AR", 200.0),
    ("CL", 450.0),  ("AR", 900.0), ("MX", 300.0),
], ["pais", "monto"])

resumen = df_ventas.groupBy("pais").agg(sum("monto").alias("total"))

# Plan completo: lógico + optimizado + físico
resumen.explain(True)
```

**Resultado esperado** (fragmento clave):
```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[pais#0], functions=[sum(monto#1)])
   +- Exchange hashpartitioning(pais#0, 200)   ← SHUFFLE aquí
      +- HashAggregate(keys=[pais#0], functions=[partial_sum(monto#1)])
         +- Scan ExistingRDD
```

> El `Exchange hashpartitioning` es el **shuffle**: redistribuye los datos por clave `pais` entre nodos. En el plan se ven dos `HashAggregate`: la **parcial** (por partición) y la **final** (post-shuffle). Reducir shuffles = reducir tiempo.

---

#### Ejemplo 3 — Avanzado: comparar el plan antes y después de aplicar broadcast

Observamos cómo cambia el plan físico de un join cuando forzamos broadcast en la tabla chica.

```python
from pyspark.sql.functions import broadcast

df_grande = spark.range(1, 100001).withColumnRenamed("id", "cliente_id") \
    .withColumn("monto", (col("cliente_id") * 1.5))

df_chico = spark.createDataFrame([(1, "AR"), (2, "MX"), (3, "CL")], ["cliente_id", "pais"])

# Sin broadcast: Spark elige SortMergeJoin (con shuffle en ambas tablas)
print("=== SIN broadcast ===")
df_grande.join(df_chico, "cliente_id", "inner").explain()

# Con broadcast: Spark elige BroadcastHashJoin (sin shuffle)
print("\n=== CON broadcast ===")
df_grande.join(broadcast(df_chico), "cliente_id", "inner").explain()
```

**Resultado esperado:**
```
=== SIN broadcast ===
== Physical Plan ==
SortMergeJoin [cliente_id], [cliente_id], Inner
:- Sort ... Exchange hashpartitioning(cliente_id, 200)   ← shuffle
+- Sort ... Exchange hashpartitioning(cliente_id, 200)   ← shuffle

=== CON broadcast ===
== Physical Plan ==
BroadcastHashJoin [cliente_id], [cliente_id], Inner, BuildRight
:- Range (1, 100001, ...)
+- BroadcastExchange HashedRelationBroadcastMode   ← sin shuffle
```

> Con `broadcast` desaparece el `Exchange` en la tabla grande. Esto elimina el shuffle más costoso y puede reducir el tiempo del job en un orden de magnitud.

---

## 6. Particionamiento

El particionamiento define **cómo se distribuyen los datos entre los nodos**. Un mal particionamiento es una de las causas más comunes de jobs lentos.

### 6.1 repartition vs coalesce

---

#### Ejemplo 1 — Simple: ver y ajustar el número de particiones

Creamos un DataFrame, vemos cuántas particiones tiene por defecto, y lo reducimos con `coalesce` antes de escribir.

```python
df = spark.range(1, 1000001)  # 1 millón de filas

print("Particiones originales:", df.rdd.getNumPartitions())

# Reducir sin shuffle (eficiente para escritura)
df_reducido = df.coalesce(4)
print("Particiones tras coalesce:", df_reducido.rdd.getNumPartitions())
```

**Resultado esperado:**
```
Particiones originales:  8      # varía según cores disponibles
Particiones tras coalesce: 4
```

> `coalesce` combina particiones existentes **sin shuffle**: simplemente agrupa particiones contiguas. Es la opción correcta antes de `write` para evitar generar demasiados archivos pequeños.

---

#### Ejemplo 2 — Medio: `repartition` para redistribuir datos uniformemente

Mostramos el problema de skew (desbalance) y cómo `repartition` lo corrige.

```python
from pyspark.sql.functions import spark_partition_id, count, col

# DataFrame con distribución muy desbalanceada
df_skewed = spark.createDataFrame(
    [("AR",) * 9000 + ("MX",) * 500 + ("CL",) * 500][0]
    , ["pais"]
)
# Versión correcta:
data_skewed = [("AR",)] * 9000 + [("MX",)] * 500 + [("CL",)] * 500
df_skewed = spark.createDataFrame(data_skewed, ["pais"])

# Ver distribución por partición antes de repartition
print("Antes de repartition:")
df_skewed.groupBy(spark_partition_id().alias("particion")).count().orderBy("particion").show()

# Reparticion por columna (shuffle completo)
df_balanced = df_skewed.repartition(6, "pais")
print("Después de repartition(6, 'pais'):")
df_balanced.groupBy(spark_partition_id().alias("particion")).count().orderBy("particion").show()
```

**Resultado esperado:**
```
Antes de repartition:
+---------+-----+
|particion|count|
+---------+-----+
|        0| 9000|   ← skew: partición sobrecargada
|        1|  500|
|        2|  500|
+---------+-----+

Después de repartition(6, 'pais'):
+---------+-----+
|particion|count|
+---------+-----+
|        0| 3000|
|        1| 3000|
|        2| 3000|
|        3|  167|
|        4|  167|
|        5|  166|
+---------+-----+
```

> `repartition(n, columna)` hace un shuffle usando la columna como clave de hash. Más costoso que `coalesce`, pero necesario para balancear datos antes de joins o agregaciones pesadas.

---

### 6.2 Cuándo usar cada uno

| Situación | Usar |
|---|---|
| Antes de un join grande | `repartition` (balancear datos) |
| Antes de escribir a disco | `coalesce` (menos archivos, sin shuffle) |
| Aumentar particiones | `repartition` |
| Reducir particiones | `coalesce` (si no hay skew) |

---

## 7. Shuffles

Un **shuffle** ocurre cuando Spark necesita redistribuir datos entre nodos.

Operaciones que generan shuffle:
- groupBy
- joins
- distinct
- orderBy

Los shuffles son:
- Costosos
- Sensibles al volumen de datos

Objetivo del ingeniero:
> **Reducir al mínimo los shuffles innecesarios**

---

## 8. Caché y persistencia

Spark puede almacenar DataFrames en memoria.

```python
df.cache()
df.persist()
```

### 8.1 Cuándo usar cache

- DataFrames usados múltiples veces
- Algoritmos iterativos

### 8.2 Cuándo NO usar cache

- DataFrames grandes usados una sola vez

---

## 9. Broadcast joins

Si una tabla es pequeña, se puede **enviar una copia completa a cada executor**, evitando el shuffle de la tabla grande.

---

#### Ejemplo 1 — Simple: broadcast de una tabla de códigos

Join entre una tabla grande de transacciones y una pequeña tabla de países.

```python
from pyspark.sql.functions import broadcast

df_tx = spark.range(1, 100001) \
    .withColumn("pais_id", (col("id") % 3).cast("int")) \
    .withColumn("monto",   (col("id") * 1.5))

df_paises = spark.createDataFrame([
    (0, "Argentina"),
    (1, "México"),
    (2, "Chile"),
], ["pais_id", "nombre_pais"])

# broadcast: df_paises se envía a todos los executors
resultado = df_tx.join(broadcast(df_paises), "pais_id", "inner")
resultado.show(5)
```

**Resultado esperado:**
```
+-------+-----+------+-----------+
|pais_id|   id| monto|nombre_pais|
+-------+-----+------+-----------+
|      1|    1|   1.5|     México|
|      2|    2|   3.0|      Chile|
|      0|    3|   4.5|  Argentina|
|      1|    4|   6.0|     México|
|      2|    5|   7.5|      Chile|
+-------+-----+------+-----------+
```

> Sin `broadcast`, Spark haría un `SortMergeJoin` con shuffle en ambas tablas. Con `broadcast`, la tabla de países (3 filas) viaja a cada executor y el join es local.

---

#### Ejemplo 2 — Medio: broadcast automático con `spark.sql.autoBroadcastJoinThreshold`

Spark puede aplicar broadcast automáticamente si la tabla es más chica que el umbral configurado.

```python
# Ver el umbral actual (por defecto 10 MB)
print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

# Aumentar el umbral a 50 MB para que más joins se beneficien
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))

# Join normal sin broadcast() explícito — Spark decide solo
resultado_auto = df_tx.join(df_paises, "pais_id", "inner")
resultado_auto.explain()  # debería mostrar BroadcastHashJoin
```

**Resultado esperado en el plan:**
```
== Physical Plan ==
BroadcastHashJoin [pais_id], [pais_id], Inner, BuildRight
:- Project [pais_id, id, monto]
+- BroadcastExchange HashedRelationBroadcastMode   ← automático
```

> Aumentar el umbral tiene sentido cuando se trabaja con tablas de dimensiones medianas (catálogos, códigos, configuraciones). En Databricks, Delta Lake maneja esto de forma inteligente.

---

#### Ejemplo 3 — Avanzado: comparar tiempos con y sin broadcast en un join real

Medimos el tiempo de ejecución de un join con y sin broadcast para cuantificar el impacto.

```python
import time

df_grande = spark.range(1, 5000001) \
    .withColumn("pais_id", (col("id") % 3).cast("int")) \
    .withColumn("monto",   (col("id") * 2.0))

df_dim = spark.createDataFrame([
    (0, "Argentina", "ARS"),
    (1, "México",    "MXN"),
    (2, "Chile",     "CLP"),
], ["pais_id", "nombre", "moneda"])

# Sin broadcast
t0 = time.time()
df_grande.join(df_dim, "pais_id", "inner").agg({"monto": "sum"}).collect()
t1 = time.time()
print(f"Sin broadcast: {t1 - t0:.2f}s")

# Con broadcast
t2 = time.time()
df_grande.join(broadcast(df_dim), "pais_id", "inner").agg({"monto": "sum"}).collect()
t3 = time.time()
print(f"Con broadcast: {t3 - t2:.2f}s")
print(f"Mejora: {(t1-t0)/(t3-t2):.1f}x más rápido")
```

**Resultado esperado** (valores orientativos en un clúster de 4 workers):
```
Sin broadcast: 18.43s
Con broadcast:  3.12s
Mejora: 5.9x más rápido
```

> La mejora varía según el tamaño del cluster y los datos, pero suele ser entre 3x y 10x cuando se elimina el shuffle de la tabla grande. Es una de las optimizaciones de mayor impacto en pipelines de producción.

---

## 10. Caso de uso real: optimización de pipeline

### Escenario

- Join de transacciones (grande)
- Tabla de países (chica)

### Optimización aplicada

- broadcast join
- cache del DF principal
- reducción de particiones antes de escritura

Resultado:
- Menor tiempo
- Menor costo
- Job más estable

---

## 11. Spark UI en Databricks

La Spark UI permite:
- Ver Jobs, Stages y Tasks
- Identificar shuffles
- Analizar skew de datos

Un Data Engineer **debe** saber leerla.

---

## 12. Práctica guiada

### Ejercicio 1 — Simple: ejecutar un join sin broadcast y analizar el plan

**Consigna:** Hacer un join entre una tabla de transacciones (grande) y una de países (chica), sin broadcast. Analizar el plan de ejecución e identificar el shuffle.

```python
from pyspark.sql.functions import col

df_tx = spark.range(1, 50001) \
    .withColumn("pais_id", (col("id") % 3).cast("int")) \
    .withColumn("monto",   (col("id") * 1.5))

df_paises = spark.createDataFrame([
    (0, "Argentina"),
    (1, "México"),
    (2, "Chile"),
], ["pais_id", "pais"])

# Desactivar broadcast automático para forzar SortMergeJoin
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

join_sin_broadcast = df_tx.join(df_paises, "pais_id", "inner")
join_sin_broadcast.explain()
```

**Resultado esperado en el plan:**
```
== Physical Plan ==
SortMergeJoin [pais_id], [pais_id], Inner
:- Sort ... +- Exchange hashpartitioning(pais_id, 200)   ← shuffle
+- Sort ... +- Exchange hashpartitioning(pais_id, 200)   ← shuffle
```

> Con `autoBroadcastJoinThreshold = -1` deshabilitamos el broadcast automático. Ahora podemos ver el `SortMergeJoin` con sus dos `Exchange` (shuffles).

---

### Ejercicio 2 — Avanzado: aplicar broadcast join y comparar planes y tiempos

**Consigna:** Aplicar `broadcast()` al join anterior, verificar que el plan ya no tiene shuffle, medir el tiempo y habilitar nuevamente el threshold automático.

```python
from pyspark.sql.functions import broadcast
import time

# Re-habilitar broadcast automático
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

# --- Sin broadcast ---
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
t0 = time.time()
df_tx.join(df_paises, "pais_id", "inner").count()
t1 = time.time()
print(f"Sin broadcast: {t1 - t0:.2f}s")

# --- Con broadcast ---
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # seguimos sin auto
t2 = time.time()
df_tx.join(broadcast(df_paises), "pais_id", "inner").count()
t3 = time.time()
print(f"Con broadcast: {t3 - t2:.2f}s")

# Ver nuevo plan
df_tx.join(broadcast(df_paises), "pais_id", "inner").explain()
```

**Resultado esperado:**
```
Sin broadcast: 4.31s
Con broadcast: 0.87s

== Physical Plan ==
BroadcastHashJoin [pais_id], [pais_id], Inner, BuildRight
:- Project [pais_id, id, monto]
+- BroadcastExchange HashedRelationBroadcastMode   ← sin Exchange en tabla grande
```

> La mejora de tiempo es visible incluso con 50.000 filas. En producción, con millones de registros, la diferencia puede ser de minutos.

---

## 13. Preguntas de revisión

- Qué optimiza Catalyst
- Qué problema atacan los shuffles
- Diferencia entre cache y persist
- Cuándo conviene broadcast join

---

## 14. Resumen

- Catalyst optimiza el plan
- Tungsten optimiza la ejecución
- explain() es clave
- Particionamiento correcto impacta fuerte
- Broadcast joins son aliados clave

---

**Próxima unidad**: Lectura y escritura de datos
