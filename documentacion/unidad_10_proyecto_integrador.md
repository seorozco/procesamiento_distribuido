## Unidad 10: Proyecto Integrador

**Tecnicatura en Datos – Procesamiento con Apache Spark (Databricks)**  
Unidad 10 de 10 — Duración estimada: 4:00 hs (entrega individual o en pares)

---

## 1. Descripción del proyecto

En esta unidad final integrás **todos los conceptos del curso** en un único pipeline de datos productivo. El proyecto simula un escenario real de una empresa de comercio electrónico que necesita procesar sus datos de ventas y construir un Data Lakehouse.

**Objetivo:** Construir un pipeline ETL completo que:
1. Ingesta datos desde dos fuentes distintas
2. Limpia y valida la calidad de los datos
3. Aplica transformaciones de negocio
4. Optimiza el almacenamiento (particionamiento, formato Parquet/Delta)
5. Expone resultados analíticos via Spark SQL
6. (Opcional avanzado) Agrega una capa de Structured Streaming

---

## 2. Dataset sugerido

### Fuente 1: Transacciones de ventas (CSV)

Campos: `transaccion_id`, `cliente_id`, `producto_id`, `monto`, `cantidad`, `pais`, `fecha_venta`, `canal`

```python
# Generar dataset de ventas (en producción: leer desde /mnt/datalake/)
from pyspark.sql.functions import rand, randn, col, when, date_add, lit, expr
import pyspark.sql.functions as F

def generar_ventas(n=100_000):
    df = (spark.range(n)
        .withColumn("transaccion_id", col("id") + 1)
        .withColumn("cliente_id",     (col("id") % 5000).cast("int"))
        .withColumn("producto_id",    (col("id") % 200).cast("int"))
        .withColumn("monto",          (rand() * 4900 + 100).cast("double"))
        .withColumn("cantidad",       (rand() * 9 + 1).cast("int"))
        .withColumn("pais",           when(col("id") % 5 == 0, "AR")
                                      .when(col("id") % 5 == 1, "MX")
                                      .when(col("id") % 5 == 2, "CL")
                                      .when(col("id") % 5 == 3, "BR")
                                      .otherwise("CO"))
        .withColumn("fecha_venta",    date_add(lit("2024-01-01").cast("date"),
                                               (col("id") % 365).cast("int")))
        .withColumn("canal",          when(col("id") % 3 == 0, "web")
                                      .when(col("id") % 3 == 1, "mobile")
                                      .otherwise("tienda"))
        # Introducir errores controlados (10% de filas)
        .withColumn("monto",          when(col("id") % 10 == 0, lit(None))
                                      .otherwise(col("monto")))
        .drop("id")
    )
    return df

df_ventas = generar_ventas()
print(f"Ventas generadas: {df_ventas.count():,} filas")
df_ventas.show(5)
```

### Fuente 2: Catálogo de productos (JSON)

Campos: `producto_id`, `nombre`, `categoria`, `subcategoria`, `precio_base`, `proveedor`

```python
def generar_productos():
    categorias = ["Electrónica", "Hogar", "Ropa", "Deportes", "Libros"]
    subcats = {
        "Electrónica": ["Celulares", "Laptops", "Audio"],
        "Hogar":       ["Cocina", "Decoración", "Limpieza"],
        "Ropa":        ["Hombre", "Mujer", "Niños"],
        "Deportes":    ["Fitness", "Outdoor", "Natación"],
        "Libros":      ["Técnicos", "Ficción", "Educación"],
    }
    
    data = []
    for prod_id in range(200):
        cat = categorias[prod_id % 5]
        sub = subcats[cat][prod_id % 3]
        data.append((
            prod_id,
            f"Producto_{prod_id:03d}",
            cat,
            sub,
            float(50 + (prod_id * 17.3) % 2000),
            f"Proveedor_{prod_id % 20:02d}",
        ))
    
    return spark.createDataFrame(data, 
        ["producto_id", "nombre", "categoria", "subcategoria", "precio_base", "proveedor"])

df_productos = generar_productos()
print(f"Productos: {df_productos.count()}")
df_productos.show(5)
```

---

## 3. Estructura del pipeline

```
┌─────────────────┐     ┌──────────────────┐
│  Fuente 1: CSV  │     │  Fuente 2: JSON  │
│  (Ventas)       │     │  (Productos)     │
└────────┬────────┘     └────────┬─────────┘
         │                        │
         ▼                        ▼
┌───────────────────────────────────────┐
│           Capa Bronze (Raw)           │
│   Datos crudos en Parquet/Delta       │
└───────────────────────────────────────┘
         │
         ▼
┌───────────────────────────────────────┐
│           Capa Silver (Limpio)        │
│   Validados, tipos correctos, sin     │
│   duplicados, sin nulos críticos      │
└───────────────────────────────────────┘
         │
         ▼
┌───────────────────────────────────────┐
│           Capa Gold (Analítico)       │
│   Enriquecido, agregado, particionado │
│   Listo para BI / SQL Analytics       │
└───────────────────────────────────────┘
```

Esta estructura se llama **Medallion Architecture** y es el estándar en Databricks / Delta Lake.

---

## 4. Implementación paso a paso

### Paso 1 — Bronze: ingesta y almacenamiento raw

```python
from pyspark.sql.functions import current_timestamp, lit

# Generar datos (en producción: spark.read.csv / spark.read.json)
df_ventas   = generar_ventas()
df_productos = generar_productos()

# Bronze: guardar tal cual, con metadata de ingesta
df_ventas_bronze = df_ventas \
    .withColumn("_ingesta_timestamp", current_timestamp()) \
    .withColumn("_fuente", lit("sistema_ventas_v2"))

df_ventas_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/tmp/proyecto/bronze/ventas/")

df_productos.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/tmp/proyecto/bronze/productos/")

print(f"Bronze ventas:    {df_ventas_bronze.count():,} filas")
print(f"Bronze productos: {df_productos.count()} filas")
```

---

### Paso 2 — Silver: limpieza y validación

```python
from pyspark.sql.functions import col, year, month, dayofmonth, round as spark_round
from pyspark.sql import DataFrame

def limpiar_ventas(df: DataFrame) -> tuple:
    """
    Limpia ventas. Retorna (df_valido, df_rechazado, metricas)
    """
    # Condición de validez
    cond_valida = (
        col("monto").isNotNull() &
        (col("monto") > 0) &
        (col("cantidad") > 0) &
        col("pais").isin("AR", "MX", "CL", "BR", "CO")
    )
    
    df_valido    = df.filter(cond_valida)
    df_rechazado = df.filter(~cond_valida)
    
    # Enriquecer válidos
    df_silver = (df_valido
        .withColumn("monto_total",   spark_round(col("monto") * col("cantidad"), 2))
        .withColumn("año",           year("fecha_venta"))
        .withColumn("mes",           month("fecha_venta"))
        .withColumn("dia",           dayofmonth("fecha_venta"))
        .drop("_ingesta_timestamp", "_fuente")
    )
    
    total     = df.count()
    validas   = df_valido.count()
    invalidas = df_rechazado.count()
    metricas  = {
        "total":    total,
        "validas":  validas,
        "invalidas": invalidas,
        "pct_calidad": round(validas / total * 100, 2),
    }
    return df_silver, df_rechazado, metricas

# Leer Bronze
df_bronze = spark.read.format("delta").load("/tmp/proyecto/bronze/ventas/")
df_silver, df_rechazado, metricas = limpiar_ventas(df_bronze)

print("=== Reporte de Calidad ===")
for k, v in metricas.items():
    print(f"  {k}: {v}")

# Guardar Silver
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("año", "mes") \
    .save("/tmp/proyecto/silver/ventas/")

# Guardar rechazados para auditoría
df_rechazado \
    .withColumn("_motivo_rechazo", 
        when(col("monto").isNull(), "monto_nulo")
        .when(col("monto") <= 0,   "monto_invalido")
        .when(col("cantidad") <= 0, "cantidad_invalida")
        .otherwise("pais_invalido")) \
    .write.format("delta").mode("overwrite") \
    .save("/tmp/proyecto/silver/ventas_rechazadas/")

print(f"\nSilver ventas: {df_silver.count():,} filas")
print(f"Rechazadas (auditoría): {df_rechazado.count():,} filas")
```

**Resultado esperado:**
```
=== Reporte de Calidad ===
  total:        100000
  validas:      90000
  invalidas:    10000
  pct_calidad:  90.0

Silver ventas: 90000 filas
Rechazadas (auditoría): 10000 filas
```

---

### Paso 3 — Gold: enriquecimiento y análisis

```python
from pyspark.sql.functions import col, sum as spark_sum, avg, count, max as spark_max
from pyspark.sql.functions import rank, desc
from pyspark.sql.window import Window

# Leer capas Silver
df_ventas_s   = spark.read.format("delta").load("/tmp/proyecto/silver/ventas/")
df_productos_b = spark.read.format("delta").load("/tmp/proyecto/bronze/productos/")

# --- JOIN: enriquecer ventas con información de productos ---
df_enriquecido = (df_ventas_s
    .join(df_productos_b, "producto_id", "left")
    .select(
        "transaccion_id", "cliente_id", "producto_id",
        "nombre", "categoria", "subcategoria",
        "monto", "cantidad", "monto_total",
        "pais", "canal", "fecha_venta", "año", "mes",
    )
)

# --- GOLD 1: resumen por país y categoría ---
df_gold_pais_cat = (df_enriquecido
    .groupBy("pais", "categoria", "año", "mes")
    .agg(
        spark_sum("monto_total").alias("revenue_total"),
        count("transaccion_id").alias("num_transacciones"),
        avg("monto").alias("ticket_promedio"),
        spark_max("monto_total").alias("transaccion_max"),
    )
    .withColumn("revenue_total",   spark_round(col("revenue_total"), 2))
    .withColumn("ticket_promedio", spark_round(col("ticket_promedio"), 2))
)

df_gold_pais_cat.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("año", "mes") \
    .save("/tmp/proyecto/gold/resumen_pais_categoria/")

# --- GOLD 2: top 10 productos por revenue ---
df_gold_top_productos = (df_enriquecido
    .groupBy("producto_id", "nombre", "categoria")
    .agg(
        spark_sum("monto_total").alias("revenue_total"),
        count("transaccion_id").alias("veces_vendido"),
    )
    .withColumn("rank_revenue",
        rank().over(Window.orderBy(desc("revenue_total"))))
    .filter(col("rank_revenue") <= 10)
    .orderBy("rank_revenue")
)

df_gold_top_productos.write \
    .format("delta").mode("overwrite") \
    .save("/tmp/proyecto/gold/top_productos/")

print("=== Top 10 Productos por Revenue ===")
df_gold_top_productos.show()
```

---

### Paso 4 — Análisis SQL sobre las capas Gold

```python
# Registrar tablas temporales
df_enriquecido.createOrReplaceTempView("ventas_enriquecidas")
df_gold_pais_cat.createOrReplaceTempView("resumen_pais_cat")

# Consulta 1: revenue por canal y país (top 5 combinaciones)
spark.sql("""
    SELECT 
        pais,
        canal,
        ROUND(SUM(monto_total), 2) AS revenue_total,
        COUNT(*)                   AS transacciones
    FROM ventas_enriquecidas
    GROUP BY pais, canal
    ORDER BY revenue_total DESC
    LIMIT 5
""").show()

# Consulta 2: evolución mensual por país con CTE
spark.sql("""
    WITH mensual AS (
        SELECT 
            pais,
            año,
            mes,
            SUM(revenue_total) AS revenue_mes
        FROM resumen_pais_cat
        GROUP BY pais, año, mes
    ),
    con_pct AS (
        SELECT
            *,
            ROUND(
                revenue_mes / SUM(revenue_mes) OVER (PARTITION BY año, mes) * 100, 
                1
            ) AS pct_mercado
        FROM mensual
    )
    SELECT * FROM con_pct
    WHERE pct_mercado > 20
    ORDER BY año, mes, pct_mercado DESC
""").show(20)

# Consulta 3: clientes con mayor gasto (top 10)
spark.sql("""
    SELECT 
        cliente_id,
        ROUND(SUM(monto_total), 2) AS gasto_total,
        COUNT(DISTINCT transaccion_id) AS num_compras,
        COUNT(DISTINCT producto_id)    AS productos_distintos,
        FIRST(pais)                    AS pais
    FROM ventas_enriquecidas
    GROUP BY cliente_id
    ORDER BY gasto_total DESC
    LIMIT 10
""").show()
```

---

### Paso 5 — Window Functions: análisis avanzado

```python
from pyspark.sql.functions import lag, col, round as spark_round
from pyspark.sql.window import Window

# Evolución del revenue mensual por país con crecimiento mes a mes
df_mensual = (df_gold_pais_cat
    .groupBy("pais", "año", "mes")
    .agg(spark_sum("revenue_total").alias("revenue_mes"))
)

w = Window.partitionBy("pais").orderBy("año", "mes")

df_con_crecimiento = (df_mensual
    .withColumn("revenue_mes_anterior", lag("revenue_mes", 1).over(w))
    .withColumn("crecimiento_pct",
        spark_round(
            (col("revenue_mes") - col("revenue_mes_anterior")) 
            / col("revenue_mes_anterior") * 100, 
            2
        )
    )
    .filter(col("revenue_mes_anterior").isNotNull())  # descartar primer mes
    .orderBy("pais", "año", "mes")
)

df_con_crecimiento.show(15)
```

---

### Paso 6 — (Opcional Avanzado) Streaming de nuevas transacciones

```python
from pyspark.sql.functions import col, when

# Simular stream de nuevas transacciones
stream_nuevas = (spark.readStream
    .format("rate")
    .option("rowsPerSecond", 50)
    .load()
    .withColumn("transaccion_id", col("value") + 1_000_000)
    .withColumn("cliente_id",     (col("value") % 5000).cast("int"))
    .withColumn("producto_id",    (col("value") % 200).cast("int"))
    .withColumn("monto",          ((col("value") * 37.7) % 5000 + 100).cast("double"))
    .withColumn("cantidad",       (col("value") % 9 + 1).cast("int"))
    .withColumn("pais",           when(col("value") % 5 == 0, "AR")
                                  .when(col("value") % 5 == 1, "MX")
                                  .otherwise("CL"))
    .withColumn("canal",          when(col("value") % 2 == 0, "web").otherwise("mobile"))
    .select("transaccion_id", "cliente_id", "producto_id", "monto", "cantidad", "pais", "canal", "timestamp")
)

def procesar_streaming_lote(df_lote, id_lote):
    df_valido = df_lote.filter(col("monto") > 0)
    df_valido.write \
        .format("delta") \
        .mode("append") \
        .save("/tmp/proyecto/silver/ventas/")
    print(f"Lote {id_lote}: {df_valido.count()} transacciones nuevas agregadas")

query_stream = (stream_nuevas.writeStream
    .foreachBatch(procesar_streaming_lote)
    .option("checkpointLocation", "/tmp/proyecto/checkpoints/stream_ventas/")
    .trigger(processingTime="10 seconds")
    .start()
)

import time
time.sleep(30)
query_stream.stop()
print("Stream detenido.")
```

---

## 5. Criterios de evaluación

### Nivel básico (aprobado)

- [ ] Ingestar las dos fuentes y almacenarlas en capa Bronze
- [ ] Implementar limpieza básica (nulos, tipos, rangos)
- [ ] Realizar el JOIN entre ventas y productos
- [ ] Generar al menos 2 agregaciones (resumen por país, top productos)
- [ ] Guardar resultados en formato Parquet o Delta

### Nivel intermedio (notable)

- [ ] Implementar la arquitectura Medallion (Bronze → Silver → Gold) completa
- [ ] Almacenar filas rechazadas con motivo de rechazo
- [ ] Usar particionamiento apropiado en escritura
- [ ] Implementar al menos 2 consultas SQL complejas (con CTE o subqueries)
- [ ] Usar Window Functions (rank, lag, lead, etc.)

### Nivel avanzado (sobresaliente)

- [ ] Agregar la capa de Structured Streaming
- [ ] Implementar manejo de errores y logging estructurado
- [ ] Documentar el pipeline (README o celdas markdown en el notebook)
- [ ] Mostrar métricas de calidad de datos (% de filas válidas/inválidas)
- [ ] Usar `explain()` para mostrar el plan de ejecución de al menos una consulta

---

## 6. Estructura de entrega

El proyecto debe entregarse como un **notebook de Databricks** (`.ipynb`) con la siguiente estructura de celdas:

```
1. [Markdown] Introducción y descripción del pipeline
2. [Código]   Imports y configuración
3. [Código]   Generación/carga de datos (Bronze)
4. [Código]   Limpieza y validación (Silver)
5. [Markdown] Reporte de calidad de datos
6. [Código]   Enriquecimiento y transformaciones (Gold)
7. [Código]   Consultas SQL analíticas
8. [Código]   (Opcional) Structured Streaming
9. [Markdown] Conclusiones y aprendizajes
```

---

## 7. Preguntas de reflexión final

1. ¿Qué formato elegiste para el almacenamiento final y por qué?
2. ¿Qué porcentaje de filas resultaron inválidas? ¿Qué tipos de errores encontraste?
3. ¿Qué particionamiento aplicaste? ¿Cómo impacta en las consultas analíticas?
4. ¿Qué transformación fue la más compleja? ¿Cómo la optimizaste?
5. ¿Cómo implementarías este pipeline en producción con Airflow?
6. Si los datos crecieran 100x, ¿qué cambiarías en el diseño?

---

## 8. Resumen del curso

| Unidad | Tema | Concepto clave |
|--------|------|----------------|
| 1 | Contexto histórico | Por qué Spark reemplazó a Hadoop MapReduce |
| 2 | Arquitectura | Driver, Executor, DAG, Lazy Evaluation |
| 3 | RDDs | Transformaciones vs Acciones, inmutabilidad |
| 4 | DataFrames y SQL | Schema, operaciones, Catalyst Optimizer |
| 5 | Transformaciones avanzadas | Joins, Window Functions, UDFs |
| 6 | Optimización | explain(), partitioning, cache, broadcast |
| 7 | Lectura/Escritura | Parquet, Delta Lake, JDBC, partitionBy |
| 8 | Structured Streaming | Fuentes, sinks, ventanas, watermarks |
| 9 | Arquitecturas | ETL/ELT, Lambda/Kappa, Airflow, monitoreo |
| 10 | Proyecto integrador | Pipeline end-to-end, Medallion Architecture |

---

**¡Felicitaciones por completar el curso de Apache Spark!**
