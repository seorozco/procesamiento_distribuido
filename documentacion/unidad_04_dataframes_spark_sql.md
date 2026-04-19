## Unidad 4: DataFrames y Spark SQL

**Tecnicatura en Datos – Procesamiento con Apache Spark**  
Unidad 4 de 10 — Duración estimada: 2:30 hs

---

## 1. Introducción

En esta unidad damos un salto de abstracción clave: pasamos de trabajar con **RDDs (bajo nivel)** a trabajar con **DataFrames y Spark SQL**, la API moderna y recomendada de Apache Spark.

A partir de aquí, Spark empieza a parecerse más a:
- Una base de datos analítica
- Un motor SQL distribuido
- Un sistema capaz de optimizar automáticamente las consultas

Esta unidad es **fundamental**: la gran mayoría del código Spark en entornos productivos se escribe con DataFrames.

---

## 2. ¿Qué es un DataFrame?

Un **DataFrame** es una colección distribuida de datos **estructurados**, organizados en **filas y columnas**, similar a una tabla.

Conceptualmente:
- Un RDD = colección de objetos
- Un DataFrame = tabla con schema

Características principales:
- Usa un **schema explícito**
- Permite optimización automática (Catalyst)
- Integra SQL de forma nativa

---

## 3. Del RDD al DataFrame

Spark internamente sigue usando RDDs, pero **el usuario no interactúa con ellos directamente**.

Ventajas del DataFrame sobre RDD:
- Menos código
- Menos errores
- Mejor performance
- Optimización automática

Ejemplo comparativo:

```python
# RDD
rdd.filter(lambda x: x.monto > 1000)

# DataFrame
df.filter(df.monto > 1000)
```

---

## 4. El Schema en Spark

El **schema** define la estructura del DataFrame:
- Nombre de columnas
- Tipo de dato
- Si permite valores nulos

Spark puede:
- Inferir el schema
- Recibirlo explícitamente

---

### 4.1 Inferencia de schema

---

#### Ejemplo 1 — Simple: inferencia automática desde CSV

Spark lee el archivo y deduce los tipos por su cuenta. Útil para explorar datos desconocidos rápidamente.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Schema_Demo").getOrCreate()

df = spark.read.csv("/data/ventas.csv", header=True, inferSchema=True)

df.printSchema()
df.show(5)
```

**Resultado esperado:**
```
root
 |-- id: integer (nullable = true)
 |-- producto: string (nullable = true)
 |-- monto: double (nullable = true)
 |-- fecha: string (nullable = true)   # ⚠ infiere string en lugar de date

+---+---------+-------+----------+
| id| producto|  monto|     fecha|
+---+---------+-------+----------+
|  1|  manzana|  150.5|2024-01-10|
|  2|   banana|   80.0|2024-01-11|
+---+---------+-------+----------+
```

> Nótese que `fecha` queda como `string`: inferSchema no siempre acierta con fechas.

---

### 4.2 Schema explícito

---

#### Ejemplo 2 — Medio: schema explícito con tipos correctos

Definimos el schema manualmente para garantizar tipos correctos y evitar el costo del escaneo previo de inferencia.

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

schema = StructType([
    StructField("id",       IntegerType(), nullable=False),
    StructField("producto", StringType(),  nullable=True),
    StructField("monto",    DoubleType(),  nullable=True),
    StructField("fecha",    DateType(),    nullable=True),
])

df = spark.read.schema(schema).csv("/data/ventas.csv", header=True)

df.printSchema()
df.show(5)
```

**Resultado esperado:**
```
root
 |-- id: integer (nullable = false)
 |-- producto: string (nullable = true)
 |-- monto: double (nullable = true)
 |-- fecha: date (nullable = true)     # ✅ ahora es DateType

+---+---------+-------+----------+
| id| producto|  monto|     fecha|
+---+---------+-------+----------+
|  1|  manzana|  150.5|2024-01-10|
|  2|   banana|   80.0|2024-01-11|
+---+---------+-------+----------+
```

> ✅ Recomendado en producción: más rápido (sin escaneo previo) y sin sorpresas de tipos.

---

#### Ejemplo 3 — Avanzado: schema con tipos complejos (JSON anidado)

Leemos un JSON con estructura anidada y definimos el schema explícitamente para acceder a campos internos de forma segura.

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

schema_pedido = StructType([
    StructField("pedido_id", IntegerType(), False),
    StructField("cliente",   StringType(),  True),
    StructField("items", ArrayType(
        StructType([
            StructField("producto", StringType(), True),
            StructField("cantidad", IntegerType(), True),
            StructField("precio",   DoubleType(),  True),
        ])
    ), True),
])

df = spark.read.schema(schema_pedido).json("/data/pedidos.json")

df.printSchema()
df.select("pedido_id", "cliente", "items").show(truncate=False)
```

**Resultado esperado:**
```
root
 |-- pedido_id: integer (nullable = false)
 |-- cliente: string (nullable = true)
 |-- items: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- producto: string (nullable = true)
 |    |    |-- cantidad: integer (nullable = true)
 |    |    |-- precio: double (nullable = true)

+---------+--------+------------------------------------------------------+
|pedido_id| cliente|                                                 items|
+---------+--------+------------------------------------------------------+
|        1|   Laura|[{laptop, 1, 1200.0}, {mouse, 2, 25.0}]              |
|        2|  Carlos|[{teclado, 1, 80.0}]                                  |
+---------+--------+------------------------------------------------------+
```

> Con `ArrayType(StructType(...))` representamos listas de objetos: el patrón más común en datos de APIs REST o eventos de e-commerce.

---

## 5. Tipos de datos en Spark SQL

Spark maneja distintos tipos de datos, clasificados en:

### 5.1 Tipos primitivos

- StringType
- IntegerType
- LongType
- DoubleType
- DecimalType
- BooleanType
- DateType
- TimestampType

---

### 5.2 Tipos complejos

Estos tipos permiten representar **datos semi-estructurados**.

- ArrayType
- MapType
- StructType

Ejemplo:

```python
StructField(
  "direcciones",
  ArrayType(
    StructType([
      StructField("calle", StringType()),
      StructField("ciudad", StringType())
    ])
  )
)
```

---

## 6. Datos tipo VARIANT (semi-estructurados)

Muchos sistemas modernos manejan datos **no estrictamente estructurados**:
- JSON
- BSON
- Avro

Algunos motores (como Snowflake) los exponen como tipo **VARIANT**.

👉 Spark **no tiene un tipo VARIANT nativo**, pero maneja este tipo de datos mediante **estructuras complejas**.

---

### 6.1 Representación de datos VARIANT en Spark

Spark transforma datos VARIANT en:
- StructType (objetos JSON)
- MapType (claves dinámicas)
- ArrayType (listas)

Ejemplo JSON:

```json
{
  "cliente": {
    "id": 123,
    "atributos": {
      "vip": true,
      "score": 95
    }
  }
}
```

Spark lo representa como un StructType anidado.

---

### 6.2 Acceso a datos VARIANT

```python
df.select("cliente.atributos.score")
```

O usando funciones SQL:

```sql
SELECT cliente.atributos.score FROM tabla
```

---

## 7. Operaciones básicas con DataFrames

Operaciones disponibles: `select`, `filter` / `where`, `withColumn`, `drop`, `withColumnRenamed`

---

#### Ejemplo 1 — Simple: `select` y `filter`

Seleccionamos solo las columnas de interés y filtramos las ventas superiores a $1000.

```python
from pyspark.sql.functions import col

data = [
    (1, "Ana",   1500.0, "AR"),
    (2, "Luis",   800.0, "MX"),
    (3, "Marta", 2200.0, "AR"),
    (4, "Pedro",  450.0, "CL"),
]
df = spark.createDataFrame(data, ["id", "cliente", "monto", "pais"])

resultado = (df
    .filter(col("monto") > 1000)
    .select("cliente", "monto", "pais")
)
resultado.show()
```

**Resultado esperado:**
```
+-------+------+----+
|cliente| monto|pais|
+-------+------+----+
|    Ana|1500.0|  AR|
|  Marta|2200.0|  AR|
+-------+------+----+
```

> `filter` y `select` son transformaciones lazy: no ejecutan nada hasta que `show()` (una acción) las dispara.

---

#### Ejemplo 2 — Medio: `withColumn`, `withColumnRenamed` y `drop`

Creamos una columna nueva con el monto convertido a USD, renombramos columnas y eliminamos las que ya no necesitamos.

```python
from pyspark.sql.functions import col, round as spark_round

TIPO_CAMBIO = 1000.0  # 1 USD = 1000 ARS

df_transformado = (df
    .withColumn("monto_usd", spark_round(col("monto") / TIPO_CAMBIO, 2))
    .withColumnRenamed("cliente", "nombre_cliente")
    .drop("id")
)
df_transformado.show()
```

**Resultado esperado:**
```
+--------------+------+----+---------+
|nombre_cliente| monto|pais|monto_usd|
+--------------+------+----+---------+
|           Ana|1500.0|  AR|      1.5|
|          Luis| 800.0|  MX|      0.8|
|         Marta|2200.0|  AR|      2.2|
|         Pedro| 450.0|  CL|     0.45|
+--------------+------+----+---------+
```

> `withColumn` no modifica el DataFrame original (los DataFrames son inmutables): devuelve uno nuevo. Por eso encadenamos las operaciones.

---

#### Ejemplo 3 — Avanzado: pipeline completo de limpieza y enriquecimiento

Combinamos varias operaciones básicas en un pipeline realista: limpiamos nulos, categorizamos el monto y ordenamos el resultado final.

```python
from pyspark.sql.functions import col, when, upper, coalesce, lit

data_cruda = [
    (1, "ana",   1500.0, "AR"),
    (2, "luis",   None,  "MX"),
    (3, "marta", 2200.0,  None),
    (4, "pedro",  450.0, "CL"),
    (5, "sofia", 5800.0, "AR"),
]
df_crudo = spark.createDataFrame(data_cruda, ["id", "cliente", "monto", "pais"])

df_limpio = (df_crudo
    # 1. Rellenar nulos
    .fillna({"monto": 0.0, "pais": "DESCONOCIDO"})
    # 2. Normalizar texto
    .withColumn("cliente", upper(col("cliente")))
    # 3. Categorizar monto
    .withColumn("categoria",
        when(col("monto") < 1000, "bajo")
        .when(col("monto") < 3000, "medio")
        .otherwise("alto")
    )
    # 4. Ordenar por monto descendente
    .orderBy(col("monto").desc())
    .select("id", "cliente", "pais", "monto", "categoria")
)
df_limpio.show()
```

**Resultado esperado:**
```
+---+-------+-----------+------+---------+
| id|cliente|       pais| monto|categoria|
+---+-------+-----------+------+---------+
|  5|  SOFIA|         AR|5800.0|     alto|
|  3|  MARTA|DESCONOCIDO|2200.0|    medio|
|  1|    ANA|         AR|1500.0|    medio|
|  4|  PEDRO|         CL| 450.0|     bajo|
|  2|   LUIS|         MX|   0.0|     bajo|
+---+-------+-----------+------+---------+
```

> `when().otherwise()` es el equivalente distribuido al `CASE WHEN` de SQL. Evita usar UDFs para lógica condicional simple.

---

## 8. Spark SQL

Los DataFrames pueden exponerse como vistas SQL y consultarse con sintaxis estándar SQL.
Spark SQL y la API de DataFrames usan el **mismo motor (Catalyst)**: el resultado es idéntico.

---

#### Ejemplo 1 — Simple: vista temporal y consulta básica

Registramos el DataFrame como vista y ejecutamos una consulta SQL para totalizar ventas por país.

```python
data = [
    ("Ana",   1500.0, "AR"),
    ("Luis",   800.0, "MX"),
    ("Marta", 2200.0, "AR"),
    ("Pedro",  450.0, "CL"),
    ("Sofía", 5800.0, "AR"),
]
df = spark.createDataFrame(data, ["cliente", "monto", "pais"])

df.createOrReplaceTempView("ventas")

resultado = spark.sql("""
    SELECT pais, SUM(monto) AS total_vendido
    FROM ventas
    GROUP BY pais
    ORDER BY total_vendido DESC
""")
resultado.show()
```

**Resultado esperado:**
```
+----+-------------+
|pais|total_vendido|
+----+-------------+
|  AR|       9500.0|
|  MX|        800.0|
|  CL|        450.0|
+----+-------------+
```

> `createOrReplaceTempView` registra la vista solo para la sesión actual. No persiste en disco.

---

#### Ejemplo 2 — Medio: subquery SQL y filtrado condicional

Usamos una subconsulta para obtener solo los clientes cuyo monto supera el promedio general.

```python
resultado = spark.sql("""
    SELECT cliente, monto, pais
    FROM ventas
    WHERE monto > (SELECT AVG(monto) FROM ventas)
    ORDER BY monto DESC
""")
resultado.show()
```

**Resultado esperado:**
```
+-------+------+----+
|cliente| monto|pais|
+-------+------+----+
|  Sofía|5800.0|  AR|
|  Marta|2200.0|  AR|
|    Ana|1500.0|  AR|
+-------+------+----+
```

> El promedio general es (1500+800+2200+450+5800)/5 = 2150. Solo los tres primeros lo superan.

---

#### Ejemplo 3 — Avanzado: SQL con CTE, window function y múltiples vistas

Combinamos dos vistas (ventas + países), usamos un CTE para calcular rankings y filtramos el top por región.

```python
data_paises = [("AR", "Argentina"), ("MX", "México"), ("CL", "Chile")]
df_paises = spark.createDataFrame(data_paises, ["codigo", "nombre_pais"])
df_paises.createOrReplaceTempView("paises")
# df "ventas" ya fue registrada arriba

resultado = spark.sql("""
    WITH ventas_enriquecidas AS (
        SELECT
            v.cliente,
            v.monto,
            p.nombre_pais,
            RANK() OVER (PARTITION BY v.pais ORDER BY v.monto DESC) AS ranking_en_pais
        FROM ventas v
        JOIN paises p ON v.pais = p.codigo
    )
    SELECT *
    FROM ventas_enriquecidas
    WHERE ranking_en_pais = 1
    ORDER BY monto DESC
""")
resultado.show()
```

**Resultado esperado:**
```
+-------+------+-----------+---------------+
|cliente| monto|nombre_pais|ranking_en_pais|
+-------+------+-----------+---------------+
|  Sofía|5800.0|  Argentina|              1|
|   Luis| 800.0|     México|              1|
|  Pedro| 450.0|      Chile|              1|
+-------+------+-----------+---------------+
```

> Los CTEs (`WITH`) hacen el SQL más legible. `RANK() OVER (PARTITION BY ...)` es una window function SQL equivalente a la API `Window` de PySpark.

---

## 9. Caso de uso real

### Caso: ingestión de eventos JSON

- Fuente: logs JSON con estructura cambiante
- Necesidad: analizar sin romper pipelines

Solución:
- DataFrames
- StructType
- Tipos complejos

Este patrón es base de Data Lakes modernos.

---

## 10. Práctica guiada

### Ejercicio 1 — Simple: explorar un JSON anidado

**Consigna:** Dado un JSON con información de clientes y su dirección anidada, leerlo, imprimir el schema y extraer el nombre del cliente junto con su ciudad.

```python
import json

data_json = [
    '{"id": 1, "nombre": "Ana", "direccion": {"ciudad": "Buenos Aires", "pais": "AR"}}',
    '{"id": 2, "nombre": "Luis", "direccion": {"ciudad": "Ciudad de México", "pais": "MX"}}',
    '{"id": 3, "nombre": "Marta", "direccion": {"ciudad": "Santiago", "pais": "CL"}}',
]

rdd_json = spark.sparkContext.parallelize(data_json)
df = spark.read.json(rdd_json)

# 1. Ver el schema inferido
df.printSchema()

# 2. Extraer campos anidados con notación punto
df.select("id", "nombre", "direccion.ciudad", "direccion.pais").show()
```

**Resultado esperado:**
```
root
 |-- direccion: struct (nullable = true)
 |    |-- ciudad: string (nullable = true)
 |    |-- pais: string (nullable = true)
 |-- id: long (nullable = true)
 |-- nombre: string (nullable = true)

+---+------+----------------+----+
| id|nombre|          ciudad|pais|
+---+------+----------------+----+
|  1|   Ana|    Buenos Aires|  AR|
|  2|  Luis|Ciudad de México|  MX|
|  3| Marta|        Santiago|  CL|
+---+------+----------------+----+
```

---

### Ejercicio 2 — Medio: comparar inferencia vs schema explícito

**Consigna:** Crear el mismo DataFrame con `inferSchema=True` y con schema manual. Verificar la diferencia de tipos y medir el impacto.

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType
import time

# Con inferencia
t0 = time.time()
df_inferido = spark.read.csv("/data/ventas.csv", header=True, inferSchema=True)
df_inferido.count()  # forzamos ejecución
t1 = time.time()
print(f"Con inferSchema: {t1-t0:.2f}s")
df_inferido.printSchema()

# Con schema explícito
schema = StructType([
    StructField("id",       IntegerType(),      False),
    StructField("producto", StringType(),        True),
    StructField("monto",    DecimalType(10, 2),  True),
    StructField("fecha",    DateType(),          True),
])
t2 = time.time()
df_explicito = spark.read.schema(schema).csv("/data/ventas.csv", header=True)
df_explicito.count()
t3 = time.time()
print(f"Con schema explícito: {t3-t2:.2f}s")
df_explicito.printSchema()
```

**Resultado esperado** (ejemplo orientativo):
```
Con inferSchema:       3.84s
root
 |-- id: integer (nullable = true)
 |-- producto: string (nullable = true)
 |-- monto: double (nullable = true)
 |-- fecha: string (nullable = true)   # ⚠ fecha mal inferida

Con schema explícito:  1.12s
root
 |-- id: integer (nullable = false)
 |-- producto: string (nullable = true)
 |-- monto: decimal(10,2) (nullable = true)
 |-- fecha: date (nullable = true)     # ✅ tipo correcto
```

> El schema explícito es ~3x más rápido porque Spark no necesita escanear el archivo completo para inferir tipos.

---

## 11. Preguntas de revisión

- ¿Por qué los DataFrames son más rápidos que RDDs?
- ¿Qué es un tipo complejo?
- ¿Cómo maneja Spark datos tipo VARIANT?

---

## 12. Resumen

- DataFrames = API principal de Spark
- Schema explícito mejora performance
- Tipos complejos permiten datos VARIANT
- Base de Spark SQL

---

**Próxima unidad**: Transformaciones avanzadas
