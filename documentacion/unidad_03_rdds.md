## Unidad 3: RDDs — Resilient Distributed Datasets

**Tecnicatura en Datos – Procesamiento con Apache Spark**  
Unidad 3 de 10 — Duración estimada: 2:30 hs

---

## 1. Introducción

Antes de los DataFrames y de Spark SQL, **el corazón original de Spark fueron los RDDs**.
Aunque hoy muchas aplicaciones se desarrollan con APIs de alto nivel, **entender los RDDs es clave** para:

- Comprender el modelo de ejecución interno de Spark
- Entender la tolerancia a fallos
- Leer la Spark UI con criterio técnico
- Diagnosticar problemas de performance

Los RDDs no están obsoletos: son la **base conceptual** sobre la que Spark está construido.

---

## 2. ¿Qué es un RDD?

RDD significa **Resilient Distributed Dataset**.

Un RDD es:
- **Resilient**: tolerante a fallos
- **Distributed**: distribuido en varios nodos
- **Dataset**: colección de elementos

Es decir: **una colección distribuida e inmutable de datos**, particionada a lo largo del clúster.

### Características clave

1. **Inmutabilidad**  
   Una vez creado, un RDD no se modifica. Cada transformación crea un nuevo RDD.

2. **Particionado**  
   Los datos se dividen en particiones, y cada partición se procesa en paralelo.

3. **Tolerancia a fallos mediante lineage**  
   Spark almacena cómo se creó el RDD, no los datos en sí.

---

## 3. Creación de RDDs

### 3.1 Desde una colección local

---

#### Ejemplo 1 — Simple: lista de números

Creamos un RDD a partir de una lista en memoria y verificamos cuántas particiones tiene.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD_Intro").getOrCreate()
sc = spark.sparkContext

numeros = sc.parallelize([1, 2, 3, 4, 5])

print("Número de particiones:", numeros.getNumPartitions())
print("Elementos:", numeros.collect())
```

**Resultado esperado:**
```
Número de particiones: 4          # varía según los cores disponibles
Elementos: [1, 2, 3, 4, 5]
```

> Uso típico: testing y ejemplos educativos. No recomendado para grandes volúmenes.

---

#### Ejemplo 2 — Medio: lista de tuplas (datos semiestructurados)

Creamos un RDD de ventas con forma `(producto, monto)` y calculamos el total vendido agrupando por producto.

```python
ventas = sc.parallelize([
    ("manzana", 100),
    ("banana",  200),
    ("manzana", 150),
    ("banana",   50),
    ("naranja", 300),
])

# Suma de montos por producto
total_por_producto = ventas.reduceByKey(lambda a, b: a + b)

print(total_por_producto.collect())
```

**Resultado esperado:**
```
[('manzana', 250), ('banana', 250), ('naranja', 300)]
```

> `reduceByKey` agrupa por la primera posición de la tupla y aplica la función acumuladora sobre los valores.

---

#### Ejemplo 3 — Avanzado: control explícito de particiones

Creamos un RDD con un número específico de particiones y verificamos que los datos se distribuyen de forma equilibrada.

```python
datos = sc.parallelize(range(1, 21), numSlices=4)  # 20 elementos en 4 particiones

# Mostrar los elementos de cada partición
particiones = datos.glom().collect()
for i, part in enumerate(particiones):
    print(f"Partición {i}: {part}")

print("Total elementos:", datos.count())
```

**Resultado esperado:**
```
Partición 0: [1, 2, 3, 4, 5]
Partición 1: [6, 7, 8, 9, 10]
Partición 2: [11, 12, 13, 14, 15]
Partición 3: [16, 17, 18, 19, 20]
Total elementos: 20
```

> `glom()` convierte cada partición en una lista, permitiendo ver cómo Spark distribuyó los datos. Esto es útil para diagnosticar skew (desbalance de datos).

---

### 3.2 Desde almacenamiento distribuido

```python
rdd = spark.sparkContext.textFile("/data/input.txt")
```

Spark:
- Divide el archivo en bloques (por defecto, el tamaño de bloque HDFS)
- Crea una partición por bloque
- Distribuye el procesamiento entre los nodos

---

## 4. Lazy Evaluation

En Spark, **las transformaciones no se ejecutan inmediatamente**.

Ejemplo:

```python
rdd2 = rdd.map(lambda x: x * 2)
```

Nada se ejecuta hasta que aparece una **acción**.

Esto permite a Spark:
- Construir el DAG completo
- Optimizar el plan de ejecución

---

## 5. Transformaciones vs Acciones

### 5.1 Transformaciones

Devuelven un nuevo RDD. **No ejecutan nada**, solo definen el plan.

Transformaciones más comunes: `map`, `filter`, `flatMap`, `distinct`, `union`

---

#### Ejemplo 1 — Simple: `filter` y `map`

Partimos de una lista de números, filtramos los pares y los duplicamos.

```python
numeros = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8])

pares     = numeros.filter(lambda x: x % 2 == 0)  # Transformación 1
duplicados = pares.map(lambda x: x * 2)            # Transformación 2

# Nada se ejecutó todavía. La acción collect() dispara todo:
print(duplicados.collect())
```

**Resultado esperado:**
```
[4, 8, 12, 16]
```

> Los números impares (1, 3, 5, 7) son eliminados por `filter`. Luego `map` duplica cada par restante.

---

#### Ejemplo 2 — Medio: `flatMap` vs `map`

Mostramos la diferencia clave entre `map` (1 elemento → 1 elemento) y `flatMap` (1 elemento → N elementos).

```python
frases = sc.parallelize([
    "Spark es rápido",
    "RDD es la base",
    "flatMap aplana listas",
])

# map: produce un RDD de listas
con_map = frases.map(lambda f: f.split(" "))
print("map →", con_map.collect())

# flatMap: produce un RDD de palabras sueltas
con_flatmap = frases.flatMap(lambda f: f.split(" "))
print("flatMap →", con_flatmap.collect())
```

**Resultado esperado:**
```
map →    [['Spark', 'es', 'rápido'], ['RDD', 'es', 'la', 'base'], ['flatMap', 'aplana', 'listas']]
flatMap → ['Spark', 'es', 'rápido', 'RDD', 'es', 'la', 'base', 'flatMap', 'aplana', 'listas']
```

> `map` produce 3 listas (una por frase). `flatMap` aplana el resultado y produce 10 palabras individuales.

---

#### Ejemplo 3 — Avanzado: cadena de transformaciones (`distinct`, `union`, `sortBy`)

Combinamos dos fuentes de datos, eliminamos duplicados y ordenamos el resultado final.

```python
ventas_enero = sc.parallelize(["manzana", "banana", "naranja", "manzana"])
ventas_febrero = sc.parallelize(["banana", "uva", "naranja", "kiwi"])

# Unión de ambos meses (puede tener duplicados)
todas = ventas_enero.union(ventas_febrero)

# Productos únicos vendidos en alguno de los dos meses
unicos = todas.distinct()

# Ordenados alfabéticamente
ordenados = unicos.sortBy(lambda x: x)

print(ordenados.collect())
```

**Resultado esperado:**
```
['banana', 'kiwi', 'manzana', 'naranja', 'uva']
```

> `union` concatena sin eliminar duplicados. `distinct` los elimina. `sortBy` ordena sin perder el paralelismo (usa una clave de ordenamiento).

---

### 5.2 Acciones

Disparan la ejecución del DAG completo y devuelven un resultado al driver o escriben en disco.

Acciones más comunes: `count()`, `collect()`, `take()`, `reduce()`, `saveAsTextFile()`

---

#### Ejemplo 1 — Simple: `count()` y `take()`

```python
numeros = sc.parallelize(range(1, 101))  # 1 al 100

print("Total de elementos:", numeros.count())
print("Primeros 5:", numeros.take(5))
```

**Resultado esperado:**
```
Total de elementos: 100
Primeros 5: [1, 2, 3, 4, 5]
```

> `count()` recorre todas las particiones para contar. `take(n)` es más eficiente: solo lee hasta encontrar los primeros n elementos.

---

#### Ejemplo 2 — Medio: `reduce()`

Calculamos la suma y el producto de todos los elementos usando `reduce`.

```python
numeros = sc.parallelize([1, 2, 3, 4, 5])

suma     = numeros.reduce(lambda a, b: a + b)
producto = numeros.reduce(lambda a, b: a * b)

print("Suma:", suma)
print("Producto:", producto)
```

**Resultado esperado:**
```
Suma:     15
Producto: 120
```

> `reduce` aplica la función acumulando de izquierda a derecha. La función debe ser **asociativa y conmutativa** para funcionar correctamente en entornos distribuidos.

---

#### Ejemplo 3 — Avanzado: `aggregate()` (múltiples acumuladores en una pasada)

Calculamos la suma y el conteo al mismo tiempo para obtener el promedio, sin recorrer el RDD dos veces.

```python
numeros = sc.parallelize([10, 20, 30, 40, 50])

# valor inicial del acumulador: (suma, conteo)
cero = (0, 0)

# función que combina un acumulador con un elemento
def combinar(acc, valor):
    return (acc[0] + valor, acc[1] + 1)

# función que combina dos acumuladores entre particiones
def fusionar(acc1, acc2):
    return (acc1[0] + acc2[0], acc1[1] + acc2[1])

suma_total, cantidad = numeros.aggregate(cero, combinar, fusionar)
promedio = suma_total / cantidad

print(f"Suma: {suma_total}, Cantidad: {cantidad}, Promedio: {promedio}")
```

**Resultado esperado:**
```
Suma: 150, Cantidad: 5, Promedio: 30.0
```

> `aggregate` es más eficiente que llamar a `sum()` y `count()` por separado porque recorre el RDD **una sola vez**, reduciendo el I/O distribuido.

---

## 6. Particiones y paralelismo

Cada RDD tiene un número de **particiones**.

```python
rdd.getNumPartitions()
```

Regla práctica:
- Más particiones → más paralelismo
- Demasiadas particiones → overhead

El número óptimo depende de:
- CPU disponibles
- Volumen de datos

---

## 7. Lineage y tolerancia a fallos

Spark **no replica RDDs por defecto**.

En cambio, mantiene el **lineage graph**:

```text
Archivo → filter → map → reduce
```

Si una partición se pierde:
- Spark recalcula solo esa partición
- Usando los pasos anteriores

✅ Recuperación eficiente
✅ Menor uso de memoria

---

## 8. Caso clásico: Word Count con RDDs

El Word Count es el "Hola Mundo" del procesamiento distribuido. Lo desarrollamos en tres niveles de complejidad.

---

### Ejemplo 1 — Simple: contar palabras en una lista pequeña

```python
texto = sc.parallelize([
    "hola mundo",
    "hola spark",
    "spark es rapido",
])

conteo = (texto
    .flatMap(lambda linea: linea.split(" "))
    .map(lambda palabra: (palabra, 1))
    .reduceByKey(lambda a, b: a + b)
)

print(conteo.collect())
```

**Resultado esperado:**
```
[('hola', 2), ('mundo', 1), ('spark', 2), ('es', 1), ('rapido', 1)]
```

> `flatMap` divide cada línea en palabras. `map` convierte cada palabra en el par `(palabra, 1)`. `reduceByKey` suma los 1s de cada palabra igual.

---

### Ejemplo 2 — Medio: Word Count desde archivo, ordenado por frecuencia

```python
rdd = spark.sparkContext.textFile("/data/textos.txt")

conteo = (rdd
    .flatMap(lambda linea: linea.split(" "))
    .map(lambda p: p.lower().strip())       # normalizar
    .filter(lambda p: len(p) > 0)           # eliminar vacíos
    .map(lambda p: (p, 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda par: par[1], ascending=False)  # más frecuentes primero
)

# Las 10 palabras más frecuentes
print(conteo.take(10))
```

**Resultado esperado** (ejemplo con un archivo de noticias):
```
[('de', 540), ('la', 410), ('en', 380), ('el', 310), ('y', 290), ...]
```

> `lower()` y `strip()` normalizan el texto para que "Spark" y "spark" se cuenten como la misma palabra. `sortBy` requiere mover datos entre particiones (shuffle), por eso va al final.

---

### Ejemplo 3 — Avanzado: Word Count excluyendo stopwords y con persistencia

Procesamos un corpus grande, filtramos palabras irrelevantes (stopwords), cacheamos el RDD intermedio para reutilizarlo, y guardamos el resultado.

```python
stopwords = {"de", "la", "el", "en", "y", "a", "los", "las", "un", "una", "es"}

rdd = spark.sparkContext.textFile("/data/corpus_grande/*.txt")

palabras_limpias = (rdd
    .flatMap(lambda linea: linea.lower().split())
    .map(lambda p: ''.join(c for c in p if c.isalpha()))   # solo letras
    .filter(lambda p: p and p not in stopwords)             # sin stopwords ni vacíos
    .map(lambda p: (p, 1))
    .reduceByKey(lambda a, b: a + b)
)

# Persistimos en memoria porque vamos a usar el RDD dos veces
palabras_limpias.persist()

# Top 20 palabras más frecuentes
top20 = palabras_limpias.sortBy(lambda par: par[1], ascending=False).take(20)
print("Top 20:", top20)

# Vocabulario total (palabras únicas)
print("Vocabulario único:", palabras_limpias.count())

# Guardar en disco
palabras_limpias.saveAsTextFile("/data/output/word_count_resultado")

palabras_limpias.unpersist()
```

**Resultado esperado** (corpus literario en español):
```
Top 20: [('dijo', 1240), ('señor', 980), ('tiempo', 870), ('gran', 760), ...]
Vocabulario único: 18432
# Se generan N archivos part-00000, part-00001, ... en /data/output/
```

> `.persist()` evita que Spark recalcule el RDD desde cero la segunda vez que lo usamos. `saveAsTextFile` escribe una parte del resultado por partición, distribuido en el clúster.

---

## 9. Uso responsable de RDDs hoy

RDDs se usan cuando:
- Se necesita control fino del procesamiento
- Se trabaja con datos no estructurados
- Se implementan librerías internas

📌 Para datos estructurados:
> **Usar DataFrames siempre que sea posible**

---

## 10. Práctica guiada

### Ejercicio 1 — Simple: filtrado y conteo de temperaturas

**Consigna:** Dado un RDD de temperaturas diarias (en °C), obtener cuántos días superaron los 30°C y cuál fue el valor máximo registrado.

```python
temperaturas = sc.parallelize([22.5, 31.0, 28.3, 35.1, 29.9, 33.4, 19.0, 30.0, 27.6, 36.2])

# 1. Filtrar los días calurosos (> 30°C)
dias_calurosos = temperaturas.filter(lambda t: t > 30)

# 2. Contar cuántos días
cantidad = dias_calurosos.count()

# 3. Temperatura máxima
maxima = temperaturas.reduce(lambda a, b: a if a > b else b)

print(f"Días con más de 30°C: {cantidad}")
print(f"Temperatura máxima: {maxima}°C")
```

**Resultado esperado:**
```
Días con más de 30°C: 4
Temperatura máxima: 36.2°C
```

---

### Ejercicio 2 — Medio: comparar `map` vs `flatMap` con datos reales

**Consigna:** Tenemos una lista de órdenes de compra. Cada orden contiene varios productos separados por coma. Obtener la lista completa de productos comprados (sin importar en qué orden aparecen).

```python
ordenes = sc.parallelize([
    "laptop,mouse,teclado",
    "monitor,auriculares",
    "laptop,webcam,mouse",
    "teclado",
])

# Con map: una lista por orden (no es lo que queremos)
con_map = ordenes.map(lambda o: o.split(","))
print("map →", con_map.collect())

# Con flatMap: todos los productos sueltos
con_flatmap = ordenes.flatMap(lambda o: o.split(","))
print("flatMap →", con_flatmap.collect())

# Productos únicos solicitados
unicos = con_flatmap.distinct().sortBy(lambda x: x)
print("Productos únicos:", unicos.collect())

# ¿Cuántas veces se pidió cada producto?
conteo = con_flatmap.map(lambda p: (p, 1)).reduceByKey(lambda a, b: a + b)
print("Frecuencia:", conteo.sortBy(lambda x: x[1], ascending=False).collect())
```

**Resultado esperado:**
```
map →    [['laptop', 'mouse', 'teclado'], ['monitor', 'auriculares'], ['laptop', 'webcam', 'mouse'], ['teclado']]
flatMap → ['laptop', 'mouse', 'teclado', 'monitor', 'auriculares', 'laptop', 'webcam', 'mouse', 'teclado']
Productos únicos: ['auriculares', 'laptop', 'monitor', 'mouse', 'teclado', 'webcam']
Frecuencia: [('teclado', 2), ('laptop', 2), ('mouse', 2), ('monitor', 1), ('auriculares', 1), ('webcam', 1)]
```

---

### Ejercicio 3 — Avanzado: análisis de logs con RDDs

**Consigna:** Procesar un RDD de líneas de log en formato `NIVEL | timestamp | mensaje`. Calcular cuántos eventos hay de cada nivel (INFO, WARN, ERROR), obtener los mensajes de ERROR, y persistir el resultado intermedio.

```python
logs = sc.parallelize([
    "INFO  | 2024-01-10 08:00 | Servicio iniciado correctamente",
    "WARN  | 2024-01-10 08:05 | Memoria al 80%",
    "ERROR | 2024-01-10 08:07 | Conexión rechazada en puerto 5432",
    "INFO  | 2024-01-10 08:10 | Solicitud procesada",
    "ERROR | 2024-01-10 08:12 | Timeout al conectar con el servicio de pagos",
    "WARN  | 2024-01-10 08:15 | CPU al 90%",
    "INFO  | 2024-01-10 08:20 | Caché limpiado",
    "ERROR | 2024-01-10 08:22 | NullPointerException en módulo de reportes",
])

# Parsear cada línea en (nivel, timestamp, mensaje)
def parsear(linea):
    partes = linea.split(" | ")
    return (partes[0].strip(), partes[1].strip(), partes[2].strip())

logs_parseados = logs.map(parsear)
logs_parseados.persist()   # reutilizaremos este RDD varias veces

# 1. Conteo por nivel
conteo_niveles = (logs_parseados
    .map(lambda r: (r[0], 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: x[0])
)
print("Eventos por nivel:", conteo_niveles.collect())

# 2. Solo los mensajes de ERROR
errores = (logs_parseados
    .filter(lambda r: r[0] == "ERROR")
    .map(lambda r: f"[{r[1]}] {r[2]}")
)
print("\nMensajes de ERROR:")
for e in errores.collect():
    print(" -", e)

# 3. Tasa de error
total  = logs_parseados.count()
nerrors = logs_parseados.filter(lambda r: r[0] == "ERROR").count()
print(f"\nTasa de error: {nerrors}/{total} = {nerrors/total:.1%}")

logs_parseados.unpersist()
```

**Resultado esperado:**
```
Eventos por nivel: [('ERROR', 3), ('INFO', 3), ('WARN', 2)]

Mensajes de ERROR:
 - [2024-01-10 08:07] Conexión rechazada en puerto 5432
 - [2024-01-10 08:12] Timeout al conectar con el servicio de pagos
 - [2024-01-10 08:22] NullPointerException en módulo de reportes

Tasa de error: 3/8 = 37.5%
```

> `.persist()` evita que Spark reprocese el RDD completo en cada acción. Sin él, las 3 operaciones finales releerían y reparsearían los logs desde el principio.

---

## 11. Preguntas de revisión

- ¿Por qué los RDDs son inmutables?
- ¿Qué ventaja tiene el lineage?
- ¿Cuándo conviene usar RDD en lugar de DataFrame?

---

## 12. Resumen

- RDD = base conceptual de Spark
- Inmutables y distribuidos
- Tolerancia a fallos vía lineage
- Transformaciones + acciones

---

**Próxima unidad**: DataFrames y Spark SQL
