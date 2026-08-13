## Unidad 11: Ingesta con Auto Loader, Schema Evolution y Cloud Files

**Tecnicatura en Datos – Procesamiento con Apache Spark (Databricks)**  
Unidad 11 de 10 — Duración estimada: 2:30 hs

---

## 1. Introducción

En la unidad anterior trabajamos con lectura y escritura de archivos usando métodos tradicionales (`spark.read.csv`, `spark.read.json`, etc.). Sin embargo, en entornos de producción surgen desafíos complejos:

- **Archivos que llegan continuamente** a una carpeta (Data Lake)
- **Esquemas que cambian** con el tiempo (nuevas columnas, tipos modificados)
- **Procesamiento incremental** (solo leer archivos nuevos, no reprocesar todo)
- **Resiliencia** ante fallos y reintentos

Para estos casos, Databricks ofrece **Auto Loader** (también llamado `cloudFiles`), un motor optimizado de ingesta incremental que:

✅ Detecta automáticamente archivos nuevos usando notificaciones del cloud storage  
✅ Maneja evolución de schema automáticamente  
✅ Escala a millones de archivos sin degradación de rendimiento  
✅ Reinicia de forma segura desde el último punto procesado  

Esta unidad cubre el **ciclo completo de ingesta moderna**:
- Cómo usar Auto Loader para ingesta incremental
- Configurar schema evolution (evolución de schema)
- Mejores prácticas para pipelines de producción

---

## 2. ¿Qué es Auto Loader?

### 2.1 El problema: ingesta tradicional con file source

**Método tradicional (Structured Streaming con file source):**

```python
# Lee archivos nuevos en la carpeta
df = (spark.readStream
    .schema(schema_fijo)
    .json("/mnt/datalake/eventos/")
)
```

**Problemas:**
- ❌ Requiere listar todos los archivos en cada trigger → lento con muchos archivos
- ❌ Schema fijo: si llega un archivo con columnas nuevas, el stream falla
- ❌ No aprovecha notificaciones del cloud (S3 Events, Azure Event Grid)

### 2.2 La solución: Auto Loader (`cloudFiles`)

**Auto Loader:**

```python
# Auto Loader detecta archivos nuevos eficientemente
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/datalake/schemas/eventos/")
    .load("/mnt/datalake/eventos/")
)
```

**Ventajas:**
- ✅ Usa **notificaciones del cloud** (file notifications) para detectar archivos nuevos
- ✅ **Schema inference** automático en el primer archivo
- ✅ **Schema evolution** configurable: añade columnas automáticamente
- ✅ Escala a **millones de archivos** sin problemas
- ✅ **Checkpoint** automático para reinicio seguro

---

## 3. Arquitectura de Auto Loader

### 3.1 Dos modos de detección de archivos

| Modo | Cómo funciona | Cuándo usar |
|------|---------------|-------------|
| **Directory listing** | Lista archivos en la carpeta periódicamente | < 10,000 archivos, desarrollo |
| **File notification** | Suscribe a eventos del cloud storage (S3/ADLS) | > 10,000 archivos, producción |

```python
# Modo directory listing (default para carpetas pequeñas)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "false")  # explícito
    .load("/mnt/datalake/eventos/")
)

# Modo file notification (automático en Databricks cuando hay muchos archivos)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")   # Databricks lo configura automáticamente
    .load("/mnt/datalake/eventos/")
)
```

> En Databricks, el modo `file notification` se activa automáticamente cuando detecta un volumen alto de archivos. No necesitas configurar colas manualmente.

---

## 4. Schema Evolution (Evolución de Schema)

### 4.1 El desafío

```
Día 1: archivo_v1.json
{
  "id": 1,
  "nombre": "Ana",
  "monto": 100.0
}

Día 30: archivo_v2.json (nueva columna "categoria")
{
  "id": 2,
  "nombre": "Luis",
  "monto": 250.0,
  "categoria": "premium"    ← columna nueva
}
```

**Sin schema evolution:** el stream fallaría al encontrar `categoria`  
**Con schema evolution:** Auto Loader agrega `categoria` automáticamente con `null` en filas anteriores

### 4.2 Modos de Schema Evolution

| Opción | Comportamiento |
|--------|----------------|
| `addNewColumns` (default) | Agrega columnas nuevas automáticamente |
| `failOnNewColumns` | Falla si detecta columnas nuevas |
| `rescue` | Guarda columnas no reconocidas en `_rescued_data` |

---

## 5. Ejemplos prácticos

### Ejemplo 1 — Simple: Ingesta básica con Auto Loader (JSON)

```python
# Ingesta incremental de archivos JSON con schema inference automático
df_eventos = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/eventos/")  # guarda schema inferido
    .load("/mnt/datalake/raw/eventos/")
)

# Escribir a tabla Delta
query = (df_eventos.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/eventos/")
    .option("mergeSchema", "true")  # permite evolución de schema en Delta
    .table("bronze.eventos")
)

query.awaitTermination()
```

**Resultado esperado:**
```
Primera ejecución:
- Lee archivo_001.json → infiere schema (id, nombre, monto)
- Guarda schema en /mnt/schemas/eventos/
- Escribe a tabla bronze.eventos

Segunda ejecución (llega archivo_002.json con columna "categoria"):
- Detecta nueva columna "categoria"
- Actualiza schema guardado
- Agrega "categoria" a la tabla (valores anteriores = null)
- Continúa procesando
```

> `schemaLocation` es **obligatorio**: aquí Auto Loader guarda y versiona el schema inferido. Si cambias esta ruta, Auto Loader reinferirá el schema desde cero.

---

### Ejemplo 2 — Medio: CSV con schema hints y rescue data

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col

# Schema inicial conocido (hint para Auto Loader)
schema_hint = StructType([
    StructField("id",      IntegerType(), False),
    StructField("cliente", StringType(),  True),
    StructField("monto",   DoubleType(),  True),
])

df_ventas = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/ventas/")
    .option("cloudFiles.schemaEvolutionMode", "rescue")  # columnas no reconocidas → _rescued_data
    .option("cloudFiles.inferColumnTypes", "true")       # infiere tipos automáticamente
    .option("header", "true")
    .schema(schema_hint)  # schema inicial (Auto Loader puede expandirlo)
    .load("/mnt/datalake/raw/ventas/")
)

# Filtrar solo filas válidas (sin datos rescatados)
df_validas = df_ventas.filter(col("_rescued_data").isNull())

# Guardar filas con errores aparte para revisión
df_errores = df_ventas.filter(col("_rescued_data").isNotNull())

query_validas = (df_validas.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/ventas/")
    .option("mergeSchema", "true")
    .table("bronze.ventas")
)

query_errores = (df_errores.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/ventas_errores/")
    .table("bronze.ventas_errores")
)

query_validas.awaitTermination()
```

**Resultado esperado:**
```
Tabla bronze.ventas:
+---+--------+------+
| id| cliente| monto|
+---+--------+------+
|  1|     Ana|1500.0|
|  2|    Luis| 800.0|
+---+--------+------+

Tabla bronze.ventas_errores (si llega columna inesperada "codigo_promo"):
+---+--------+------+------------------------------+
| id| cliente| monto|               _rescued_data|
+---+--------+------+------------------------------+
|  3|   Marta|2200.0|{"codigo_promo": "VERANO2024"}|
+---+--------+------+------------------------------+
```

> Modo `rescue` es útil en QA: permite continuar la ingesta mientras capturas anomalías de schema para revisión manual.

---

### Ejemplo 3 — Avanzado: Ingesta multi-formato con particiones y metadata

```python
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, 
    to_date, year, month, dayofmonth
)

# Auto Loader con JSON, metadata de ingesta y particionado
df_transacciones = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/transacciones/")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.maxFilesPerTrigger", 1000)  # procesa hasta 1000 archivos por micro-lote
    .load("/mnt/datalake/raw/transacciones/")
)

# Enriquecer con metadata de ingesta
df_enriquecido = (df_transacciones
    .withColumn("archivo_origen", input_file_name())
    .withColumn("timestamp_ingesta", current_timestamp())
    .withColumn("fecha_transaccion", to_date(col("timestamp")))  # asume que hay columna "timestamp"
    .withColumn("año", year(col("fecha_transaccion")))
    .withColumn("mes", month(col("fecha_transaccion")))
    .withColumn("dia", dayofmonth(col("fecha_transaccion")))
)

# Escribir particionado por año/mes para optimizar queries
query = (df_enriquecido.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/transacciones/")
    .option("mergeSchema", "true")
    .partitionBy("año", "mes")  # particionado físico
    .table("bronze.transacciones")
)

query.awaitTermination()
```

**Resultado esperado:**
```
Estructura física en storage:
/mnt/datalake/bronze/transacciones/
  ├── año=2024/
  │   ├── mes=1/
  │   │   └── part-00000-xxx.snappy.parquet
  │   └── mes=2/
  │       └── part-00000-yyy.snappy.parquet
  └── año=2025/
      └── mes=1/
          └── part-00000-zzz.snappy.parquet

Query optimizado por filtro de partición:
SELECT * FROM bronze.transacciones 
WHERE año = 2024 AND mes = 1
→ solo lee la carpeta año=2024/mes=1/ (partition pruning)
```

> `partitionBy` mejora drásticamente el rendimiento de queries con filtros temporales. Elige columnas de baja cardinalidad (año, mes, país) como particiones.

---

## 6. Configuraciones importantes de Auto Loader

### 6.1 Opciones clave

| Opción | Descripción | Valor recomendado |
|--------|-------------|-------------------|
| `cloudFiles.format` | Formato de archivo (csv, json, parquet, avro, orc, text, binaryFile) | Obligatorio |
| `cloudFiles.schemaLocation` | Ruta donde guardar schema inferido | Obligatorio (ruta Delta) |
| `cloudFiles.schemaEvolutionMode` | Cómo manejar cambios: `addNewColumns`, `failOnNewColumns`, `rescue`, `none` | `addNewColumns` |
| `cloudFiles.inferColumnTypes` | Inferir tipos de datos (true/false) | `true` |
| `cloudFiles.maxFilesPerTrigger` | Límite de archivos por micro-lote | `1000` (ajustar según volumen) |
| `cloudFiles.useNotifications` | Usar eventos del cloud storage | `true` (auto en prod) |
| `cloudFiles.includeExistingFiles` | Procesar archivos existentes al iniciar | `true` (default) |

---

### Ejemplo 4 — Configuración completa de producción

```python
df_prod = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/eventos_prod/")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.maxFilesPerTrigger", 500)
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.includeExistingFiles", "true")  # procesa histórico al iniciar
    .option("cloudFiles.validateOptions", "true")      # valida opciones al iniciar
    .load("/mnt/datalake/raw/eventos_prod/")
)

query = (df_prod.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/eventos_prod/")
    .option("mergeSchema", "true")
    .trigger(processingTime="10 minutes")  # trigger cada 10 minutos (batch)
    .table("bronze.eventos_prod")
)

query.awaitTermination()
```

---

## 7. Mejores prácticas

### 7.1 Estrategia de checkpoint y schema location

```python
# ✅ CORRECTO: rutas separadas y persistentes
checkpointLocation = "/mnt/checkpoints/mi_pipeline/"
schemaLocation     = "/mnt/schemas/mi_pipeline/"

# ❌ INCORRECTO: reutilizar la misma ruta
checkpointLocation = "/tmp/checkpoint/"  # se borra al reiniciar cluster
```

**Reglas:**
- ✅ Usar rutas en storage persistente (S3, ADLS, DBFS)
- ✅ Nunca borrar `schemaLocation` (rompe evolución de schema)
- ✅ Solo borrar `checkpointLocation` si querés reprocesar TODO desde cero
- ❌ Nunca usar `/tmp/` o rutas locales del cluster (se pierden)

---

### 7.2 Manejo de errores y datos corruptos

```python
from pyspark.sql.functions import col

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/eventos/")
    .option("mode", "PERMISSIVE")  # filas malformadas → columna _corrupt_record
    .load("/mnt/datalake/raw/eventos/")
)

# Detectar y separar filas corruptas
df_valido = df.filter(col("_corrupt_record").isNull())
df_corrupto = df.filter(col("_corrupt_record").isNotNull())

# Stream principal (datos válidos)
query_valido = (df_valido.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/eventos_validos/")
    .table("bronze.eventos")
)

# Stream de cuarentena (errores para revisión)
query_errores = (df_corrupto.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/eventos_errores/")
    .table("bronze.eventos_errores")
)

query_valido.awaitTermination()
```

---

### 7.3 Monitoreo del pipeline

```python
# Consultar métricas del stream
query.lastProgress  # último micro-lote procesado
query.status        # estado actual del query
query.recentProgress  # últimos N micro-lotes

# Ejemplo de métricas
import json
print(json.dumps(query.lastProgress, indent=2))
```

**Métricas clave a monitorear:**
```json
{
  "numInputRows": 15420,
  "inputRowsPerSecond": 2570,
  "processedRowsPerSecond": 3850,
  "durationMs": {
    "triggerExecution": 4005
  },
  "sources": [{
    "description": "CloudFilesSource[/mnt/datalake/raw/eventos/]",
    "numInputRows": 15420
  }]
}
```

---

## 8. Comparación: File Source vs Auto Loader

| Característica | File Source tradicional | Auto Loader (`cloudFiles`) |
|----------------|------------------------|----------------------------|
| Detección de archivos | Listing manual (lento) | Notificaciones cloud (rápido) |
| Escalabilidad | Degrada con >10k archivos | Escala a millones |
| Schema inference | Manual o requiere schema | Automático |
| Schema evolution | No (stream falla) | Sí (configurable) |
| Rescue de errores | No | Sí (`_rescued_data`) |
| Configuración | Simple | Más opciones (más potente) |
| Cuándo usar | Pruebas, pocos archivos | Producción, alta escala |

---

## 9. Casos de uso comunes

### 9.1 Pipeline Bronze (Raw → Bronze)

```python
# Capa Bronze: ingesta cruda con metadata, sin transformaciones
df_bronze = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/bronze_eventos/")
    .load("/mnt/datalake/raw/eventos/")
    .withColumn("fecha_ingesta", current_timestamp())
    .withColumn("archivo_origen", input_file_name())
)

query_bronze = (df_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/bronze_eventos/")
    .option("mergeSchema", "true")
    .table("bronze.eventos")
)
```

---

### 9.2 Pipeline con CDC (Change Data Capture)

```python
# Ingesta de logs CDC (INSERT, UPDATE, DELETE)
df_cdc = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/cdc_usuarios/")
    .load("/mnt/datalake/raw/cdc_usuarios/")
)

# Escribir como tabla Delta con MERGE (upsert)
from delta.tables import DeltaTable

def upsert_cdc(df_micro_lote, id_lote):
    delta_table = DeltaTable.forPath(spark, "/mnt/delta/usuarios")
    
    delta_table.alias("destino").merge(
        df_micro_lote.alias("origen"),
        "destino.id = origen.id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

query_cdc = (df_cdc.writeStream
    .foreachBatch(upsert_cdc)
    .option("checkpointLocation", "/mnt/checkpoints/cdc_usuarios/")
    .start()
)
```

---

## 10. Troubleshooting común

### Problema 1: "Schema location is not a Delta table"

**Error:**
```
AnalysisException: Schema location '/mnt/schemas/eventos/' is not a Delta table
```

**Solución:**
```python
# La primera vez, Auto Loader crea la tabla de schema automáticamente
# Si borraste esa carpeta por error, solo borrá todo y dejá que Auto Loader la recree:
dbutils.fs.rm("/mnt/schemas/eventos/", True)
# Volver a ejecutar el stream
```

---

### Problema 2: Stream no detecta archivos nuevos

**Causas comunes:**
- Checkpoint corrupto: borrá `checkpointLocation` y reiniciá (reprocesa todo)
- Archivos con timestamp más antiguo que el último procesado: verificar metadata del archivo
- Permisos: el stream no puede leer la carpeta de origen

**Debug:**
```python
# Ver último archivo procesado
checkpoint_path = "/mnt/checkpoints/mi_pipeline/"
df_checkpoint = spark.read.json(f"{checkpoint_path}/sources/0/0")
df_checkpoint.select("path").show(10, False)
```

---

### Problema 3: Schema evolution no funciona

**Verificar:**
```python
# 1. mergeSchema debe estar en true en el writeStream
.option("mergeSchema", "true")

# 2. schemaEvolutionMode debe permitir cambios
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")

# 3. El schema location no debe estar corrupto
dbutils.fs.ls("/mnt/schemas/mi_pipeline/")  # debe mostrar archivos Delta
```

---

## 11. Ejercicio integrador

**Objetivo:** Implementar un pipeline completo de ingesta con Auto Loader

**Requerimientos:**
1. Ingestar archivos JSON desde `/mnt/raw/ventas/`
2. Activar schema evolution automático
3. Agregar columnas de metadata: `fecha_ingesta`, `archivo_origen`
4. Particionar por `año` y `mes` extraídos de la columna `fecha_venta`
5. Separar registros válidos y errores en tablas distintas
6. Configurar checkpoint en ruta persistente
7. Escribir a tabla Delta `bronze.ventas`

**Solución:**

```python
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name,
    to_date, year, month
)

# 1. Leer con Auto Loader
df_ventas = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/schemas/ventas/")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("mode", "PERMISSIVE")  # captura errores en _corrupt_record
    .load("/mnt/raw/ventas/")
)

# 2 y 3. Agregar metadata
df_enriquecido = (df_ventas
    .withColumn("fecha_ingesta", current_timestamp())
    .withColumn("archivo_origen", input_file_name())
    .withColumn("fecha_venta_date", to_date(col("fecha_venta")))
    .withColumn("año", year(col("fecha_venta_date")))
    .withColumn("mes", month(col("fecha_venta_date")))
)

# 4. Separar válidos y errores
df_validos = df_enriquecido.filter(col("_corrupt_record").isNull())
df_errores = df_enriquecido.filter(col("_corrupt_record").isNotNull())

# 5 y 6. Escribir válidos particionado
query_validos = (df_validos.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/ventas_validas/")
    .option("mergeSchema", "true")
    .partitionBy("año", "mes")
    .table("bronze.ventas")
)

# 7. Escribir errores
query_errores = (df_errores.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/ventas_errores/")
    .table("bronze.ventas_errores")
)

query_validos.awaitTermination()
```

---

## 12. Resumen y conclusiones

### ✅ Auto Loader es la solución recomendada para ingesta en Databricks cuando:
- Procesás archivos que llegan continuamente a un Data Lake
- Necesitás escalar a miles o millones de archivos
- El schema puede cambiar con el tiempo
- Requerís resiliencia ante fallos y reintentos

### 🔑 Conceptos clave:
1. **`cloudFiles.format`**: el formato de tus archivos (csv, json, parquet, etc.)
2. **`cloudFiles.schemaLocation`**: dónde guardar el schema inferido (obligatorio)
3. **`schemaEvolutionMode`**: cómo manejar cambios de schema (`addNewColumns`, `rescue`, `failOnNewColumns`)
4. **`checkpointLocation`**: dónde guardar el progreso del stream (obligatorio en producción)
5. **`mergeSchema`**: permitir evolución de schema en la tabla Delta destino

### 📚 Recursos adicionales:
- Documentación oficial: [Auto Loader - Databricks](https://docs.databricks.com/ingestion/auto-loader/index.html)
- Guía de schema evolution: [Schema Evolution Guide](https://docs.databricks.com/ingestion/auto-loader/schema.html)
- Best practices: [Auto Loader Best Practices](https://docs.databricks.com/ingestion/auto-loader/best-practices.html)

---

**Próxima unidad:** Optimización de pipelines de streaming y arquitecturas Delta Lake avanzadas