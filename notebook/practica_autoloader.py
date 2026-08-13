# Databricks notebook source
# DBTITLE 1,Sección 5: foreachBatch - Procesamiento por Micro-lotes
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # ⚙️ PARTE 5: foreachBatch - Lógica Personalizada
# MAGIC
# MAGIC `foreachBatch` permite ejecutar código personalizado en cada micro-lote:
# MAGIC - UPSERT/MERGE operations (actualizar o insertar)
# MAGIC - Deduplicación
# MAGIC - Validaciones complejas
# MAGIC - Escritura a múltiples destinos
# MAGIC - Logging y auditoría
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,43. Preparar datos para UPSERT - Clientes actualizados
# Crear archivos CSV con actualizaciones y nuevos registros
import os

# Carpeta para datos de actualización
update_path_local = f"{base_path_local}/clientes_updates"
os.makedirs(update_path_local, exist_ok=True)

# BATCH 1: Actualizaciones de clientes existentes + nuevos
csv_update_1 = """id,nombre,email,telefono,ciudad,ultima_compra
1,Ana García ACTUALIZADO,ana.nueva@example.com,555-9999,Madrid,2024-02-15
5,Laura Torres VIP,laura.vip@example.com,555-8888,Barcelona,2024-02-14
20,Nuevo Cliente 1,nuevo1@example.com,555-0020,Sevilla,2024-02-13"""

with open(f"{update_path_local}/clientes_update_batch1.csv", "w") as f:
    f.write(csv_update_1)

print("✅ Creado batch 1: Actualizaciones (Ana, Laura) + Nuevo (id=20)")

# BATCH 2: Más actualizaciones y nuevos
csv_update_2 = """id,nombre,email,telefono,ciudad,ultima_compra
2,Luis Martínez PREMIUM,luis.premium@example.com,555-7777,Valencia,2024-02-16
21,Nuevo Cliente 2,nuevo2@example.com,555-0021,Málaga,2024-02-15
22,Nuevo Cliente 3,nuevo3@example.com,555-0022,Bilbao,2024-02-16"""

with open(f"{update_path_local}/clientes_update_batch2.csv", "w") as f:
    f.write(csv_update_2)

print("✅ Creado batch 2: Actualización (Luis) + Nuevos (id=21,22)")

# COMMAND ----------

# DBTITLE 1,44. Configurar Auto Loader con foreachBatch
# Configuración para stream con foreachBatch
update_source = f"{base_path_volumes}/clientes_updates"
update_checkpoint = f"{checkpoint_path}/clientes_upsert"
update_schema = f"{schema_path}/clientes_upsert"

# Limpiar
dbutils.fs.rm(update_checkpoint, True)
dbutils.fs.rm(update_schema, True)

# Crear carpeta en Volumes
dbutils.fs.mkdirs(update_source)

print(f"📁 Fuente: {update_source}")
print(f"💾 Checkpoint: {update_checkpoint}")
print(f"📊 Schema: {update_schema}")

# COMMAND ----------

# DBTITLE 1,45. Leer stream de actualizaciones
# Stream de actualizaciones
df_updates = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", update_schema)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(update_source)
    .withColumn("fecha_actualizacion", current_timestamp())
)

print("✅ Stream de actualizaciones configurado")

# COMMAND ----------

# DBTITLE 1,46. Implementar foreachBatch con MERGE (UPSERT)
from delta.tables import DeltaTable
from pyspark.sql.functions import lit

# Contador de micro-lotes procesados
batch_counter = 0

def upsert_clientes(df_microlote, batch_id):
    """
    Función que se ejecuta para cada micro-lote.
    Realiza UPSERT: actualiza si existe, inserta si es nuevo.
    """
    global batch_counter
    batch_counter += 1
    
    print(f"\n🔄 Procesando micro-lote {batch_id} (#{batch_counter})")
    print(f"   Registros en micro-lote: {df_microlote.count()}")
    
    # Verificar si la tabla destino existe
    tabla_existe = spark.catalog.tableExists("bronze.clientes")
    
    if not tabla_existe:
        # Primera ejecución: crear tabla directamente
        print("   📝 Tabla no existe - creando con datos iniciales")
        df_microlote.write.format("delta").mode("overwrite").saveAsTable("bronze.clientes")
    else:
        # Tabla existe: hacer MERGE (UPSERT)
        print("   🔀 Ejecutando MERGE (UPSERT)")
        
        delta_table = DeltaTable.forName(spark, "bronze.clientes")
        
        # MERGE: actualiza si id existe, inserta si es nuevo
        (
            delta_table.alias("destino")
            .merge(
                df_microlote.alias("origen"),
                "destino.id = origen.id"  # Condición de match
            )
            .whenMatchedUpdateAll()  # Si existe: actualizar todas las columnas
            .whenNotMatchedInsertAll()  # Si no existe: insertar
            .execute()
        )
        
        print("   ✅ MERGE completado")
    
    # Mostrar resumen
    total_registros = spark.table("bronze.clientes").count()
    print(f"   📊 Total de registros en tabla: {total_registros}")

print("✅ Función upsert_clientes definida")

# COMMAND ----------

# DBTITLE 1,47. Iniciar stream con foreachBatch
# Iniciar stream con foreachBatch
query_upsert = (df_updates.writeStream
    .foreachBatch(upsert_clientes)  # Usar nuestra función personalizada
    .option("checkpointLocation", update_checkpoint)
    .trigger(processingTime="10 seconds")
    .start()
)

print("✅ Stream con foreachBatch iniciado")
print("\n⏳ Esperando... (el stream está activo pero no hay archivos aún)")

# COMMAND ----------

# DBTITLE 1,48. Ver estado inicial de la tabla
# MAGIC %sql
# MAGIC -- Estado actual de clientes (antes de actualizaciones)
# MAGIC SELECT 
# MAGIC     id, 
# MAGIC     nombre, 
# MAGIC     email,
# MAGIC     CASE 
# MAGIC         WHEN id <= 5 THEN '⭐ Registro original v1'
# MAGIC         WHEN id <= 10 THEN '⭐ Registro original v2'
# MAGIC         WHEN id <= 15 THEN '⭐ Registro original v3'
# MAGIC         ELSE '🆕 Nuevo'
# MAGIC     END as tipo
# MAGIC FROM bronze.clientes
# MAGIC ORDER BY id

# COMMAND ----------

# DBTITLE 1,49. COPIAR BATCH 1 - Trigger UPSERT
print("\n🚀 BATCH 1: Copiando actualizaciones...\n")

# Copiar batch 1
dbutils.fs.cp(
    f"file:{update_path_local}/clientes_update_batch1.csv",
    f"{update_source}/clientes_update_batch1.csv"
)

print("✅ Batch 1 copiado")
print("   - Actualizará: Ana (id=1), Laura (id=5)")
print("   - Insertará: Nuevo Cliente 1 (id=20)")
print("\n⏳ Esperando 15 segundos para procesamiento...")

import time
time.sleep(15)

print("\n✅ Batch 1 procesado")

# COMMAND ----------

# DBTITLE 1,50. Verificar UPSERT del Batch 1
# MAGIC %sql
# MAGIC -- Ver cambios después del UPSERT
# MAGIC -- Ana y Laura deben tener datos ACTUALIZADOS
# MAGIC -- Debe aparecer Nuevo Cliente 1 (id=20)
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     nombre,
# MAGIC     email,
# MAGIC     telefono,
# MAGIC     ciudad,
# MAGIC     ultima_compra,
# MAGIC     CASE 
# MAGIC         WHEN nombre LIKE '%ACTUALIZADO%' OR nombre LIKE '%VIP%' THEN '🔄 ACTUALIZADO'
# MAGIC         WHEN id >= 20 THEN '🆕 NUEVO (UPSERT)'
# MAGIC         ELSE '📌 Original sin cambios'
# MAGIC     END as estado
# MAGIC FROM bronze.clientes
# MAGIC WHERE id IN (1, 2, 3, 4, 5, 20)
# MAGIC ORDER BY id

# COMMAND ----------

# DBTITLE 1,51. COPIAR BATCH 2 - Segundo UPSERT
print("\n🚀 BATCH 2: Copiando más actualizaciones...\n")

# Copiar batch 2
dbutils.fs.cp(
    f"file:{update_path_local}/clientes_update_batch2.csv",
    f"{update_source}/clientes_update_batch2.csv"
)

print("✅ Batch 2 copiado")
print("   - Actualizará: Luis (id=2)")
print("   - Insertará: Nuevos Clientes (id=21,22)")
print("\n⏳ Esperando 15 segundos...")

time.sleep(15)

print("\n✅ Batch 2 procesado")

# COMMAND ----------

# DBTITLE 1,52. Ver resultado final del UPSERT
# MAGIC %sql
# MAGIC -- Estado final después de todos los UPSERTs
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     nombre,
# MAGIC     email,
# MAGIC     ciudad,
# MAGIC     ultima_compra,
# MAGIC     CASE 
# MAGIC         WHEN nombre LIKE '%ACTUALIZADO%' OR nombre LIKE '%PREMIUM%' OR nombre LIKE '%VIP%' THEN '🔄 ACTUALIZADO'
# MAGIC         WHEN id >= 20 THEN '🆕 INSERTADO'
# MAGIC         ELSE '📌 Original'
# MAGIC     END as estado
# MAGIC FROM bronze.clientes
# MAGIC ORDER BY id

# COMMAND ----------

# DBTITLE 1,53. Ejemplo 2: foreachBatch con Deduplicación
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🔍 Ejemplo 2: Deduplicación en Micro-lotes
# MAGIC
# MAGIC Caso común: archivos de ingesta contienen duplicados. Queremos mantener solo el registro más reciente por clave.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,54. Crear datos con duplicados
# Crear datos JSON con duplicados
import json
from datetime import datetime

duplicates_path_local = f"{base_path_local}/ventas_duplicadas"
os.makedirs(duplicates_path_local, exist_ok=True)

# Archivo con duplicados (mismo id, diferente timestamp)
ventas_dup = [
    {"id": 100, "cliente_id": 1, "monto": 150.00, "timestamp": "2024-02-01 10:00:00"},  # Primera versión
    {"id": 100, "cliente_id": 1, "monto": 155.00, "timestamp": "2024-02-01 10:05:00"},  # Actualización (más reciente)
    {"id": 101, "cliente_id": 2, "monto": 200.00, "timestamp": "2024-02-01 11:00:00"},
    {"id": 101, "cliente_id": 2, "monto": 210.00, "timestamp": "2024-02-01 11:10:00"},  # Más reciente
    {"id": 102, "cliente_id": 3, "monto": 300.00, "timestamp": "2024-02-01 12:00:00"},  # Sin duplicados
]

with open(f"{duplicates_path_local}/ventas_dup_batch1.json", "w") as f:
    for venta in ventas_dup:
        f.write(json.dumps(venta) + "\n")

print("✅ Creado archivo con duplicados")
print("   - id=100 aparece 2 veces (queremos la más reciente)")
print("   - id=101 aparece 2 veces (queremos la más reciente)")
print("   - id=102 aparece 1 vez (sin duplicados)")

# COMMAND ----------

# DBTITLE 1,55. Stream con deduplicación en foreachBatch
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Configuración
dup_source = f"{base_path_volumes}/ventas_duplicadas"
dup_checkpoint = f"{checkpoint_path}/ventas_dedup"
dup_schema = f"{schema_path}/ventas_dedup"

dbutils.fs.rm(dup_checkpoint, True)
dbutils.fs.rm(dup_schema, True)
dbutils.fs.mkdirs(dup_source)

# Limpiar tabla anterior
spark.sql("DROP TABLE IF EXISTS bronze.ventas_dedup")

# Stream
df_ventas_dup = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", dup_schema)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(dup_source)
)

def dedup_and_write(df_batch, batch_id):
    """
    Deduplicar micro-lote: mantener solo el registro más reciente por id.
    """
    print(f"\n🔍 Deduplicando micro-lote {batch_id}")
    print(f"   Registros antes de dedup: {df_batch.count()}")
    
    # Crear ventana para particionar por 'id' y ordenar por 'timestamp' DESC
    window_spec = Window.partitionBy("id").orderBy(col("timestamp").desc())
    
    # Agregar número de fila (1 = más reciente)
    df_with_rank = df_batch.withColumn("rank", row_number().over(window_spec))
    
    # Filtrar solo rank=1 (el más reciente)
    df_dedup = df_with_rank.filter(col("rank") == 1).drop("rank")
    
    registros_dedup = df_dedup.count()
    duplicados_removidos = df_batch.count() - registros_dedup
    
    print(f"   Registros después de dedup: {registros_dedup}")
    print(f"   ❌ Duplicados removidos: {duplicados_removidos}")
    
    # Escribir resultados deduplicados
    df_dedup.write.format("delta").mode("append").saveAsTable("bronze.ventas_dedup")
    
    print("   ✅ Escritura completada")

print("✅ Función de deduplicación definida")

# COMMAND ----------

# DBTITLE 1,56. Iniciar stream con deduplicación
# Detener stream anterior
try:
    query_upsert.stop()
except:
    pass

# Iniciar nuevo stream
query_dedup = (df_ventas_dup.writeStream
    .foreachBatch(dedup_and_write)
    .option("checkpointLocation", dup_checkpoint)
    .trigger(processingTime="10 seconds")
    .start()
)

print("✅ Stream de deduplicación iniciado")

# COMMAND ----------

# DBTITLE 1,57. Copiar archivo con duplicados
print("\n📥 Copiando archivo con duplicados...\n")

dbutils.fs.cp(
    f"file:{duplicates_path_local}/ventas_dup_batch1.json",
    f"{dup_source}/ventas_dup_batch1.json"
)

print("✅ Archivo copiado (contiene 5 registros, 4 duplicados)")
print("\n⏳ Esperando 15 segundos...")

time.sleep(15)

print("\n✅ Procesamiento completado")

# COMMAND ----------

# DBTITLE 1,58. Verificar resultados de deduplicación
# MAGIC %sql
# MAGIC -- Resultado: solo 3 registros únicos (uno por id)
# MAGIC -- Para id=100 y id=101 debe aparecer solo el registro más reciente
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     cliente_id,
# MAGIC     monto,
# MAGIC     timestamp,
# MAGIC     CASE 
# MAGIC         WHEN id = 100 AND monto = 155.00 THEN '✅ Correcto (versión más reciente)'
# MAGIC         WHEN id = 100 AND monto = 150.00 THEN '❌ ERROR (versión antigua)'
# MAGIC         WHEN id = 101 AND monto = 210.00 THEN '✅ Correcto (versión más reciente)'
# MAGIC         WHEN id = 101 AND monto = 200.00 THEN '❌ ERROR (versión antigua)'
# MAGIC         ELSE '✅ Sin duplicados'
# MAGIC     END as validacion
# MAGIC FROM bronze.ventas_dedup
# MAGIC ORDER BY id, timestamp

# COMMAND ----------

# DBTITLE 1,59. Comparación: CON vs SIN deduplicación
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Comparación: Resultados
# MAGIC
# MAGIC ### Sin deduplicación:
# MAGIC ```
# MAGIC id=100: 2 registros (monto: 150.00 y 155.00)
# MAGIC id=101: 2 registros (monto: 200.00 y 210.00)
# MAGIC id=102: 1 registro  (monto: 300.00)
# MAGIC Total: 5 registros
# MAGIC ```
# MAGIC
# MAGIC ### Con deduplicación (foreachBatch):
# MAGIC ```
# MAGIC id=100: 1 registro (monto: 155.00 - el más reciente)
# MAGIC id=101: 1 registro (monto: 210.00 - el más reciente)
# MAGIC id=102: 1 registro (monto: 300.00)
# MAGIC Total: 3 registros únicos
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,60. Detener todos los streams finales
print("\n🛑 Deteniendo todos los streams...\n")

for stream in spark.streams.active:
    print(f"Deteniendo: {stream.id}")
    stream.stop()

print("\n✅ Todos los streams detenidos")
print(f"\n📊 Total de micro-lotes procesados (UPSERT): {batch_counter}")

# COMMAND ----------

# DBTITLE 1,Resumen foreachBatch
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # 📝 Resumen: foreachBatch
# MAGIC
# MAGIC ## ¿Cuándo usar foreachBatch?
# MAGIC
# MAGIC ### ✅ Casos de uso ideales:
# MAGIC 1. **UPSERT/MERGE** - Actualizar registros existentes o insertar nuevos
# MAGIC 2. **Deduplicación** - Mantener solo registros únicos por clave
# MAGIC 3. **Validación compleja** - Lógica de negocio que requiere todo el micro-lote
# MAGIC 4. **Escritura múltiple** - Escribir a varios destinos (Delta, PostgreSQL, S3)
# MAGIC 5. **Enriquecimiento** - Joins complejos o lookups contra otras tablas
# MAGIC 6. **Logging/Auditoría** - Registrar métricas por cada micro-lote
# MAGIC
# MAGIC ### ❌ Cuándo NO usar foreachBatch:
# MAGIC - Transformaciones simples que pueden hacerse con `.select()`, `.filter()`
# MAGIC - Escritura directa a una sola tabla Delta (usar `.table()` directamente)
# MAGIC - Operaciones que no necesitan ver el micro-lote completo
# MAGIC
# MAGIC ## Diferencias clave:
# MAGIC
# MAGIC ```python
# MAGIC # OPCIÓN 1: Escritura directa (más simple)
# MAGIC df.writeStream.table("mi_tabla")  # ✅ Recomendado para casos simples
# MAGIC
# MAGIC # OPCIÓN 2: foreachBatch (más control)
# MAGIC df.writeStream.foreachBatch(mi_funcion)  # ✅ Para lógica compleja
# MAGIC ```
# MAGIC
# MAGIC ## Ventajas de foreachBatch:
# MAGIC - ✅ Control total sobre cada micro-lote
# MAGIC - ✅ Acceso a la API completa de DataFrames (no solo streaming)
# MAGIC - ✅ Puedes hacer operaciones que no están disponibles en streaming (MERGE, múltiples writes)
# MAGIC - ✅ Manejo de errores personalizado
# MAGIC
# MAGIC ## Desventajas:
# MAGIC - ⚠️ Más código para mantener
# MAGIC - ⚠️ Mayor responsabilidad (manejo de errores, idempotencia)
# MAGIC - ⚠️ No hay garantías automáticas de "exactly-once" (debes implementarlas)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Patrón recomendado:
# MAGIC
# MAGIC ```python
# MAGIC def procesar_microlote(df_batch, batch_id):
# MAGIC     # 1. Validar/transformar
# MAGIC     df_limpio = validar_datos(df_batch)
# MAGIC     
# MAGIC     # 2. Deduplicar si es necesario
# MAGIC     df_dedup = deduplicar(df_limpio)
# MAGIC     
# MAGIC     # 3. Enriquecer con datos externos
# MAGIC     df_enriquecido = enriquecer(df_dedup)
# MAGIC     
# MAGIC     # 4. Hacer UPSERT con MERGE
# MAGIC     hacer_merge(df_enriquecido, "mi_tabla")
# MAGIC     
# MAGIC     # 5. Log de métricas
# MAGIC     registrar_metricas(batch_id, df_batch.count())
# MAGIC
# MAGIC df.writeStream.foreachBatch(procesar_microlote).start()
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Sección 3: Auto Loader con Parquet
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # 📦 PARTE 3: Auto Loader con Parquet (Productos)
# MAGIC
# MAGIC Formato columnar, ideal para producción (comprimido, rápido, con metadata).
# MAGIC
# MAGIC **Schema inicial (v1):** id, nombre, precio  
# MAGIC **Schema v2:** + categoria  
# MAGIC **Schema v3:** + stock, proveedor
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,29. Detener stream JSON
# Detener el stream de ventas
try:
    query_ventas.stop()
    print("✅ Stream de ventas detenido")
except:
    print("⚠️ Stream ya estaba detenido")

# COMMAND ----------

# DBTITLE 1,30. Configurar Auto Loader para Parquet
# Configuración para Parquet
parquet_source = f"{base_path_volumes}/productos"
parquet_checkpoint = f"{checkpoint_path}/productos_parquet"
parquet_schema = f"{schema_path}/productos_parquet"

# Limpiar checkpoint y schema previos
dbutils.fs.rm(parquet_checkpoint, True)
dbutils.fs.rm(parquet_schema, True)

print(f"📁 Fuente: {parquet_source}")
print(f"💾 Checkpoint: {parquet_checkpoint}")
print(f"📊 Schema: {parquet_schema}")

# COMMAND ----------

# DBTITLE 1,31. Iniciar stream Auto Loader - Parquet
# Configurar Auto Loader para Parquet
df_productos = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", parquet_schema)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(parquet_source)
    .withColumn("fecha_ingesta", current_timestamp())
    .withColumn("archivo_origen", input_file_name())
)

print("✅ Stream Parquet configurado")
print("\nSchema inferido:")
df_productos.printSchema()

# COMMAND ----------

# DBTITLE 1,32. Escribir a tabla Delta - Parquet
# Limpiar tabla anterior
spark.sql("DROP TABLE IF EXISTS bronze.productos")

# Iniciar escritura
query_productos = (df_productos.writeStream
    .format("delta")
    .option("checkpointLocation", parquet_checkpoint)
    .option("mergeSchema", "true")
    .trigger(processingTime="10 seconds")
    .table("bronze.productos")
)

print("✅ Stream Parquet iniciado - tabla bronze.productos")
print("\n⏳ Esperando 15 segundos...")

time.sleep(15)

print("\n✅ Procesamiento inicial completado")

# COMMAND ----------

# DBTITLE 1,33. Verificar datos iniciales - Parquet
# MAGIC %sql
# MAGIC -- Verificar productos cargados (v1)
# MAGIC SELECT * FROM bronze.productos ORDER BY id

# COMMAND ----------

# DBTITLE 1,34. SCHEMA EVOLUTION - Copiar Parquet v2
print("\n🔄 EVOLUCIÓN PARQUET - Agregando columna 'categoria'\n")

# Copiar archivos v2 (Parquet es directorio)
for file_info in dbutils.fs.ls("file:/Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader/productos/productos_v2.parquet"):
    if file_info.name.endswith(".parquet"):
        dbutils.fs.cp(
            file_info.path,
            f"{base_path_volumes}/productos/v2_{file_info.name}"
        )

print("✅ Copiado productos_v2.parquet (incluye 'categoria')")
print("\n⏳ Esperando 15 segundos...")

time.sleep(15)

print("\n✅ Evolución detectada")

# COMMAND ----------

# DBTITLE 1,35. Verificar nueva columna - Parquet
# MAGIC %sql
# MAGIC -- Ver columna 'categoria' agregada
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     nombre,
# MAGIC     precio,
# MAGIC     categoria,  -- Nueva columna (NULL en registros antiguos)
# MAGIC     fecha_ingesta
# MAGIC FROM bronze.productos
# MAGIC ORDER BY id

# COMMAND ----------

# DBTITLE 1,36. SEGUNDA EVOLUCIÓN - Copiar Parquet v3
print("\n🔄 SEGUNDA EVOLUCIÓN PARQUET - Agregando 'stock' y 'proveedor'\n")

# Copiar archivos v3
for file_info in dbutils.fs.ls("file:/Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader/productos/productos_v3.parquet"):
    if file_info.name.endswith(".parquet"):
        dbutils.fs.cp(
            file_info.path,
            f"{base_path_volumes}/productos/v3_{file_info.name}"
        )

print("✅ Copiado productos_v3.parquet (incluye 'stock' y 'proveedor')")
print("\n⏳ Esperando 15 segundos...")

time.sleep(15)

print("\n✅ Segunda evolución completada")

# COMMAND ----------

# DBTITLE 1,37. Schema completo final - Parquet
# MAGIC %sql
# MAGIC -- Ver todas las columnas evolucionadas
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     nombre,
# MAGIC     precio,
# MAGIC     categoria,   -- NULL en v1
# MAGIC     stock,       -- NULL en v1 y v2
# MAGIC     proveedor,   -- NULL en v1 y v2
# MAGIC     fecha_ingesta
# MAGIC FROM bronze.productos
# MAGIC ORDER BY id

# COMMAND ----------

# DBTITLE 1,Sección 4: Monitoreo y Métricas
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # 📊 PARTE 4: Monitoreo y Métricas
# MAGIC
# MAGIC Verificamos el estado de los streams y analizamos métricas de procesamiento.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,38. Ver métricas del stream de productos
import json

# Ver último progreso del stream
if query_productos.lastProgress:
    print("📊 Métricas del último micro-lote:\n")
    print(json.dumps(query_productos.lastProgress, indent=2))
else:
    print("⚠️ No hay progreso aún")

# COMMAND ----------

# DBTITLE 1,39. Estado de todos los streams
# Ver estado de todos los streams activos
print("🔍 Streams activos en este notebook:\n")

for stream in spark.streams.active:
    print(f"Stream ID: {stream.id}")
    print(f"Nombre: {stream.name}")
    print(f"Estado: {stream.status}")
    print("-" * 50)

# COMMAND ----------

# DBTITLE 1,40. Detener todos los streams
# Detener todos los streams
print("\n🛑 Deteniendo todos los streams...\n")

for stream in spark.streams.active:
    print(f"Deteniendo: {stream.name or stream.id}")
    stream.stop()
    
print("\n✅ Todos los streams detenidos")

# COMMAND ----------

# DBTITLE 1,41. Resumen de tablas creadas
# MAGIC %sql
# MAGIC -- Ver todas las tablas creadas
# MAGIC SHOW TABLES IN bronze

# COMMAND ----------

# DBTITLE 1,42. Estadísticas de cada tabla
# MAGIC %sql
# MAGIC -- Contar registros en cada tabla
# MAGIC SELECT 'clientes' as tabla, COUNT(*) as registros FROM bronze.clientes
# MAGIC UNION ALL
# MAGIC SELECT 'ventas' as tabla, COUNT(*) as registros FROM bronze.ventas
# MAGIC UNION ALL
# MAGIC SELECT 'productos' as tabla, COUNT(*) as registros FROM bronze.productos

# COMMAND ----------

# DBTITLE 1,Resumen y Conclusiones
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # ✅ Resumen de la Práctica
# MAGIC
# MAGIC ## Lo que aprendimos:
# MAGIC
# MAGIC ### 1. **Auto Loader (`cloudFiles`)**
# MAGIC - Ingesta incremental automática de archivos nuevos
# MAGIC - Funciona con múltiples formatos: CSV, JSON, Parquet
# MAGIC - Detecta archivos sin necesidad de listarlos manualmente
# MAGIC - Escalable a millones de archivos
# MAGIC
# MAGIC ### 2. **Schema Evolution**
# MAGIC - Auto Loader infiere el schema inicial automáticamente
# MAGIC - Detecta cambios en el schema cuando llegan archivos nuevos
# MAGIC - Agrega columnas nuevas a la tabla destino
# MAGIC - Llena con `NULL` los valores faltantes en registros antiguos
# MAGIC - Modo `addNewColumns`: acepta columnas nuevas sin fallar
# MAGIC
# MAGIC ### 3. **Configuraciones Clave**
# MAGIC ```python
# MAGIC .option("cloudFiles.format", "json")           # Formato de archivo
# MAGIC .option("cloudFiles.schemaLocation", path)    # Dónde guardar schema
# MAGIC .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Aceptar cambios
# MAGIC .option("cloudFiles.inferColumnTypes", "true") # Inferir tipos
# MAGIC .option("checkpointLocation", path)            # Checkpoint (obligatorio)
# MAGIC .option("mergeSchema", "true")                 # Permitir cambios en Delta
# MAGIC ```
# MAGIC
# MAGIC ### 4. **Buenas Prácticas**
# MAGIC - ✅ Usar rutas persistentes en Volumes para checkpoints y schemas
# MAGIC - ✅ Particionar tablas por columnas de baja cardinalidad (año, mes)
# MAGIC - ✅ Agregar metadata de ingesta (fecha, archivo origen)
# MAGIC - ✅ Monitorear métricas del stream regularmente
# MAGIC - ✅ Detener streams correctamente cuando no se necesitan
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Ejercicios Adicionales Sugeridos:
# MAGIC
# MAGIC 1. **Modo Rescue**: Configura `schemaEvolutionMode = "rescue"` y observa cómo se capturan columnas inesperadas en `_rescued_data`
# MAGIC
# MAGIC 2. **Manejo de Errores**: Agrega archivos corruptos (JSON mal formado) y separa registros válidos de erróneos usando `_corrupt_record`
# MAGIC
# MAGIC 3. **Optimización**: Usa `OPTIMIZE` y `Z-ORDER` en las tablas Delta para mejorar rendimiento de queries
# MAGIC
# MAGIC 4. **Watermarks**: Experimenta con `withWatermark()` para manejar datos tardíos (late data)
# MAGIC
# MAGIC 5. **foreachBatch**: Implementa lógica personalizada usando `foreachBatch` para UPSERTS con Delta MERGE
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **📚 Documentación:**
# MAGIC - [Auto Loader - Databricks](https://docs.databricks.com/ingestion/auto-loader/index.html)
# MAGIC - [Schema Evolution](https://docs.databricks.com/ingestion/auto-loader/schema.html)
# MAGIC - [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

# COMMAND ----------

# DBTITLE 1,Sección 2: Auto Loader con JSON
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # 📋 PARTE 2: Auto Loader con JSON (Ventas)
# MAGIC
# MAGIC Ahora trabajaremos con archivos JSON (formato común para APIs y logs).
# MAGIC
# MAGIC **Schema inicial (v1):** id, cliente_id, monto, fecha  
# MAGIC **Schema v2:** + producto  
# MAGIC **Schema v3:** + descuento, metodo_pago
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,18. Detener stream CSV anterior
# Detener el stream de clientes antes de iniciar el siguiente
try:
    query_clientes.stop()
    print("✅ Stream de clientes detenido")
except:
    print("⚠️ Stream ya estaba detenido o no existía")

# COMMAND ----------

# DBTITLE 1,19. Configurar Auto Loader para JSON
# Configuración para JSON
json_source = f"{base_path_volumes}/ventas"
json_checkpoint = f"{checkpoint_path}/ventas_json"
json_schema = f"{schema_path}/ventas_json"

# Limpiar checkpoint y schema previos
dbutils.fs.rm(json_checkpoint, True)
dbutils.fs.rm(json_schema, True)

print(f"📁 Fuente: {json_source}")
print(f"💾 Checkpoint: {json_checkpoint}")
print(f"📊 Schema: {json_schema}")

# COMMAND ----------

# DBTITLE 1,20. Iniciar stream Auto Loader - JSON
from pyspark.sql.functions import year, month, to_date

# Configurar Auto Loader para JSON
df_ventas = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", json_schema)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(json_source)
    .withColumn("fecha_ingesta", current_timestamp())
    .withColumn("archivo_origen", input_file_name())
    .withColumn("fecha_date", to_date(col("fecha")))
    .withColumn("año", year(col("fecha_date")))
    .withColumn("mes", month(col("fecha_date")))
)

print("✅ Stream JSON configurado")
print("\nSchema inferido:")
df_ventas.printSchema()

# COMMAND ----------

# DBTITLE 1,21. Escribir a tabla Delta particionada - JSON
# Limpiar tabla anterior
spark.sql("DROP TABLE IF EXISTS bronze.ventas")

# Iniciar escritura con particionado
query_ventas = (df_ventas.writeStream
    .format("delta")
    .option("checkpointLocation", json_checkpoint)
    .option("mergeSchema", "true")
    .partitionBy("año", "mes")  # Particionar para optimizar queries temporales
    .trigger(processingTime="10 seconds")
    .table("bronze.ventas")
)

print("✅ Stream JSON iniciado - tabla bronze.ventas (particionada)")
print("\n⏳ Esperando 15 segundos...")

time.sleep(15)

print("\n✅ Procesamiento inicial completado")

# COMMAND ----------

# DBTITLE 1,22. Verificar datos iniciales - JSON
# MAGIC %sql
# MAGIC -- Verificar ventas cargadas (v1)
# MAGIC SELECT id, cliente_id, monto, fecha, archivo_origen 
# MAGIC FROM bronze.ventas 
# MAGIC ORDER BY id

# COMMAND ----------

# DBTITLE 1,23. Ver particiones creadas
# MAGIC %sql
# MAGIC -- Ver las particiones físicas creadas
# MAGIC SHOW PARTITIONS bronze.ventas

# COMMAND ----------

# DBTITLE 1,24. SCHEMA EVOLUTION - Copiar JSON v2
print("\n🔄 EVOLUCIÓN JSON - Agregando campo 'producto'\n")

# Copiar archivo v2 (con campo "producto")
dbutils.fs.cp(
    f"file:/Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader/ventas/ventas_v2.json",
    f"{base_path_volumes}/ventas/ventas_v2.json"
)

print("✅ Copiado ventas_v2.json (incluye campo 'producto')")
print("\n⏳ Esperando 15 segundos...")

time.sleep(15)

print("\n✅ Evolución detectada")

# COMMAND ----------

# DBTITLE 1,25. Verificar nueva columna - JSON
# MAGIC %sql
# MAGIC -- Ver campo 'producto' agregado
# MAGIC SELECT 
# MAGIC     id, 
# MAGIC     cliente_id, 
# MAGIC     monto, 
# MAGIC     fecha,
# MAGIC     producto,  -- Nueva columna (NULL en registros antiguos)
# MAGIC     fecha_ingesta
# MAGIC FROM bronze.ventas
# MAGIC ORDER BY id

# COMMAND ----------

# DBTITLE 1,26. SEGUNDA EVOLUCIÓN - Copiar JSON v3
print("\n🔄 SEGUNDA EVOLUCIÓN JSON - Agregando 'descuento' y 'metodo_pago'\n")

# Copiar archivo v3
dbutils.fs.cp(
    f"file:/Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader/ventas/ventas_v3.json",
    f"{base_path_volumes}/ventas/ventas_v3.json"
)

print("✅ Copiado ventas_v3.json (incluye 'descuento' y 'metodo_pago')")
print("\n⏳ Esperando 15 segundos...")

time.sleep(15)

print("\n✅ Segunda evolución completada")

# COMMAND ----------

# DBTITLE 1,27. Ver schema completo evolucionado - JSON
# MAGIC %sql
# MAGIC -- Schema final con todas las evoluciones
# MAGIC DESCRIBE EXTENDED bronze.ventas

# COMMAND ----------

# DBTITLE 1,28. Análisis de ventas con schema evolucionado
# MAGIC %sql
# MAGIC -- Análisis completo mostrando evolución del schema
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     cliente_id,
# MAGIC     monto,
# MAGIC     producto,       -- NULL en v1
# MAGIC     descuento,      -- NULL en v1 y v2
# MAGIC     metodo_pago,    -- NULL en v1 y v2
# MAGIC     fecha,
# MAGIC     CASE 
# MAGIC         WHEN producto IS NULL THEN 'v1 (schema inicial)'
# MAGIC         WHEN descuento IS NULL THEN 'v2 (+ producto)'
# MAGIC         ELSE 'v3 (+ descuento, metodo_pago)'
# MAGIC     END as version_schema
# MAGIC FROM bronze.ventas
# MAGIC ORDER BY id

# COMMAND ----------

# DBTITLE 1,7. CARGA INICIAL - Copiar archivos V1
# Copiar solo archivos v1 (schema inicial)
print("\n🚀 CARGA INICIAL - Archivos con schema base (v1)\n")

# CSV - Clientes v1
dbutils.fs.cp(
    f"file:/Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader/clientes/clientes_v1.csv",
    f"{base_path_volumes}/clientes/clientes_v1.csv"
)
print("✅ Copiado clientes_v1.csv")

# JSON - Ventas v1
dbutils.fs.cp(
    f"file:/Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader/ventas/ventas_v1.json",
    f"{base_path_volumes}/ventas/ventas_v1.json"
)
print("✅ Copiado ventas_v1.json")

# Parquet - Productos v1
for file_info in dbutils.fs.ls("file:/Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader/productos/productos_v1.parquet"):
    if file_info.name.endswith(".parquet"):
        dbutils.fs.cp(
            file_info.path,
            f"{base_path_volumes}/productos/{file_info.name}"
        )
print("✅ Copiado productos_v1.parquet")

print("\n✅ Archivos v1 listos para ingesta incremental")

# COMMAND ----------

# DBTITLE 1,Sección 1: Auto Loader con CSV
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # 📄 PARTE 1: Auto Loader con CSV (Clientes)
# MAGIC
# MAGIC Vamos a ingestar archivos CSV con Auto Loader y observar schema evolution cuando lleguen archivos con columnas nuevas.
# MAGIC
# MAGIC **Schema inicial (v1):** id, nombre, email  
# MAGIC **Schema v2:** + telefono  
# MAGIC **Schema v3:** + ciudad
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,8. Configurar Auto Loader para CSV
from pyspark.sql.functions import col, current_timestamp, input_file_name

# Configuración para CSV
csv_source = f"{base_path_volumes}/clientes"
csv_checkpoint = f"{checkpoint_path}/clientes_csv"
csv_schema = f"{schema_path}/clientes_csv"

print(f"📁 Fuente: {csv_source}")
print(f"💾 Checkpoint: {csv_checkpoint}")
print(f"📊 Schema: {csv_schema}")

# COMMAND ----------

# DBTITLE 1,9. Iniciar stream Auto Loader - CSV
# Limpiar checkpoint y schema previos (solo para demo)
dbutils.fs.rm(csv_checkpoint, True)
dbutils.fs.rm(csv_schema, True)

print("🧹 Limpieza completada - iniciando desde cero")

# Configurar Auto Loader para CSV
df_clientes = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", csv_schema)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Clave para schema evolution
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(csv_source)
    .withColumn("fecha_ingesta", current_timestamp())
    .withColumn("archivo_origen", input_file_name())
)

print("✅ Stream configurado para CSV")
print("\nSchema inferido inicial:")
df_clientes.printSchema()

# COMMAND ----------

# DBTITLE 1,10. Escribir a tabla Delta - CSV
# Crear tabla Delta si no existe
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Limpiar tabla anterior (solo para demo)
spark.sql("DROP TABLE IF EXISTS bronze.clientes")

# Iniciar escritura streaming
query_clientes = (df_clientes.writeStream
    .format("delta")
    .option("checkpointLocation", csv_checkpoint)
    .option("mergeSchema", "true")  # Permitir evolución de schema en Delta
    .trigger(processingTime="10 seconds")  # Procesar cada 10 segundos
    .table("bronze.clientes")
)

print("✅ Stream iniciado - tabla bronze.clientes")
print("\n⏳ Esperando 15 segundos para procesamiento inicial...")

import time
time.sleep(15)

print("\n✅ Procesamiento inicial completado")

# COMMAND ----------

# DBTITLE 1,11. Verificar datos iniciales - CSV
# MAGIC %sql
# MAGIC -- Verificar datos cargados (v1)
# MAGIC SELECT * FROM bronze.clientes ORDER BY id

# COMMAND ----------

# DBTITLE 1,12. Ver schema actual - CSV
# MAGIC %sql
# MAGIC -- Ver schema actual de la tabla
# MAGIC DESCRIBE EXTENDED bronze.clientes

# COMMAND ----------

# DBTITLE 1,13. SIMULAR SCHEMA EVOLUTION - Copiar CSV v2
print("\n🔄 SCHEMA EVOLUTION - Agregando archivos con columna nueva\n")

# Copiar archivo v2 (con columna "telefono")
dbutils.fs.cp(
    f"file:/Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader/clientes/clientes_v2.csv",
    f"{base_path_volumes}/clientes/clientes_v2.csv"
)

print("✅ Copiado clientes_v2.csv (incluye columna 'telefono')")
print("\n⏳ Esperando 15 segundos para que Auto Loader detecte el cambio...")

time.sleep(15)

print("\n✅ Auto Loader debió detectar y procesar el nuevo schema")

# COMMAND ----------

# DBTITLE 1,14. Verificar SCHEMA EVOLUTION - CSV
# MAGIC %sql
# MAGIC -- Verificar que se agregó la columna "telefono"
# MAGIC -- Los registros antiguos deben tener NULL en telefono
# MAGIC SELECT 
# MAGIC     id, 
# MAGIC     nombre, 
# MAGIC     email, 
# MAGIC     telefono,  -- Nueva columna
# MAGIC     fecha_ingesta
# MAGIC FROM bronze.clientes 
# MAGIC ORDER BY id

# COMMAND ----------

# DBTITLE 1,15. SEGUNDA EVOLUCIÓN - Copiar CSV v3
print("\n🔄 SEGUNDA EVOLUCIÓN - Agregando archivo con otra columna\n")

# Copiar archivo v3 (con columna "ciudad")
dbutils.fs.cp(
    f"file:/Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader/clientes/clientes_v3.csv",
    f"{base_path_volumes}/clientes/clientes_v3.csv"
)

print("✅ Copiado clientes_v3.csv (incluye columna 'ciudad')")
print("\n⏳ Esperando 15 segundos...")

time.sleep(15)

print("\n✅ Segunda evolución completada")

# COMMAND ----------

# DBTITLE 1,16. Verificar ESQUEMA FINAL - CSV
# MAGIC %sql
# MAGIC -- Ver schema completo con todas las evoluciones
# MAGIC DESCRIBE EXTENDED bronze.clientes

# COMMAND ----------

# DBTITLE 1,17. Ver todos los datos con schema evolucionado
# MAGIC %sql
# MAGIC -- Observar cómo los registros antiguos tienen NULL en columnas nuevas
# MAGIC SELECT 
# MAGIC     id,
# MAGIC     nombre,
# MAGIC     email,
# MAGIC     telefono,    -- NULL para registros v1
# MAGIC     ciudad,      -- NULL para registros v1 y v2
# MAGIC     archivo_origen,
# MAGIC     fecha_ingesta
# MAGIC FROM bronze.clientes
# MAGIC ORDER BY id

# COMMAND ----------

# DBTITLE 1,Configuración inicial
# MAGIC %md
# MAGIC # Práctica: Auto Loader, Schema Evolution y Cloud Files
# MAGIC
# MAGIC **Objetivo:** Implementar pipelines de ingesta con Auto Loader, demostrando schema evolution en diferentes formatos de archivo (CSV, JSON, Parquet).
# MAGIC
# MAGIC ## Estructura del ejercicio:
# MAGIC 1. Crear datos de ejemplo con diferentes versiones (schema evolution)
# MAGIC 2. Copiar datos a Volumes para ingesta
# MAGIC 3. Implementar Auto Loader para cada formato
# MAGIC 4. Observar schema evolution en acción
# MAGIC 5. Monitorear y troubleshoot
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,1. Configurar rutas y limpiar ambiente
# Configuración de rutas
import shutil
import os

# Rutas locales para crear archivos de ejemplo
base_path_local = "/Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader"

# Rutas en Volumes para ingesta
base_path_volumes = "/Volumes/workspace/default/tempo/autoloader"
checkpoint_path = "/Volumes/workspace/default/checkpoint"
schema_path = "/Volumes/workspace/default/checkpoint/schemas"

# Crear carpetas locales si no existen
for carpeta in ["clientes", "ventas", "productos"]:
    os.makedirs(f"{base_path_local}/{carpeta}", exist_ok=True)

print("✅ Rutas configuradas")
print(f"Datos locales: {base_path_local}")
print(f"Ingesta desde: {base_path_volumes}")
print(f"Checkpoints en: {checkpoint_path}")

# COMMAND ----------

# DBTITLE 1,2. Crear archivos CSV - Clientes (con schema evolution)
# VERSIÓN 1: Schema básico (id, nombre, email)
csv_v1 = """id,nombre,email
1,Ana García,ana@example.com
2,Luis Martínez,luis@example.com
3,María López,maria@example.com
4,Carlos Ruiz,carlos@example.com
5,Laura Torres,laura@example.com"""

with open(f"{base_path_local}/clientes/clientes_v1.csv", "w") as f:
    f.write(csv_v1)

print("✅ Creado clientes_v1.csv (3 columnas: id, nombre, email)")

# VERSIÓN 2: Schema con columna nueva (telefono)
csv_v2 = """id,nombre,email,telefono
6,Pedro Sánchez,pedro@example.com,555-0001
7,Sofia Morales,sofia@example.com,555-0002
8,Diego Castro,diego@example.com,555-0003
9,Carmen Silva,carmen@example.com,555-0004
10,Roberto Vega,roberto@example.com,555-0005"""

with open(f"{base_path_local}/clientes/clientes_v2.csv", "w") as f:
    f.write(csv_v2)

print("✅ Creado clientes_v2.csv (4 columnas: + telefono) - SCHEMA EVOLUTION")

# VERSIÓN 3: Schema con otra columna nueva (ciudad)
csv_v3 = """id,nombre,email,telefono,ciudad
11,Isabel Romero,isabel@example.com,555-0006,Madrid
12,Miguel Ángel,miguel@example.com,555-0007,Barcelona
13,Patricia Núñez,patricia@example.com,555-0008,Valencia
14,Javier Ortiz,javier@example.com,555-0009,Sevilla
15,Elena Fernández,elena@example.com,555-0010,Bilbao"""

with open(f"{base_path_local}/clientes/clientes_v3.csv", "w") as f:
    f.write(csv_v3)

print("✅ Creado clientes_v3.csv (5 columnas: + ciudad) - SCHEMA EVOLUTION 2")

# COMMAND ----------

# DBTITLE 1,3. Crear archivos JSON - Ventas (con schema evolution)
import json
from datetime import datetime, timedelta

# VERSIÓN 1: Schema básico (id, cliente_id, monto, fecha)
ventas_v1 = [
    {"id": 1, "cliente_id": 1, "monto": 150.50, "fecha": "2024-01-15"},
    {"id": 2, "cliente_id": 2, "monto": 320.00, "fecha": "2024-01-16"},
    {"id": 3, "cliente_id": 3, "monto": 89.99, "fecha": "2024-01-17"},
    {"id": 4, "cliente_id": 1, "monto": 450.75, "fecha": "2024-01-18"},
    {"id": 5, "cliente_id": 4, "monto": 210.00, "fecha": "2024-01-19"},
]

with open(f"{base_path_local}/ventas/ventas_v1.json", "w") as f:
    for venta in ventas_v1:
        f.write(json.dumps(venta) + "\n")

print("✅ Creado ventas_v1.json (4 campos: id, cliente_id, monto, fecha)")

# VERSIÓN 2: Schema con campo nuevo (producto)
ventas_v2 = [
    {"id": 6, "cliente_id": 5, "monto": 599.99, "fecha": "2024-01-20", "producto": "Laptop"},
    {"id": 7, "cliente_id": 6, "monto": 125.50, "fecha": "2024-01-21", "producto": "Teclado"},
    {"id": 8, "cliente_id": 7, "monto": 89.99, "fecha": "2024-01-22", "producto": "Mouse"},
    {"id": 9, "cliente_id": 2, "monto": 1200.00, "fecha": "2024-01-23", "producto": "Monitor"},
    {"id": 10, "cliente_id": 8, "monto": 45.00, "fecha": "2024-01-24", "producto": "Cable HDMI"},
]

with open(f"{base_path_local}/ventas/ventas_v2.json", "w") as f:
    for venta in ventas_v2:
        f.write(json.dumps(venta) + "\n")

print("✅ Creado ventas_v2.json (5 campos: + producto) - SCHEMA EVOLUTION")

# VERSIÓN 3: Schema con campos adicionales (descuento, metodo_pago)
ventas_v3 = [
    {"id": 11, "cliente_id": 9, "monto": 350.00, "fecha": "2024-01-25", "producto": "Auriculares", "descuento": 10.0, "metodo_pago": "tarjeta"},
    {"id": 12, "cliente_id": 10, "monto": 799.99, "fecha": "2024-01-26", "producto": "Tablet", "descuento": 0.0, "metodo_pago": "efectivo"},
    {"id": 13, "cliente_id": 3, "monto": 199.99, "fecha": "2024-01-27", "producto": "Webcam", "descuento": 5.0, "metodo_pago": "tarjeta"},
    {"id": 14, "cliente_id": 11, "monto": 450.00, "fecha": "2024-01-28", "producto": "Impresora", "descuento": 15.0, "metodo_pago": "transferencia"},
    {"id": 15, "cliente_id": 12, "monto": 89.50, "fecha": "2024-01-29", "producto": "USB Drive", "descuento": 0.0, "metodo_pago": "tarjeta"},
]

with open(f"{base_path_local}/ventas/ventas_v3.json", "w") as f:
    for venta in ventas_v3:
        f.write(json.dumps(venta) + "\n")

print("✅ Creado ventas_v3.json (7 campos: + descuento, metodo_pago) - SCHEMA EVOLUTION 2")

# COMMAND ----------

# DBTITLE 1,4. Crear archivos Parquet - Productos (con schema evolution)
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# VERSIÓN 1: Schema básico (id, nombre, precio)
schema_v1 = StructType([
    StructField("id", IntegerType(), False),
    StructField("nombre", StringType(), True),
    StructField("precio", DoubleType(), True),
])

productos_v1 = [
    Row(id=1, nombre="Laptop Dell", precio=599.99),
    Row(id=2, nombre="Mouse Logitech", precio=25.50),
    Row(id=3, nombre="Teclado Mecánico", precio=89.99),
    Row(id=4, nombre="Monitor Samsung 24\"", precio=199.00),
    Row(id=5, nombre="Auriculares Sony", precio=79.99),
]

df_productos_v1 = spark.createDataFrame(productos_v1, schema_v1)
df_productos_v1.write.mode("overwrite").parquet(f"{base_path_local}/productos/productos_v1.parquet")

print("✅ Creado productos_v1.parquet (3 columnas: id, nombre, precio)")

# VERSIÓN 2: Schema con columna nueva (categoria)
schema_v2 = StructType([
    StructField("id", IntegerType(), False),
    StructField("nombre", StringType(), True),
    StructField("precio", DoubleType(), True),
    StructField("categoria", StringType(), True),
])

productos_v2 = [
    Row(id=6, nombre="Tablet iPad", precio=499.99, categoria="Tablets"),
    Row(id=7, nombre="Cable HDMI", precio=15.99, categoria="Accesorios"),
    Row(id=8, nombre="Webcam HD", precio=59.99, categoria="Periféricos"),
    Row(id=9, nombre="SSD 1TB", precio=129.99, categoria="Almacenamiento"),
    Row(id=10, nombre="Router WiFi", precio=89.99, categoria="Redes"),
]

df_productos_v2 = spark.createDataFrame(productos_v2, schema_v2)
df_productos_v2.write.mode("overwrite").parquet(f"{base_path_local}/productos/productos_v2.parquet")

print("✅ Creado productos_v2.parquet (4 columnas: + categoria) - SCHEMA EVOLUTION")

# VERSIÓN 3: Schema con columnas adicionales (stock, proveedor)
schema_v3 = StructType([
    StructField("id", IntegerType(), False),
    StructField("nombre", StringType(), True),
    StructField("precio", DoubleType(), True),
    StructField("categoria", StringType(), True),
    StructField("stock", IntegerType(), True),
    StructField("proveedor", StringType(), True),
])

productos_v3 = [
    Row(id=11, nombre="Impresora HP", precio=199.99, categoria="Impresión", stock=15, proveedor="HP Inc"),
    Row(id=12, nombre="Scanner Epson", precio=149.99, categoria="Impresión", stock=8, proveedor="Epson"),
    Row(id=13, nombre="USB 64GB", precio=19.99, categoria="Almacenamiento", stock=50, proveedor="SanDisk"),
    Row(id=14, nombre="Hub USB-C", precio=39.99, categoria="Accesorios", stock=25, proveedor="Anker"),
    Row(id=15, nombre="Micrófono USB", precio=79.99, categoria="Audio", stock=12, proveedor="Blue"),
]

df_productos_v3 = spark.createDataFrame(productos_v3, schema_v3)
df_productos_v3.write.mode("overwrite").parquet(f"{base_path_local}/productos/productos_v3.parquet")

print("✅ Creado productos_v3.parquet (6 columnas: + stock, proveedor) - SCHEMA EVOLUTION 2")

# COMMAND ----------

# DBTITLE 1,5. Verificar archivos creados
# MAGIC %undefined
# MAGIC # Verificar estructura de archivos creados
# MAGIC echo "📁 Estructura de archivos creados:"
# MAGIC echo ""
# MAGIC ls -lh /Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader/clientes/
# MAGIC echo ""
# MAGIC ls -lh /Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader/ventas/
# MAGIC echo ""
# MAGIC ls -lh /Workspace/Users/seorozco@gmail.com/procesamiento_distribuido/notebook/datos/autoloader/productos/

# COMMAND ----------

# DBTITLE 1,6. Copiar archivos a Volumes para ingesta
# Crear carpetas en Volumes
dbutils.fs.mkdirs(f"{base_path_volumes}/clientes")
dbutils.fs.mkdirs(f"{base_path_volumes}/ventas")
dbutils.fs.mkdirs(f"{base_path_volumes}/productos")

print("✅ Carpetas creadas en Volumes")
print("")
print("⚠️ IMPORTANTE: Vamos a copiar los archivos GRADUALMENTE para simular llegada incremental")
print("Primero copiaremos solo los archivos v1, luego v2, y finalmente v3")
print("Esto nos permitirá observar schema evolution en acción")

# COMMAND ----------

# DBTITLE 1,Explicación: Estrategia de carga incremental
# MAGIC %md
# MAGIC ## 📝 Estrategia de ingesta incremental
# MAGIC
# MAGIC Para demostrar **schema evolution** de forma realista:
# MAGIC
# MAGIC 1. **Primera carga (v1)**: Solo archivos con schema inicial
# MAGIC 2. **Segunda carga (v2)**: Archivos con columnas adicionales
# MAGIC 3. **Tercera carga (v3)**: Archivos con aún más columnas
# MAGIC
# MAGIC Auto Loader detectará automáticamente:
# MAGIC - Nuevos archivos
# MAGIC - Cambios en el schema
# MAGIC - Agregará columnas nuevas a la tabla destino
# MAGIC - Llenará con `null` los valores faltantes en registros anteriores
# MAGIC
# MAGIC ---

# COMMAND ----------


