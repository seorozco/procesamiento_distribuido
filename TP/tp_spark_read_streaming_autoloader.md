# Trabajo practico: ingesta incremental con Spark

**Tecnicatura en Datos | Unidades 7, 8 y 11 | Databricks**

Modalidad: individual o en pares, a definir por el docente. Duracion sugerida: 4 a 6 horas. Fecha de entrega: a confirmar.

## Objetivo

Implementar y comprobar pipelines de lectura y escritura con Structured Streaming, Auto Loader y evolucion de esquemas. El escenario es una tienda que recibe archivos de clientes (CSV), ventas (JSON) y productos (Parquet) en distintas entregas.

**Evaluacion: 100 puntos, 90 de practica y 10 de teoria.** Este documento Markdown es el enunciado del trabajo practico: explica que realizar, en que orden y que evidencias presentar. No incluye una solucion ni requiere completar una plantilla provista.

El alumno debe crear su propio notebook en Databricks para implementar los ejercicios y responder las preguntas teoricas. Organizar la resolucion en secciones P1 a P5 y una seccion de teoria, siguiendo este documento.

## Material incluido

- `tp_spark_read_streaming_autoloader.md`: enunciado completo del trabajo practico, con consignas, instrucciones de carga y criterios de evaluacion.
- `datos/clientes/clientes_v1.csv`, `clientes_v2.csv`, `clientes_v3.csv`.
- `datos/ventas/ventas_v1.json`, `ventas_v2.json`, `ventas_v3.json` (JSON Lines: un objeto por linea).
- `datos/productos/productos_v1.parquet`, `productos_v2.parquet`, `productos_v3.parquet` (archivos Parquet reales).

Los nueve archivos de entrada se entregan listos para usar. No hay que generarlos ni instalar dependencias locales: subirlos a Databricks y cargarlos por etapas siguiendo las consignas.

## Preparacion en Databricks

Se requiere Databricks con Unity Catalog y Auto Loader disponible. Spark local o Colab permiten practicar file source, pero no reemplazan Auto Loader (`cloudFiles` es una funcion de Databricks).

1. Crear un notebook Python desde **Workspace > Crear > Notebook**, identificarlo con nombre y apellido y seleccionar computo con soporte para Auto Loader.
2. Ir a **Catalogo > workspace > default > Crear > Volumen**. Crear un **volumen administrado** llamado `tp_streaming`, como en las capturas de clase. Puede utilizarse otro catalogo/esquema con permisos.
3. Usar la raiz `/Volumes/workspace/default/tp_streaming/<apellido_nombre>/corrida_01/`. Definir en el notebook variables para esta raiz y sus subcarpetas. Cada entrega debe usar su propio prefijo.
4. Crear `staging/clientes`, `staging/ventas` y `staging/productos` dentro de esa raiz y subir alli las tres versiones correspondientes desde `TP/datos`.
5. Crear las carpetas `entrada/file_clientes`, `entrada/rescue_clientes`, `entrada/ventas` y `entrada/productos`, inicialmente **vacias**. Son las carpetas observadas por los streams.
6. Reservar subcarpetas separadas `salida`, `checkpoints` y `schemas`. Todas deben estar en el volumen persistente y fuera de `entrada`.

No usar rutas personales `/Workspace/Users/...`, `/mnt`, DBFS root ni `/tmp`. La ruta `/Volumes/<catalogo>/<esquema>/<volumen>/...` requiere que el volumen exista; `dbutils.fs.mkdirs` crea subcarpetas, no volumenes.

**Regla de carga:** copiar desde `staging` hacia `entrada` solo la version indicada en cada etapa. Subir v1/v2/v3 juntas a `entrada` invalida la demostracion de schema evolution, porque la inferencia inicial puede incluir ya todas las columnas.

## Datos y etapas

Cada archivo tiene cinco registros y claves distintas. Las tres versiones suman 15 registros por entidad.

| Entidad | v1 | v2 | v3 |
|---|---|---|---|
| Clientes CSV | id, nombre, email | + telefono | + ciudad |
| Ventas JSON | id, cliente_id, monto, fecha | + producto | + descuento, metodo_pago |
| Productos Parquet | id, nombre, precio | + categoria | + stock, proveedor |

Los IDs de clientes y ventas son 1 a 15; los de productos son 101 a 115. Las fechas de venta cubren enero, febrero y marzo de 2025. En ventas v3, id=14 tiene monto negativo e id=15 monto nulo: ambas filas deben ir a rechazados, no desaparecer.

## Parte practica: 90 puntos

### P1. Entorno y rutas persistentes (5 puntos)

- Incluir identificacion y configurar las rutas en el notebook creado por el alumno; listar los nueve archivos en `staging`.
- Crear subcarpetas de entrada, salida, checkpoints y esquemas sin mezclar responsabilidades.
- Demostrar que no hay archivos de etapas futuras en las fuentes antes de iniciar.

**Evidencia:** configuracion ejecutada, listado de archivos y verificacion de fuentes vacias. No se evalua crear servicios cloud externos.

### P2. Structured Streaming con file source CSV (20 puntos)

1. Copiar solo `clientes_v1.csv` a `entrada/file_clientes`.
2. Definir un `StructType` explicito para `id` (entero), `nombre` y `email` (strings). Leer con `spark.readStream`, `header=true` y file source CSV, **sin `cloudFiles`**.
3. Agregar `fecha_ingesta` y ruta de origen mediante `col("_metadata.file_path")` en Databricks. Escribir a Delta con `writeStream`, modo `append` y checkpoint propio.
4. Usar `trigger(availableNow=True)` y esperar con `query.awaitTermination()` antes de leer el destino.
5. Copiar v2, reconstruir/iniciar el stream con el mismo checkpoint y esquema, esperar y verificar 10 filas. Repetir con v3 para obtener 15.
6. Reejecutar sin copiar archivos: el conteo debe seguir en 15 y cada id aparecer una sola vez.

El file source CSV con esquema fijo no agrega `telefono` ni `ciudad`. En estas muestras conserva las tres columnas declaradas; verificarlo en el destino. No asumir que toda columna adicional provoca una excepcion. No activar `FAILFAST` para esta prueba.

**Puntaje:** lectura y esquema 6; escritura y metadata 6; cargas 5/10/15 y prueba sin nuevos archivos 8.

### P3. Auto Loader JSON y schema evolution (25 puntos)

1. Copiar solo `ventas_v1.json` a `entrada/ventas`.
2. Leer con `cloudFiles.format=json`, `cloudFiles.schemaLocation` propio, `cloudFiles.inferColumnTypes=true` y `cloudFiles.schemaEvolutionMode=addNewColumns`. No fijar un esquema completo con `.schema()`.
3. Agregar metadata y escribir Bronze en Delta con `mergeSchema=true`, checkpoint propio y `availableNow`.
4. Procesar v1, luego copiar v2, procesarla; finalmente copiar v3 y procesarla. Mostrar conteo y esquema en cada etapa.
5. Comprobar `producto IS NULL` para ids 1 a 5 y `descuento`/`metodo_pago IS NULL` para ids 1 a 10. Los valores de las versiones nuevas deben coincidir con las fuentes.

**Comportamiento esperado:** al descubrir columnas nuevas, `addNewColumns` actualiza el esquema guardado y detiene el stream con un error de evolucion (por ejemplo, `UnknownFieldException`). Registrar la excepcion y reconstruir el DataFrame y el writer desde `spark.readStream`, usando **las mismas rutas** de schema y checkpoint. Volver a iniciar y esperar. No basta reiniciar el writer del DataFrame antiguo. No capturar y ocultar otros errores como si fueran evolucion.

`mergeSchema` permite que el destino Delta incorpore las columnas; no evita el reinicio de la fuente. `schemaLocation` guarda versiones del esquema en `_schemas`, **no es una tabla Delta**. Auto Loader no cambia automaticamente tipos existentes con `addNewColumns`.

**Puntaje:** fuente y configuracion 7; escritura Delta 5; evolucion v1/v2/v3 y reinicios 8; comprobacion de valores y nulos historicos 5.

### P4. Auto Loader Parquet y escritura particionada (20 puntos)

1. Copiar solo `productos_v1.parquet` a `entrada/productos`. Leer con Auto Loader y `cloudFiles.format=parquet`.
2. Usar schema location, checkpoint y destino propios. Escribir Bronze Delta con `mergeSchema=true` y `availableNow`.
3. Cargar v2 y v3 secuencialmente aplicando el mismo procedimiento de reinicio por evolucion que en P3. Verificar 5, 10 y 15 filas.
4. Comprobar tipos: `id` entero, `precio` double y `stock` entero. Parquet conserva estos tipos desde sus metadatos, sin inferencia textual. `categoria` debe ser nulo para los primeros cinco productos; `stock` y `proveedor` para los primeros diez.
5. Leer Bronze como stream Delta y escribir a un **segundo destino Parquet** en `append`, particionado por `categoria`, con otro checkpoint. Procesar con `availableNow` y esperar. Leer el Parquet resultante y verificar 15 filas; los valores nulos de categoria son esperados.

Esto evalua tanto leer archivos Parquet como **escribir Parquet mediante streaming**. No sustituir ese writer por `df.write.parquet` batch. No convertir previamente el archivo de entrada a CSV.

**Puntaje:** ingesta Parquet 5; evolucion y tipos 7; escritura streaming particionada Parquet y lectura de comprobacion 8.

### P5. Rescue, calidad y recuperacion (20 puntos)

**A. CSV con rescue (7 puntos):** usar una fuente, schema location, checkpoint y salida independientes de P2. Copiar v1, leer con Auto Loader CSV, esquema explicito de las tres columnas iniciales, `header=true`, `cloudFiles.schemaEvolutionMode=rescue` y `rescuedDataColumn=_rescued_data`. Escribir a Delta y esperar. Cargar v2 y v3 en orden. Verificar 15 filas: 5 con `_rescued_data` nulo y 10 con datos rescatados. Mostrar `telefono` y `ciudad` en el JSON rescatado; este puede incluir informacion de ruta. No descartar esas filas ni confundir rescue con un registro malformado.

**B. Calidad de ventas (7 puntos):** leer Bronze de P3 como stream Delta, validar `monto` no nulo y mayor que cero, y escribir validos y rechazados en destinos Delta diferentes con checkpoints propios. La condicion debe ser total (por ejemplo, `coalesce(condicion, lit(False))`) para no perder filas por la logica SQL de nulos. Agregar `motivo_rechazo` (`monto_invalido` o `monto_nulo`) a rechazados y `anio`/`mes` a validos, particionando estos ultimos. Procesar con `availableNow` y esperar ambas queries. Verificar **13 validos + 2 rechazados = 15**, con ids 14 y 15 en rechazados.

**C. Recuperacion y metricas (6 puntos):** conservar referencias a las queries, mostrar `status`, `lastProgress` o `recentProgress`, y probar reinicio con los mismos checkpoints sin nuevas entradas. Todos los conteos deben permanecer iguales. Mostrar cantidad de archivos nuevos o filas procesadas cuando la metrica este disponible, sin exigir tasas exactas. Confirmar que las queries propias terminaron. No detener streams ajenos con un bucle sobre todos los streams del workspace.

## Parte teorica: 10 puntos

Responder cuatro preguntas en Markdown, **2,5 puntos cada una**, con 3 a 6 lineas por respuesta:

1. Comparar file source y Auto Loader: inferencia, evolucion y deteccion de archivos. Explicar por que no basta con llamar `spark.read` para resolver esta ingesta incremental.
2. Distinguir schema location, checkpoint y `mergeSchema`. Explicar el reinicio observado con `addNewColumns` y el riesgo de borrar el checkpoint manteniendo el destino.
3. Comparar `addNewColumns` y `rescue` usando lo observado en JSON y CSV. Distinguir nuevas columnas de cambios de tipo y de errores de parseo.
4. Justificar `availableNow` y `append` para estos archivos. Explicar de que dependen las garantias exactly-once y por que `foreachBatch` necesita idempotencia propia si se utiliza.

La teoria se corrige por precision tecnica (1,5 puntos) y vinculacion con evidencia propia (1 punto) por respuesta. No se pide investigar arquitecturas ajenas al alcance de este TP.

## Rubrica y resultados de control

| Parte | Puntos | Evidencia principal |
|---|---:|---|
| P1 | 5 | Rutas y archivos preparados |
| P2 | 20 | CSV file source, Delta, 5/10/15 y sin duplicados |
| P3 | 25 | JSON Auto Loader, evolucion y nulos historicos |
| P4 | 20 | Parquet Auto Loader, tipos y salida Parquet streaming |
| P5 | 20 | Rescue, 13/2, checkpoints y metricas |
| Teoria | 10 | Cuatro respuestas fundamentadas |
| **Total** | **100** | **90 practica + 10 teoria** |

| Destino final | Filas | Control adicional |
|---|---:|---|
| Clientes file source Delta | 15 | Solo esquema fijo + metadata; ids unicos |
| Ventas Bronze Delta | 15 | Columnas nuevas y nulos historicos |
| Productos Bronze Delta | 15 | Tipos preservados y nulos historicos |
| Productos salida Parquet | 15 | Particiones por categoria |
| Clientes rescue Delta | 15 | 5 sin rescue, 10 con rescue |
| Ventas validas Delta | 13 | Particiones por anio/mes |
| Ventas rechazadas Delta | 2 | id=14 negativo, id=15 nulo |

No se requieren tiempos exactos ni se evalua la cantidad de micro-lotes. Las evidencias de ejecucion y pruebas forman parte del 90% practico. Los puntos no demostrados se descuentan en su criterio, sin penalizaciones duplicadas.

## Entrega y ejecucion

El material de la consigna es este archivo `.md` junto con los datos de entrada. La resolucion del alumno se entrega por separado como notebook ejecutado; no se debe editar el enunciado para resolver el trabajo.

Entregar `tp_streaming_APELLIDO_NOMBRE.ipynb` con codigo, salidas visibles, conteos/asserts, esquemas por etapa, metricas y respuestas teoricas. En pares incluir ambos nombres. No entregar credenciales ni datos personales reales.

Para una ejecucion limpia usar un **nuevo prefijo personal de corrida**, subir de nuevo los archivos a `staging` y seguir las etapas. No borrar checkpoints ni tablas para aprobar la prueba de recuperacion. Si se reinicia toda la demostracion, cambiar conjuntamente entrada, salida, schema y checkpoint. Conservar en la entrega la evidencia del error esperado y su reinicio; al automatizarlo, capturar solo errores de evolucion reconocidos y limitar los reintentos.

`availableNow` termina al consumir los archivos disponibles al inicio; no permanece esperando archivos que se copien despues. Para cada nueva carga iniciar otra query, esperar su terminacion y solo entonces consultar el destino. Evitar `time.sleep()` como prueba de finalizacion y la fuente `rate` para este ejercicio finito.

Referencias del curso: [Unidad 7](../documentacion/unidad_07_lectura_escritura.md), [Unidad 8](../documentacion/unidad_08_structured_streaming.md) y [Unidad 11](../documentacion/unidad_11_ingesta_autoloader.md).

Referencias oficiales: [Auto Loader y esquemas](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema), [Unity Catalog Volumes](https://docs.databricks.com/aws/en/volumes/) y [Structured Streaming](https://spark.apache.org/docs/latest/streaming/index.html).