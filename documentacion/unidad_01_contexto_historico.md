# Unidad 1: Del origen a Spark — Contexto histórico y tecnológico

> **Tecnicatura en Datos | Procesamiento con Apache Spark**
> Unidad 1 de 10 | Duración estimada: 2:30 hs

---

## Introducción

Antes de escribir una sola línea de código Spark, es fundamental entender **por qué existe**. Apache Spark no surgió de la nada: es la respuesta a más de dos décadas de problemas reales que las empresas más grandes del mundo tuvieron que resolver.

Esta unidad cuenta esa historia. Vamos a recorrer el camino desde los problemas de almacenamiento masivo de Google a principios de los 2000, hasta llegar a Spark como motor de procesamiento distribuido moderno. Cada tecnología que estudiaremos nació para resolver las limitaciones de la anterior, y entender esa evolución es lo que te va a permitir tomar mejores decisiones técnicas cuando trabajés con Spark.

---

## 1. El problema que lo inició todo

### 1.1 El desafío de Google a principios del año 2000

Imaginá que sos un ingeniero en Google en el año 2001. El motor de búsqueda está creciendo exponencialmente. Cada día, millones de páginas web nuevas necesitan ser indexadas. El volumen de datos es tan gigantesco que ninguna base de datos tradicional puede manejarlo. El hardware más potente del mercado es insuficiente y prohibitivamente caro.

La pregunta que se hacían los ingenieros de Google era: **¿cómo almacenamos y procesamos cientos de terabytes (y pronto petabytes) de datos de forma confiable y económica?**

La respuesta convencional de la época era usar hardware muy caro y muy poderoso: servidores grandes, discos RAID, bases de datos Oracle. Pero este enfoque —conocido como **scale-up** o escalamiento vertical— tiene un límite físico y económico muy claro. Llega un punto en que no podés comprar una máquina más grande.

Google tomó una decisión revolucionaria: en lugar de usar pocas máquinas muy caras, usarían **miles de máquinas baratas y comunes** (hardware commodity). Si una falla —y van a fallar, porque son baratas— el sistema tiene que ser lo suficientemente inteligente para continuar funcionando sin interrupciones.

Este es el paradigma del **scale-out** o escalamiento horizontal: agregar más máquinas en lugar de máquinas más potentes.

### 1.2 Las consecuencias de esta decisión

Este cambio de paradigma trajo consigo un conjunto completamente nuevo de problemas:

- **¿Cómo distribuir los datos en miles de discos?**
- **¿Cómo leer esos datos de forma eficiente?**
- **¿Qué pasa cuando una máquina (o un disco) falla?**
- **¿Cómo garantizar que los datos no se pierdan?**
- **¿Cómo coordinar el trabajo entre miles de nodos?**

Para resolver estos problemas, Google diseñó el **Google File System (GFS)**, cuyo paper fue publicado en 2003 y cambió para siempre la industria del procesamiento de datos.

---

## 2. Google File System (GFS)

### 2.1 ¿Qué es GFS?

GFS es un **sistema de archivos distribuido** diseñado específicamente para:

- Correr sobre hardware commodity (máquinas baratas y comunes)
- Manejar archivos de tamaño muy grande (decenas o cientos de gigabytes)
- Trabajar en entornos donde las fallas de hardware son la norma, no la excepción
- Optimizar para lecturas y escrituras secuenciales de grandes volúmenes de datos

Es importante notar lo que GFS **no** está diseñado para hacer: no es una base de datos, no soporta actualizaciones aleatorias de archivos, y no está optimizado para muchos archivos pequeños. Está pensado para un patrón de acceso muy específico: escribir una vez, leer muchas veces, en secuencia.

### 2.2 Arquitectura de GFS

La arquitectura de GFS tiene tres componentes principales:

#### Master Node (Nodo Master)

El Master es el cerebro del sistema. Es un **único servidor** que mantiene toda la metadata del sistema de archivos:

- El árbol de directorios y nombres de archivos
- Los metadatos de cada archivo (permisos, fechas, tamaño)
- La ubicación de cada chunk (fragmento de datos) en los ChunkServers

El Master no almacena los datos en sí mismos, solo la información sobre dónde están. Esto es clave para su diseño: como no maneja datos, puede responder muy rápido a las consultas de metadata.

**Limitación importante**: al haber un único Master, existe un riesgo de punto único de falla. GFS mitiga esto con replicación del log de operaciones y checkpoints periódicos, pero este será uno de los problemas que Hadoop eventualmente abordará.

#### ChunkServers (Servidores de Chunks)

Los ChunkServers son los trabajadores del sistema. Almacenan los datos reales. Cada archivo en GFS se divide en fragmentos llamados **chunks** de tamaño fijo (64 MB por defecto, una decisión deliberada para optimizar transferencias de grandes volúmenes).

Cada chunk se identifica con un ID único de 64 bits. Los ChunkServers almacenan los chunks como archivos del sistema operativo local y los sirven a los clientes cuando estos los solicitan.

#### Replicación

Aquí está uno de los mecanismos más importantes de GFS: **cada chunk se replica en múltiples ChunkServers** (por defecto, 3 réplicas). Esto significa que si una máquina falla (y en un clúster de miles de máquinas, alguna siempre está fallando), los datos no se pierden: están en otras dos máquinas.

El Master monitorea constantemente el estado de los ChunkServers mediante heartbeats periódicos. Si un ChunkServer deja de responder, el Master sabe que debe crear nuevas réplicas de sus chunks en otras máquinas para mantener el factor de replicación deseado.

### 2.3 Flujo de lectura en GFS

Entender cómo funciona una lectura en GFS es útil para comprender las decisiones de diseño:

1. El **cliente** quiere leer un archivo. Consulta al **Master** cuáles chunks componen ese archivo y en qué ChunkServers están.
2. El Master responde con la lista de ChunkServers que tienen cada chunk (incluyendo las réplicas).
3. El cliente elige el ChunkServer más cercano (en términos de red) y le solicita los chunks directamente.
4. El ChunkServer envía los datos al cliente.

Notá que después del paso inicial de metadata, el Master queda fuera del flujo de datos. Los clientes hablan directamente con los ChunkServers. Esto es una decisión de diseño brillante: el Master no se convierte en un cuello de botella para las transferencias de datos.

### 2.4 Legado de GFS

GFS demostró que era posible construir un sistema de almacenamiento confiable y escalable usando hardware barato. El paper de GFS inspiró directamente la creación de **HDFS** (Hadoop Distributed File System), que es básicamente una implementación open source de los conceptos de GFS.

Los principios de GFS —replicación para tolerancia a fallos, separación de metadata y datos, optimización para grandes archivos secuenciales— siguen siendo fundamentales en los sistemas de almacenamiento distribuido modernos.

---

## 3. MapReduce

### 3.1 El problema del procesamiento

GFS resolvió el almacenamiento, pero quedaba otro problema enorme: ¿cómo **procesar** esos petabytes de datos de forma eficiente?

Un año después del paper de GFS, en 2004, Google publicó otro paper seminal: **MapReduce: Simplified Data Processing on Large Clusters**. Si GFS fue la revolución en almacenamiento, MapReduce fue la revolución en procesamiento.

### 3.2 El modelo de programación MapReduce

MapReduce es un **modelo de programación** que abstrae la complejidad del procesamiento distribuido. La idea central es extraordinariamente elegante: dividir cualquier problema de procesamiento de datos en dos fases simples.

#### Fase Map

La fase Map toma los datos de entrada y los transforma en pares **clave-valor**. Cada elemento de entrada se procesa de forma independiente, generando cero, uno o más pares clave-valor como salida.

El punto crucial es que las operaciones Map son **completamente independientes entre sí**. No hay comunicación entre los distintos workers que ejecutan el Map. Esto hace que sea trivialmente paralelizable: podés correr miles de operaciones Map simultáneamente en distintas máquinas.

#### Fase Reduce

La fase Reduce toma todos los pares clave-valor generados por el Map, los agrupa por clave, y para cada clave aplica una función de reducción sobre todos sus valores.

#### Ejemplo clásico: Word Count

El ejemplo más famoso de MapReduce es el conteo de palabras. Dado un conjunto de documentos de texto, contar cuántas veces aparece cada palabra.

**Fase Map**: para cada documento, para cada palabra, emitir el par `(palabra, 1)`.

```
Entrada: "el gato y el perro"
Salida Map:
  ("el", 1)
  ("gato", 1)
  ("y", 1)
  ("el", 1)
  ("perro", 1)
```

**Shuffle**: el framework agrupa automáticamente todos los pares con la misma clave.

```
("el", [1, 1])
("gato", [1])
("perro", [1])
("y", [1])
```

**Fase Reduce**: para cada clave, sumar todos los valores.

```
("el", 2)
("gato", 1)
("perro", 1)
("y", 1)
```

### 3.3 Cómo MapReduce ejecuta un job

Cuando se lanza un job MapReduce, ocurre lo siguiente:

1. El framework divide los datos de entrada en **splits** (fragmentos).
2. Para cada split, se lanza una tarea **Map** en un worker (idealmente en el mismo nodo donde están los datos, principio de **data locality**).
3. La salida de los Mappers se escribe en el **disco local** de cada worker.
4. El framework ejecuta el **Shuffle**: mueve los datos entre workers para agruparlos por clave. Esta es la fase más costosa en términos de red.
5. Para cada grupo de claves, se lanza una tarea **Reduce**.
6. La salida de los Reducers se escribe en **HDFS**.

### 3.4 La fortaleza de MapReduce

MapReduce fue revolucionario por varias razones:

- **Tolerancia a fallos automática**: si un worker falla, el framework automáticamente relanza la tarea en otro worker.
- **Escalabilidad lineal**: agregar más máquinas al clúster aumenta proporcionalmente la capacidad de procesamiento.
- **Abstracción de la complejidad**: los desarrolladores solo escriben las funciones Map y Reduce; el framework maneja toda la distribución, el shuffle y la recuperación ante fallos.
- **Data locality**: el framework intenta ejecutar las tareas Map en los mismos nodos donde están los datos, reduciendo el tráfico de red.

### 3.5 Las limitaciones de MapReduce

A pesar de su éxito, MapReduce tiene limitaciones fundamentales que eventualmente llevaron a su reemplazo:

#### Escritura intermedia en disco

Cada fase de MapReduce escribe sus resultados en disco: los Mappers escriben en disco local, el Shuffle mueve datos entre discos, los Reducers escriben en HDFS. Para algoritmos que requieren múltiples pasadas sobre los datos (como los algoritmos de Machine Learning iterativos), esto se vuelve extremadamente lento.

**Ejemplo**: un algoritmo de regresión logística puede requerir cientos de iteraciones. Con MapReduce, cada iteración implica leer y escribir en disco. Con procesamiento en memoria (como Spark), los datos intermedios se mantienen en RAM.

#### Un solo nivel de paralelismo (Map + Reduce)

MapReduce fuerza a que todos los problemas se expresen en exactamente dos fases. Muchos algoritmos complejos son difíciles o imposibles de expresar naturalmente en este modelo. Frecuentemente, se necesitan encadenar múltiples jobs MapReduce, y cada encadenamiento implica más lecturas y escrituras en HDFS.

#### API de bajo nivel y difícil de usar

Escribir un job MapReduce requiere mucho código boilerplate. Tareas que deberían ser simples (un join entre dos datasets, por ejemplo) requieren implementaciones complejas y propensas a errores.

#### Latencia alta

Dado el overhead de lanzar los workers, leer datos de HDFS, el shuffle, escribir resultados, etc., incluso los jobs simples tienen latencias de minutos. MapReduce simplemente no es adecuado para casos de uso interactivos o semi-interactivos.

---

## 4. Apache Hadoop

### 4.1 Del paper a la realidad open source

Mientras Google usaba GFS y MapReduce internamente, dos ingenieros trabajando en proyectos open source —Doug Cutting (creador de Lucene y Nutch) y Mike Cafarella— leyeron los papers de Google y decidieron implementar esas ideas en Java como software open source.

En 2006, el proyecto fue donado a la Apache Software Foundation y pasó a llamarse **Apache Hadoop**, en referencia al elefante de juguete del hijo de Doug Cutting.

### 4.2 Componentes de Hadoop

Hadoop en su versión 1.x estaba compuesto por dos componentes principales:

#### HDFS (Hadoop Distributed File System)

HDFS es la implementación open source de los conceptos de GFS. Las diferencias con GFS son menores; la arquitectura conceptual es prácticamente idéntica:

- **NameNode**: equivalente al Master de GFS. Almacena la metadata del sistema de archivos.
- **DataNodes**: equivalentes a los ChunkServers. Almacenan los bloques de datos.
- **Bloques de 128 MB**: HDFS usa bloques más grandes que GFS (128 MB por defecto vs 64 MB), optimizado para lecturas secuenciales de grandes archivos.
- **Factor de replicación 3**: cada bloque se replica en 3 DataNodes por defecto.

HDFS introdujo algunos conceptos importantes:

**Rack awareness**: HDFS sabe en qué rack físico del datacenter está cada DataNode. Al replicar bloques, se asegura de que no todas las réplicas estén en el mismo rack. Así, si un rack entero falla (ej. por un corte en el switch de red), los datos siguen disponibles en otros racks.

**Secondary NameNode**: a diferencia del Master de GFS, HDFS introdujo un Secondary NameNode que hace checkpoints periódicos del estado del NameNode principal. Importante aclaración: el Secondary NameNode **no es un standby** del NameNode principal; no puede tomar su lugar si el NameNode falla. Solo sirve para aliviar la carga del NameNode principal respecto a los checkpoints.

#### MapReduce en Hadoop 1.x

Hadoop 1.x incluía su propia implementación de MapReduce con una arquitectura de dos componentes:

- **JobTracker**: el componente central que acepta los jobs de los clientes, los divide en tareas, y las asigna a los TaskTrackers. También monitorea el progreso y maneja los fallos.
- **TaskTrackers**: los workers que ejecutan las tareas Map y Reduce en cada nodo del clúster.

### 4.3 El problema del JobTracker: el cuello de botella

En Hadoop 1.x, el JobTracker tenía dos responsabilidades que no deberían estar juntas:

1. **Gestión de recursos**: decidir cuánta memoria y CPU asignar a cada tarea.
2. **Gestión del ciclo de vida de los jobs**: monitorear el progreso, reintentar tareas fallidas, etc.

Esta mezcla de responsabilidades trajo problemas serios a medida que los clústeres crecían:

- El JobTracker era un **single point of failure**: si caía, todos los jobs en ejecución se perdían.
- El JobTracker tenía un límite de escalabilidad de aproximadamente **4,000 nodos** y **40,000 tareas concurrentes**.
- Solo soportaba MapReduce: no había forma de correr otros frameworks de procesamiento (como sistemas de grafos o procesamiento en tiempo real) sobre el mismo clúster HDFS.

Esto llevó al diseño de **YARN**, el componente que transformó Hadoop de un sistema de procesamiento MapReduce a una plataforma de procesamiento de datos genérica.

---

## 5. YARN (Yet Another Resource Negotiator)

### 5.1 La motivación de YARN

YARN fue introducido en Hadoop 2.0 (2012) con un objetivo claro: **separar la gestión de recursos del clúster de la lógica de los frameworks de procesamiento**.

La idea era simple pero poderosa: si separamos "¿cuántos recursos tiene disponibles el clúster?" de "¿cómo procesa los datos un framework específico?", entonces múltiples frameworks pueden compartir el mismo clúster HDFS simultáneamente.

### 5.2 Arquitectura de YARN

YARN introduce tres componentes principales:

#### ResourceManager

El ResourceManager es el árbitro supremo de todos los recursos del clúster. Tiene dos componentes internos:

- **Scheduler**: decide cómo asignar los recursos disponibles entre las aplicaciones que los solicitan. El Scheduler no monitorea el estado de las aplicaciones ni reintenta tareas fallidas; su única responsabilidad es la asignación de recursos.
- **ApplicationsManager**: acepta los submissions de aplicaciones, negocia el primer contenedor para ejecutar el ApplicationMaster de cada aplicación, y provee el servicio de reinicio de ApplicationMasters en caso de fallo.

#### NodeManager

El NodeManager corre en cada nodo del clúster y es responsable de:

- Lanzar y monitorear los **Containers** (unidades de recursos: CPU + memoria) en su nodo
- Reportar el estado y uso de recursos al ResourceManager mediante heartbeats
- Matar containers cuando el ResourceManager lo indica

#### ApplicationMaster

Aquí está la innovación clave de YARN: **cada aplicación tiene su propio ApplicationMaster**. Cuando un usuario submite una aplicación (sea un job MapReduce, un job Spark, o cualquier otro framework), YARN lanza un ApplicationMaster específico para esa aplicación.

El ApplicationMaster es responsable de:

- Negociar los recursos necesarios con el ResourceManager
- Trabajar con los NodeManagers para lanzar y monitorear las tareas de la aplicación
- Manejar los fallos y reintentos

Esta arquitectura distribuye la responsabilidad de gestión de aplicaciones: en lugar de un único JobTracker que maneja todo, ahora cada aplicación gestiona sus propias tareas.

### 5.3 Flujo de ejecución de una aplicación en YARN

1. El cliente submite la aplicación al **ResourceManager**.
2. El ResourceManager selecciona un NodeManager para lanzar el **ApplicationMaster** de la aplicación.
3. El ApplicationMaster se registra con el ResourceManager y negocia los **Containers** necesarios.
4. El ResourceManager asigna Containers en distintos NodeManagers.
5. El ApplicationMaster coordina la ejecución de las tareas en los Containers asignados.
6. A medida que las tareas terminan, el ApplicationMaster reporta el progreso al ResourceManager.
7. Cuando la aplicación termina, el ApplicationMaster se desregistra del ResourceManager.

### 5.4 El impacto de YARN

YARN transformó fundamentalmente el ecosistema Hadoop:

- **Múltiples frameworks en el mismo clúster**: Spark, MapReduce, Storm, Flink y otros pueden correr simultáneamente compartiendo los recursos de un único clúster HDFS.
- **Mayor escalabilidad**: YARN puede manejar clústeres de decenas de miles de nodos.
- **Mejor utilización de recursos**: el Scheduler de YARN puede usar políticas sofisticadas (Fair Scheduler, Capacity Scheduler) para maximizar la utilización del clúster.

---

## 6. Apache Spark

### 6.1 El origen de Spark

En 2009, Matei Zaharia, estudiante de doctorado en UC Berkeley, comenzó a trabajar en un proyecto de investigación para superar las limitaciones de MapReduce. El paper original de Spark fue publicado en 2010 bajo el título "Spark: Cluster Computing with Working Sets".

La motivación era clara: los algoritmos iterativos de Machine Learning y los análisis de datos interactivos sufrían enormemente con el modelo de MapReduce, donde cada iteración requería leer y escribir en disco.

La idea central de Spark era radical en su simplicidad: **mantener los datos intermedios en memoria (RAM) en lugar de escribirlos en disco**.

En 2013, el proyecto fue donado a la Apache Software Foundation. En 2014, Spark superó a Hadoop MapReduce en el benchmark de sorting de 100 TB, siendo **3 veces más rápido usando 10 veces menos máquinas**. En ese momento, quedó claro que Spark era el futuro del procesamiento distribuido.

### 6.2 La abstracción fundamental: RDD

El concepto central de Spark es el **Resilient Distributed Dataset (RDD)**. Un RDD es una colección de elementos distribuida a través de los nodos del clúster, con dos características fundamentales:

**Resiliente (tolerante a fallos)**: si se pierde una partición de un RDD (porque un nodo falló), Spark puede recalcularla automáticamente usando el **lineage** —el registro de todas las transformaciones que se aplicaron para crear ese RDD desde los datos originales.

**Distribuida**: los datos del RDD están particionados a través de múltiples nodos del clúster, permitiendo el procesamiento paralelo.

Esta combinación —procesamiento en memoria + tolerancia a fallos mediante lineage— es lo que hace a Spark tan poderoso.

### 6.3 Comparativa: MapReduce vs Spark

| Característica | MapReduce | Apache Spark |
|---|---|---|
| **Procesamiento** | En disco | En memoria (RAM) |
| **Velocidad** | Lento (lectura/escritura en cada paso) | Hasta 100x más rápido en memoria |
| **Modelo de programación** | Solo Map + Reduce | DAG de transformaciones arbitrario |
| **API** | Java, difícil de usar | Python, Scala, Java, R; API de alto nivel |
| **Casos de uso** | Batch processing simple | Batch, streaming, ML, grafos, SQL |
| **Algoritmos iterativos** | Muy lento | Excelente (datos en memoria entre iteraciones) |
| **Consultas interactivas** | No soportado | Spark Shell interactivo |
| **Ecosistema** | Solo MapReduce | Spark SQL, MLlib, GraphX, Structured Streaming |

### 6.4 El modelo de ejecución de Spark

Cuando se ejecuta una aplicación Spark, intervienen los siguientes componentes:

**Driver Program**: el proceso que corre el código principal de la aplicación. El Driver es responsable de:
- Crear el SparkContext (o SparkSession en versiones modernas)
- Convertir el código del usuario en un plan de ejecución (DAG)
- Coordinar la ejecución de las tareas en el clúster

**SparkContext / SparkSession**: el punto de entrada a todas las funcionalidades de Spark. En versiones modernas (Spark 2.x+), se usa SparkSession como punto de entrada unificado.

**Cluster Manager**: el sistema que gestiona los recursos del clúster. Spark puede trabajar con varios Cluster Managers:
- **Standalone**: el Cluster Manager propio de Spark, simple de configurar
- **YARN**: el Cluster Manager de Hadoop (más usado en producción)
- **Kubernetes**: gestión de recursos basada en contenedores (creciente adopción)
- **Mesos**: alternativa menos común hoy en día

**Executors**: los procesos que corren en los nodos del clúster y ejecutan las tareas. Cada Executor tiene una cantidad configurable de CPU y memoria. Los Executors son responsables de:
- Ejecutar las tareas asignadas por el Driver
- Mantener los datos en memoria (cache/persist)
- Retornar los resultados al Driver

### 6.5 El concepto de DAG

Una de las diferencias fundamentales entre MapReduce y Spark es cómo planifican la ejecución.

MapReduce tiene un plan fijo: Map → Shuffle → Reduce. Siempre.

Spark, en cambio, construye un **Directed Acyclic Graph (DAG)** de operaciones. Un DAG es un grafo donde cada nodo es una operación (una transformación sobre los datos) y las aristas representan las dependencias entre operaciones.

Cuando el programa Spark ejecuta una acción (como `count()` o `collect()`), Spark no ejecuta las operaciones de a una: primero construye el DAG completo de todo lo que necesita hacer, lo **optimiza** (puede combinar varias operaciones en un solo paso, puede evitar shuffles innecesarios, puede aplicar predicados más temprano para reducir el volumen de datos), y **luego** lo ejecuta.

Este modelo de "lazy evaluation" —donde las transformaciones no se ejecutan hasta que se necesita un resultado— permite a Spark optimizar el plan de ejecución de forma holística.

### 6.6 El ecosistema de Spark

Una de las grandes ventajas de Spark es su ecosistema integrado. Sobre el core de Spark se construyen varias bibliotecas de alto nivel:

**Spark SQL**: permite trabajar con datos estructurados usando SQL o la API de DataFrames. Internamente usa el Catalyst Optimizer para generar planes de ejecución eficientes.

**MLlib**: biblioteca de Machine Learning distribuida. Incluye algoritmos de clasificación, regresión, clustering, reducción de dimensionalidad, y pipelines de ML.

**GraphX**: API para procesamiento de grafos y computación paralela sobre grafos. Útil para análisis de redes sociales, detección de fraude, etc.

**Structured Streaming**: motor de procesamiento de streams de datos en tiempo real, construido sobre el mismo motor que Spark SQL y DataFrames.

---

## 7. La línea de tiempo: una historia de evolución continua

```
2003 ─── Paper GFS (Google)
           │
           └── Soluciona: almacenamiento distribuido confiable
           
2004 ─── Paper MapReduce (Google)
           │
           └── Soluciona: procesamiento distribuido escalable
           
2006 ─── Apache Hadoop 1.x (HDFS + MapReduce)
           │
           └── Implementación open source de GFS + MapReduce
           └── Problema: JobTracker como cuello de botella
           └── Problema: escritura en disco entre etapas
           
2010 ─── Paper Spark (UC Berkeley)
           │
           └── Soluciona: procesamiento en memoria, API más simple
           
2012 ─── Apache Hadoop 2.x con YARN
           │
           └── Soluciona: separación de gestión de recursos y procesamiento
           └── Permite múltiples frameworks en el mismo clúster
           
2013 ─── Apache Spark donado a la ASF
           │
           └── Proyecto de código abierto con crecimiento explosivo
           
2014 ─── Spark bate récord mundial de sorting
           │
           └── 100 TB en 23 min (vs 72 min de Hadoop MapReduce)
           └── Usando 10x menos máquinas
           
2016 ─── Spark 2.0: Structured APIs (DataFrames, Datasets, Spark SQL)
           │
           └── API unificada, mejor rendimiento, más fácil de usar
           
2020 ─── Spark 3.0: mejoras de rendimiento, soporte Python mejorado
           │
           └── Adaptive Query Execution, aceleración GPU
```

---

## 8. Conceptos clave para recordar

### Principios fundamentales que persisten

A lo largo de toda esta evolución, algunos principios se mantuvieron constantes:

**Tolerancia a fallos mediante replicación y lineage**: desde GFS replicando chunks hasta Spark recalculando particiones perdidas usando el lineage del RDD, el principio es el mismo: asumir que los componentes van a fallar y diseñar el sistema para que eso no sea catastrófico.

**Data locality**: mover el cómputo hacia donde están los datos es más barato que mover los datos hacia donde está el cómputo. GFS y MapReduce lo implementaban ejecutando las tareas Map en los mismos nodos que almacenan los datos. Spark lo sigue aplicando cuando es posible.

**Paralelismo masivo sobre hardware commodity**: en lugar de escalar verticalmente (máquinas más caras), escalar horizontalmente (más máquinas baratas). Este paradigma, establecido por Google en 2003, sigue siendo la base de todos los sistemas de big data modernos.

**Abstracción de la complejidad distribuida**: tanto MapReduce como Spark buscan que el programador no tenga que pensar en la distribución. El programador escribe código como si trabajara con datos locales; el framework se encarga de distribuir el trabajo y manejar los fallos.

---

## 9. Actividad práctica: Análisis comparativo

### Parte 1: Diagrama de arquitecturas

Dibujá (en papel o en una herramienta de diagramas) los siguientes diagramas comparativos:

1. **GFS**: Master + ChunkServers + Clientes, mostrando el flujo de lectura (2 pasos: metadata al Master, datos al ChunkServer).

2. **Hadoop 1.x**: NameNode + DataNodes (HDFS) + JobTracker + TaskTrackers (MapReduce), mostrando cómo un job fluye desde el cliente.

3. **Hadoop 2.x con YARN**: NameNode + DataNodes (HDFS) + ResourceManager + NodeManagers + ApplicationMasters, mostrando cómo conviven múltiples aplicaciones.

4. **Spark sobre YARN**: Driver + SparkContext + ResourceManager + Executors, mostrando el flujo de una aplicación Spark.

### Parte 2: Análisis de limitaciones de MapReduce

Dado el siguiente pseudocódigo de un algoritmo iterativo de K-Means (clustering):

```
inicializar centroides aleatoriamente
repetir 100 veces:
    para cada punto de datos:
        asignar el punto al centroide más cercano
    recalcular los centroides como el promedio de los puntos asignados
```

Responder:

1. ¿Cuántos jobs MapReduce se necesitarían para ejecutar este algoritmo?
2. ¿Cuántas lecturas y escrituras en HDFS implicaría una ejecución completa?
3. ¿Cómo cambia este escenario con Spark? ¿Cuántas lecturas desde disco se necesitan?

### Parte 3: Ejercicio de instalación

Instalar Spark en modo local siguiendo estos pasos:

```bash
# Opción 1: Con pip (recomendado para empezar)
pip install pyspark

# Verificar la instalación
python -c "import pyspark; print(pyspark.__version__)"
```

Una vez instalado, ejecutar el siguiente script de verificación:

```python
from pyspark.sql import SparkSession

# Crear una SparkSession
spark = SparkSession.builder \
    .appName("HolaSpark") \
    .master("local[*]") \
    .getOrCreate()

# Ver la versión de Spark
print(f"Versión de Spark: {spark.version}")

# Crear un DataFrame simple
data = [("GFS", 2003), ("MapReduce", 2004), 
        ("Hadoop", 2006), ("Spark", 2013)]
df = spark.createDataFrame(data, ["Tecnologia", "Año"])

# Mostrar el resultado
df.show()

# Ver la Spark UI (abrir en el navegador)
print(f"\nSpark UI disponible en: http://localhost:4040")
print("Presioná Enter para terminar...")
input()

spark.stop()
```

Explorar la **Spark UI** en `http://localhost:4040` y familiarizarse con las pestañas Jobs, Stages y Executors.

---

## 10. Preguntas de revisión

Las siguientes preguntas sirven para autoevaluar la comprensión de los conceptos de esta unidad:

1. ¿Por qué Google eligió usar hardware commodity en lugar de servidores de alta gama? ¿Cuáles son los trade-offs de esa decisión?

2. En GFS, ¿por qué el Master no maneja directamente el tráfico de datos entre clientes y ChunkServers?

3. ¿Cuál es la diferencia entre escalamiento vertical (scale-up) y escalamiento horizontal (scale-out)? ¿En qué situaciones conviene cada uno?

4. Explicá con tus propias palabras el concepto de "lazy evaluation" en Spark. ¿Por qué es importante para la optimización?

5. ¿Por qué el modelo de MapReduce es ineficiente para algoritmos iterativos como los de Machine Learning?

6. ¿Qué problema resuelve YARN que no resolvía Hadoop 1.x? ¿Cuál es el componente clave de YARN que hace posible esta solución?

7. ¿Qué es el lineage en Spark y cómo contribuye a la tolerancia a fallos?

8. En un sistema bancario real, ¿qué tipo de procesamiento sería más adecuado para cada tecnología: un batch nocturno de reconciliación de transacciones, análisis interactivo de datos de clientes, detección de fraude en tiempo real?

---

## Resumen de la unidad

| Tecnología | Año | Problema que resuelve | Limitación principal |
|---|---|---|---|
| GFS | 2003 | Almacenamiento distribuido confiable sobre hardware commodity | Single Master, optimizado solo para archivos grandes |
| MapReduce | 2004 | Procesamiento distribuido paralelizable | Escritura en disco, latencia alta, solo Map+Reduce |
| Hadoop HDFS | 2006 | Implementación open source de GFS | NameNode como punto único de falla (Hadoop 1.x) |
| Hadoop MapReduce | 2006 | Implementación open source de MapReduce | JobTracker como cuello de botella |
| YARN | 2012 | Separación de gestión de recursos y procesamiento | Complejidad operativa mayor |
| Apache Spark | 2013 | Procesamiento en memoria, API de alto nivel, múltiples paradigmas | Requiere más RAM que MapReduce |

---

> **Próxima unidad**: Arquitectura interna de Apache Spark — Driver, Executors, DAG, Jobs, Stages y Tasks. Entenderemos en detalle cómo Spark planifica y ejecuta el trabajo en el clúster.
