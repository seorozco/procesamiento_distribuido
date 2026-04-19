## Unidad 2: Arquitectura de Apache Spark

**Tecnicatura en Datos – Procesamiento con Apache Spark**  
Unidad 2 de 10 — Duración estimada: 2:30 hs

---

## 1. Introducción

En la unidad anterior entendimos **por qué nació Spark**.  
En esta unidad vamos a responder una pregunta clave para cualquier ingeniero de datos:

> **¿Qué pasa realmente cuando ejecutamos un programa Spark?**

Comprender la **arquitectura interna de Spark** es fundamental para:
- Diagnosticar problemas de rendimiento
- Entender por qué un job es lento o falla
- Tomar decisiones correctas de particionamiento y memoria
- Leer e interpretar la Spark UI con criterio técnico

A partir de ahora dejamos de usar Spark como una “caja negra”.

---

## 2. Componentes principales de Spark

Una aplicación Spark está compuesta por varios procesos distribuidos que cooperan entre sí.

### 2.1 Driver Program

El **Driver** es el proceso principal de una aplicación Spark.

Responsabilidades del Driver:
- Ejecutar el código principal del usuario
- Crear el `SparkSession`
- Construir el **DAG** (plan de ejecución)
- Coordinar la ejecución de tareas
- Recibir los resultados finales

👉 El Driver **no procesa datos masivos**, coordina.

---

### 2.2 SparkSession (y SparkContext)

El **SparkSession** es el punto de entrada a Spark desde Spark 2.x en adelante.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder     .appName("EjemploArquitectura")     .master("local[*]")     .getOrCreate()
```

Internamente, SparkSession:
- Contiene un `SparkContext`
- Integra Spark SQL, DataFrames y configuración

---

## 3. Executors

Los **Executors** son procesos que corren en los nodos del clúster.

Funciones:
- Ejecutar tasks
- Mantener datos en memoria
- Enviar resultados al Driver

---

## 4. Cluster Managers

- Standalone
- YARN
- Kubernetes

---

## 5. Modos de ejecución

- Local
- Client
- Cluster

---

## 6. DAG (Directed Acyclic Graph)

Spark construye un plan lógico antes de ejecutar.

---

## 7. Jobs, Stages y Tasks

- Job: acción
- Stage: separación por shuffle
- Task: una partición

---

## 8. Flujo completo

1. Driver construye DAG
2. Planificación
3. Ejecución distribuida

---

## 9. Caso de uso

Procesamiento batch bancario nocturno.

---

## 10. Práctica guiada

Identificación de Jobs y Stages en Spark UI.

---

## 11. Preguntas de revisión

- Diferencias Driver vs Executor
- Qué es un DAG

---

## 12. Resumen

Arquitectura fundamental para entender rendimiento Spark.

---

**Próxima unidad:** RDDs – Resilient Distributed Datasets
