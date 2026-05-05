# Trabajo Práctico — Unidades 1 a 4
## Procesamiento con Apache Spark | Tecnicatura en Datos

---

## Descripción general

Este trabajo práctico evalúa los contenidos vistos en las unidades 1 a 4 de la materia:

- **Unidad 1:** Contexto histórico del procesamiento distribuido (GFS, MapReduce, Hadoop, Spark)
- **Unidad 4:** DataFrames y Spark SQL — carga, transformación, agregaciones y joins

Trabajarás con dos datasets reales provistos por la cátedra y deberás resolver una serie de ejercicios dentro de un Jupyter Notebook.

---

## Archivos del trabajo

| Archivo | Descripción |
|---|---|
| `trabajo_practico_u1_u4.ipynb` | Notebook que debés completar y entregar |
| `datos/input/ventas.csv` | 200 registros de ventas |
| `datos/input/productos.csv` | 20 productos |

> Los CSV están disponibles en el aula virtual para su descarga. Según el entorno que uses, deberás subirlos de forma distinta (ver sección **Configuración por entorno**).

---

## Estructura del trabajo práctico

| Parte | Tema | Tipo |
|---|---|---|
| **Parte 1** | Contexto histórico (Unidad 1) | Respuestas en celdas Markdown |
| **Parte 2** | DataFrames — carga, columnas, filtros y agregaciones | Código Python |
| **Parte 3** | JOINs con DataFrame API (INNER, LEFT, RIGHT, FULL OUTER) | Código Python |
| **Parte 4** | Spark SQL — consultas con `spark.sql()` | Código SQL |
| **Parte 5** | Comparación DataFrame API vs Spark SQL | Código Python + SQL |
| **Parte 6** | Plan de ejecución con `.explain()` | Código + reflexión en Markdown |

---

## Plataformas disponibles

| Plataforma | Estado | Observación |
|---|---|---|
| **Databricks (versión gratuita)** | ✅ Recomendada | Cómputo serverless incluido, sin instalación |
| **Google Colab** | ✅ Opcional | Requiere instalar PySpark en cada sesión |
| **Local / VS Code** | ✅ Opcional | Requiere entorno virtual con PySpark instalado |

---

## Configuración por entorno

### Databricks (recomendado)

1. Creá una cuenta gratuita en [databricks.com](https://www.databricks.com/try-databricks) si no tenés una.
2. Desde el workspace, creá un nuevo **Notebook** o importá `trabajo_practico_u1_u4.ipynb` desde el menú **Workspace > Import**.
3. Subí los archivos CSV:
   - En el menú lateral: **Catalog > + Add data > Upload files to DBFS**
   - Subí `ventas.csv` y `productos.csv`
   - Las rutas resultantes serán `/FileStore/tables/ventas.csv` y `/FileStore/tables/productos.csv`
4. En el notebook, asegurate de que en la **Parte 0** quede activa la sección de **Databricks** (ya viene así por defecto).
5. Seleccioná un clúster **Serverless** para ejecutar. No necesitás configurar nada más — `spark` ya está disponible.

### Google Colab (opcional)

1. Subí el notebook a [colab.research.google.com](https://colab.research.google.com).
2. En la **Parte 0**, descomentá la **Celda B** y comentá la Celda A.
3. En el **Ejercicio 2.1**, descomentá el bloque de rutas para Colab y subí los archivos cuando se ejecute `files.upload()`.
4. Al terminar cada sesión, los archivos se pierden — no es necesario ejecutar `spark.stop()`.

### Local / VS Code (opcional)

1. Activá el entorno virtual del proyecto:
   ```powershell
   # Windows (PowerShell)
   Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
   .venv\Scripts\Activate.ps1
   ```
2. En la **Parte 0**, descomentá la **Celda C** y comentá la Celda A.
3. En el **Ejercicio 2.1**, descomentá el bloque de rutas para Local.
4. Al finalizar, ejecutá la última celda del notebook para detener la SparkSession.

---

## Instrucciones para completar el trabajo

1. Configurá tu entorno siguiendo los pasos de la sección anterior.
2. Abrí `trabajo_practico_u1_u4.ipynb` y completá tu nombre y apellido en la primera celda.
3. Resolvé cada ejercicio **en la celda indicada**. No agregues celdas adicionales salvo que sea estrictamente necesario.
4. Para las preguntas teóricas (Parte 1 y reflexiones), escribí tu respuesta en las celdas Markdown donde dice `✏️ Escribí tu respuesta aquí`.
5. Antes de entregar, verificá que el notebook ejecuta sin errores de principio a fin:
   - **Databricks:** `Run all` desde el menú superior
   - **Colab / Local:** `Kernel > Restart & Run All`
6. Cada celda de código debe mostrar su resultado con `.show()`, `print()` o similar.

---

## Criterios de evaluación

| Parte | Criterio | Peso |
|---|---|---|
| Parte 1 | Comprensión del contexto histórico y evolución tecnológica | 15% |
| Parte 2 | Correcta definición de schema, transformaciones y agregaciones | 20% |
| Parte 3 | Implementación correcta de los 4 tipos de JOIN y análisis de resultados | 30% |
| Parte 4 | Consultas SQL correctas y uso adecuado de funciones de agregación | 20% |
| Parte 5 | Equivalencia correcta entre ambas APIs | 10% |
| Parte 6 | Ejecución de `.explain()` y comprensión básica del plan | 5% |

**Penalizaciones:**
- Notebook que no ejecuta de principio a fin: **−20 puntos**
- Ejercicios sin resultado visible (sin `.show()` / `print()`): **−5 puntos por ejercicio**
- Preguntas teóricas sin responder: **0 puntos en esa pregunta**

---

## Formato y modalidad de entrega

- **Modalidad:** Individual
- **Formato:** Un único archivo `.ipynb` con todos los ejercicios resueltos y las salidas visibles (celdas ejecutadas)
- **Nombre del archivo:** `tp_u1_u4_APELLIDO_NOMBRE.ipynb`  
  Ejemplo: `tp_u1_u4_GARCIA_ANA.ipynb`

### ¿Cómo exportar el notebook?

**Desde Databricks:**
- `File > Export > IPython Notebook (.ipynb)`
- Verificá que el archivo exportado tenga las salidas de las celdas visibles.

**Desde Google Colab:**
- `Archivo > Descargar > Descargar .ipynb`

**Desde Local / VS Code:**
- El archivo `.ipynb` ya está en tu equipo, solo asegurate de haberlo guardado con las salidas.

### ¿Dónde entregar?

Subí el archivo al aula virtual en el espacio habilitado para este trabajo práctico.

> **Solo entregues el notebook `.ipynb`**, no los archivos CSV.

---

## Fecha de entrega

| Instancia | Fecha |
|---|---|
| Entrega regular | A confirmar por el docente |
| Recuperatorio | A confirmar por el docente |

---

## Consultas

Las dudas sobre el enunciado o los datos deben realizarse **antes de la fecha de entrega**, a través del foro de consultas del aula virtual o en clase.

No se responderán consultas técnicas el día de la entrega.
