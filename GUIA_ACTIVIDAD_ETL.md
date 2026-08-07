# Taller Práctico: ETL Clínico con Generadores en Python

Este documento contiene la descripción de la actividad de laboratorio, las instrucciones de entrega y la rúbrica de evaluación para el taller de ETL basado en streaming de datos clínicos.

---

## Descripción de la Actividad

### Contexto

La **Clínica San José** necesita procesar un archivo masivo de logs de admisiones médicas (`logs_admisiones_masivas.csv`) con más de 100,000 registros históricos. El servidor de integración actual tiene recursos limitados y dispone de un **máximo de 20 MB de RAM** para esta tarea. El sistema actual colapsa debido a que intenta cargar el archivo completo en memoria, provocando errores de Out-Of-Memory (OOM).

### Objetivo

Diseñar e implementar un pipeline **ETL (Extract, Transform, Load)** en Python utilizando **Generadores (`yield`)** para procesar el dataset de manera streaming (lote a lote). Esto evitará el consumo excesivo de memoria RAM y garantizará una carga eficiente en una base de datos local SQLite (o PostgreSQL como alternativa avanzada).

---

## Entregables de la Actividad

Para que la actividad sea calificada, los estudiantes deberán entregar un archivo comprimido con el nombre `Taller_ETL_Apellido_Nombre.zip` que contenga únicamente:

1. **Código fuente completo (`plantilla_taller.py`)**: Con todos los bloques `# TODO` resueltos y el pipeline funcional.
2. **Captura de pantalla del output**: Imagen de la consola que muestre el log de lotes, el tiempo total y el **Pico Máximo de Memoria RAM (< 20 MB)**.
3. **Este archivo completo (`GUIA_ACTIVIDAD_ETL.md`)**: Completado con las respuestas a las preguntas de análisis y teoría de la sección final.

---

## Rúbrica de Evaluación (Ponderación Total: 100%)

| Criterio | Ponderación | Aspecto a Evaluar |
| :--- | :---: | :--- |
| **1. Extractor con `yield`** | **20%** | Implementación correcta y perezosa en lotes exactos y control del remanente final. |
| **2. Transformación de Datos** | **25%** | Aplicación de reglas de negocio clínicas (limpieza, comisión, costo neto y alerta de gravedad) con manejo de excepciones. |
| **3. Carga en Base de Datos** | **15%** | Carga masiva eficiente (`executemany` o similar) y control de transacciones por lote. |
| **4. Monitoreo de RAM** | **10%** | Demostración de consumo de RAM constante por debajo de 20 MB validado mediante captura de pantalla. |
| **5. Calidad del Código** | **15%** | Código limpio, estructurado bajo PEP 8, modular y debidamente documentado. |
| **6. Respuestas al Cuestionario** | **15%** | Explicación clara del streaming, justificación de cargas agrupadas y respuestas fundamentadas del cuestionario de este archivo. |

---

> [!IMPORTANT]
> **Nota para el Estudiante:** El límite de consumo de memoria RAM (<20 MB) es un requerimiento crítico. Si la solución procesa los registros pero supera el límite de RAM, se penalizará severamente en los criterios de Extractor y Monitoreo.

---

## Cuestionario de Evaluación y Análisis Técnico

### Nombre del Estudiante:

Leo Sornoza

### 1. Funcionamiento de la Evaluación Perezosa (Lazy Evaluation)

Describa cómo funciona el extractor implementado y de qué manera la instrucción `yield` en Python evita el agotamiento de memoria RAM en el servidor.

> **Respuesta:** El extractor abre el archivo CSV y utiliza `csv.DictReader` para leer una fila a la vez. Cada fila se agrega temporalmente a una lista hasta completar un lote de 2.000 registros. Cuando el lote alcanza ese tamaño, `yield` lo entrega al pipeline y pausa la función sin cerrar el archivo ni perder la posición de lectura. Después, la lista se reinicia para preparar el siguiente lote. Si al final quedan registros que no completan un lote, también se entregan mediante `yield`. De esta manera, el programa nunca carga las 100.000 filas simultáneamente en memoria, sino solamente el lote que está procesando en ese momento.

### 2. Justificación de la Inserción por Lotes (Batch Loading)

Explique el impacto que tiene en el rendimiento de la base de datos agrupar los registros para su carga en lugar de insertarlos individualmente registro por registro.

> **Respuesta:** La inserción por lotes mejora el rendimiento porque agrupa muchos registros en una sola operación. Esto reduce la cantidad de comunicaciones entre Python y la base de datos, así como el número de confirmaciones de transacciones. En el pipeline se utiliza `executemany` para insertar todas las filas válidas del lote y luego se realiza un solo `commit`. Si se insertara cada registro individualmente, existirían miles de llamadas a la base de datos y el proceso sería mucho más lento. Los lotes también permiten mantener un equilibrio entre velocidad y consumo de memoria.

### 3. Diferencias en Memoria: `yield` vs `return`

Detalle la diferencia técnica en la asignación de memoria RAM entre una función que genera y acumula una lista en memoria (`return`) y una función generadora.

> **Respuesta:** Una función tradicional que utiliza `return` normalmente construye primero una lista completa y conserva todos sus elementos en memoria antes de devolver el resultado. Por esta razón, su consumo de RAM aumenta conforme crece el archivo. En cambio, una función generadora con `yield` produce los datos bajo demanda. La función conserva solamente su estado de ejecución y el lote actual, se pausa después de cada entrega y continúa cuando el siguiente lote es solicitado. Por lo tanto, no necesita almacenar el dataset completo y utiliza una cantidad de memoria mucho menor.

### 4. Escalabilidad de la Solución

¿Por qué el consumo de memoria RAM medido en la consola se mantiene constante y bajo sin importar el tamaño total del archivo procesado (sea de 100,000 o de 10,000,000 de registros)?

> **Respuesta:** El consumo de memoria depende principalmente del tamaño configurado para cada lote y no de la cantidad total de filas del archivo. El pipeline mantiene en memoria el lote crudo y su resultado transformado; después de cargarlo en la base de datos, continúa con el siguiente. Como el tamaño del lote permanece en 2.000 registros, la memoria utilizada se mantiene aproximadamente constante aunque el archivo tenga 100.000 o 10.000.000 de filas. Un archivo más grande aumentará el tiempo y la cantidad de lotes procesados, pero no obligará a cargar más registros simultáneamente en RAM.

### 5. Optimización Tecnológica (`executemany`)

¿Cuál es la diferencia de desempeño y uso de conexiones en la base de datos entre usar el método `executemany` y usar un bucle iterativo que llame individualmente a `execute`?

> **Respuesta:** `executemany` permite enviar una sentencia SQL junto con todas las tuplas de un lote usando el mismo cursor y la misma conexión. Esto disminuye el número de llamadas realizadas desde Python, reduce la sobrecarga del controlador y permite confirmar la transacción una sola vez por lote. En cambio, un bucle con `execute` realiza una llamada separada por cada registro y, si además se ejecuta un `commit` por fila, aumenta considerablemente el costo de comunicación y de manejo de transacciones. Por ello, `executemany` es más rápido y eficiente para cargas masivas.
