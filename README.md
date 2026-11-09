# Proyecto Integrador: Orquestación de Pipeline ETL con Apache Airflow

Este repositorio contiene la implementación y documentación paso a paso para la configuración, creación y ejecución de un pipeline ETL (Extracción, Transformación y Carga) utilizando **Apache Airflow 3** en un entorno virtualizado Linux **Debian 13 (Trixie)** en VMware.

---

## Introducción

### 1. ¿Qué es Apache Airflow?
Apache Airflow es una plataforma de código abierto diseñada para programar, orquestar y monitorear flujos de trabajo complejos. Permite definir tareas y sus dependencias mediante código en Python.

### 2. Conceptos Clave
* **DAG (Directed Acyclic Graph):** Un Grafo Acíclico Dirigido es una colección de tareas ordenadas que se ejecutan en una secuencia lógica sin bucles infinitos.
* **Operadores:** Son las plantillas que definen el trabajo a realizar en cada nodo del DAG.
  * `BashOperator`: Ejecuta comandos de shell en el sistema operativo.
  * `PythonOperator`: Ejecuta funciones personalizadas de Python.
* **XComs (Cross-Communications):** Mecanismo nativo de Airflow que permite a las tareas compartir pequeñas cantidades de datos entre sí durante la ejecución.
* **Pipeline (Pipeline de Datos):** Es la secuencia automatizada de pasos (extracción, transformación, validación y carga) por la que viajan los datos desde sus fuentes de origen hasta su destino final.

---

## Descripción del Script (`mi_primer_dag.py`)

El flujo ETL desarrollado simula la recolección y procesamiento de datos meteorológicos mediante 4 tareas conectadas secuencialmente:

1. **`tarea_inicio` (`BashOperator`):** Emite un mensaje inicial en la consola de ejecución notificando el arranque del pipeline.
2. **`tarea_extraer` (`PythonOperator`):** Ejecuta la función `extraer_datos()`, la cual simula la lectura de una API devolviendo datos de temperatura, humedad y ciudad.
3. **`tarea_transformar` (`PythonOperator`):** Ejecuta `transformar_datos()`. Utiliza `XCom` (`xcom_pull`) para recibir la información de la tarea anterior, convierte la temperatura de grados Celsius a Fahrenheit y normaliza el texto.
4. **`tarea_fin` (`BashOperator`):** Finaliza el flujo emitiendo un mensaje de confirmación de éxito.

---

## Guía Paso a Paso del Proceso (Evidencia de Ejecución)

A continuación se detalla el proceso técnico realizado para desplegar y verificar el proyecto:

### Paso 1: Inicialización del Entorno y Servidor Airflow
Se accedió al directorio del proyecto en Debian, se activó el entorno virtual de Python (`venv`) y se inició el servidor Airflow en modo `standalone`.
<img width="747" height="488" alt="iniciarairworkflw" src="https://github.com/user-attachments/assets/d6682a95-ceb5-46b1-a3b4-5a6310502248" />

---

### Paso 2: Creación del Directorio de DAGs
Se creó la estructura de carpetas `~/airflow/dags` en el sistema de archivos y se preparó el archivo de script `mi_primer_dag.py` mediante la terminal.
<img width="953" height="566" alt="creacionpy" src="https://github.com/user-attachments/assets/f5875c87-8cd8-40a0-a812-ca9791794d90" />

---

### Paso 3: Redacción del Código DAG
Se programó el pipeline ETL en Python utilizando el editor de texto `nano`, definiendo los argumentos principales, las funciones de transformación y la secuencia lógica del grafo (`t1 >> t2 >> t3 >> t4`).
<img width="976" height="605" alt="py" src="https://github.com/user-attachments/assets/f1b9876a-cd6a-4cad-8bd6-db2dbd82a017" />

---

### Paso 4: Consulta de Credenciales e Inicio de Sesión
Se consultó la contraseña autogenerada para el usuario `admin` y se accedió al panel de autenticación web de Airflow a través de Firefox en `http://localhost:8080`.
<img width="1476" height="970" alt="iniciar sesion" src="https://github.com/user-attachments/assets/3a5bec8f-e781-4259-a302-c7eac86fdef5" />

---

### Paso 5: Acceso al Panel Principal (Dashboard)
Una vez autenticado, se visualizó el panel principal de Airflow confirmando que todos los componentes centrales (Base de datos, Scheduler, Triggerer y DagProcessor) se encontraban activos y con estado de salud óptimo.
<img width="1153" height="764" alt="inicio" src="https://github.com/user-attachments/assets/777a136c-a8d1-47d8-9627-f1dded321fec" />

---

### Paso 6: Localización del DAG en la Interfaz
En el menú lateral **DAGs**, se verificó la presencia de `dag_ejemplo_etl_estudiante` junto a las etiquetas personalizadas (`educativo`, `proyecto`) y su estado inicial apagado.
<img width="1143" height="745" alt="activar t" src="https://github.com/user-attachments/assets/0330d324-09d3-48e7-837d-98963c194f70" />

---

### Paso 7: Activación y Disparo Manual (Trigger)
Se activó el interruptor del DAG para habilitar su programación y se desplegó el menú flotante para forzar su ejecución manual (*Trigger DAG*).
<img width="1143" height="750" alt="seleccion" src="https://github.com/user-attachments/assets/d82e3703-3f8c-4769-91ba-9bcb75a0be11" />

---

### Paso 8: Confirmación de Ejecución en Progreso
El sistema confirmó la activación del pipeline mediante una notificación en verde, mostrando las primeras tareas en cola dentro del contador de ejecuciones.
<img width="1141" height="752" alt="activar" src="https://github.com/user-attachments/assets/41b3a150-dee2-4bfc-b51c-93ff68e796c9" />

---

### Paso 9: Verificación de Ejecución Exitosa
Se ingresó a la vista detallada de la ejecución manual. Se confirmó que las 4 instancias de tareas (`tarea_inicio`, `tarea_extraer`, `tarea_transformar` y `tarea_fin`) cambiaron a estado **`Exitoso`** (en verde), completando el pipeline de forma limpia en 7.17 segundos.
<img width="1140" height="750" alt="final" src="https://github.com/user-attachments/assets/18a16bea-92eb-47fb-8d4f-dcf74a2c1196" />

---

## 🎯 Conclusión
El proyecto demostró con éxito la capacidad de desplegar un entorno de orquestación de datos moderno con Apache Airflow, la creación de componentes personalizados en Python y la gestión completa del ciclo de vida de un pipeline de datos desde la interfaz de comandos y su consola web.
