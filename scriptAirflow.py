from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Argumentos por defecto del DAG
default_args = {
    'owner': 'estudiante',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Funciones de Python para las tareas
def extraer_datos():
    print("[EXTRAER] Obteniendo datos desde la API fuente...")
    datos = {"temperatura": 24.5, "humedad": 60, "ciudad": "Guadalajara"}
    return datos

def transformar_datos(**context):
    ti = context['ti']
    datos_brutos = ti.xcom_pull(task_ids='tarea_extraer')
    print(f"[TRANSFORMAR] Procesando datos de {datos_brutos['ciudad']}...")
    
    temp_f = (datos_brutos['temperatura'] * 9/5) + 32
    datos_procesados = {
        "ciudad": datos_brutos['ciudad'].upper(),
        "temp_farenheit": temp_f,
        "humedad": datos_brutos['humedad']
    }
    print(f"[RESULTADO] Datos transformados: {datos_procesados}")

# Definición del DAG
with DAG(
    dag_id='dag_ejemplo_etl_estudiante',
    default_args=default_args,
    description='Un DAG simple de ejemplo ETL para el proyecto integrador',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['proyecto', 'educativo'],
) as dag:

    tarea_inicio = BashOperator(
        task_id='tarea_inicio',
        bash_command='echo "Iniciando Pipeline ETL con Apache Airflow..."'
    )

    tarea_extraer = PythonOperator(
        task_id='tarea_extraer',
        python_callable=extraer_datos
    )

    tarea_transformar = PythonOperator(
        task_id='tarea_transformar',
        python_callable=transformar_datos,
        provide_context=True
    )

    tarea_fin = BashOperator(
        task_id='tarea_fin',
        bash_command='echo "Pipeline completado con éxito."'
    )

    # Orden de ejecución
    tarea_inicio >> tarea_extraer >> tarea_transformar >> tarea_fin