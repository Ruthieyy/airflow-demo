from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'data_eng',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print("Hello from PySpark + Airflow demo!")

with DAG(
    dag_id='example_demo',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['demo'],
) as dag:
    start = DummyOperator(task_id='start')
    hello = PythonOperator(
        task_id='say_hello',
        python_callable=print_hello,
    )
    end = DummyOperator(task_id='end')
    start >> hello >> end
