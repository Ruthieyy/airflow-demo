from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'katrina',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def ingest_task():
    import boto3
    s3 = boto3.client('s3')
    print("① 从S3摄取数据...")

def quality_task():
    import great_expectations as ge
    print("② 数据质量检查...")

def etl_task():
    from pyspark.sql import SparkSession
    print("③ PySpark ETL处理...")

with DAG(
    'data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
) as dag:

    t1 = PythonOperator(task_id='ingest', python_callable=ingest_task)
    t2 = PythonOperator(task_id='quality', python_callable=quality_task)
    t3 = PythonOperator(task_id='etl', python_callable=etl_task)

    t1 >> t2 >> t3
