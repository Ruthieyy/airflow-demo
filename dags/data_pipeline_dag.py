"""Daily local orders pipeline.

ingest copies data/raw/orders_sample.csv to data/staging/orders.csv.
quality fails the task when a row or the schema breaks the checks in
orders_tasks.validate_orders.
etl writes data/curated/orders.csv and data/curated/orders.parquet.

This DAG does not talk to S3, Great Expectations, or Spark.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Airflow adds this folder to sys.path while parsing. The insert also lets
# `python dags/data_pipeline_dag.py` import the sibling module.
_DAG_DIR = str(Path(__file__).resolve().parent)
if _DAG_DIR not in sys.path:
    sys.path.insert(0, _DAG_DIR)

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from orders_tasks import etl, ingest, quality

with DAG(
    dag_id="data_pipeline",
    default_args={
        "owner": "katrina",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description=(
        "Local sample pipeline: stage orders_sample.csv, fail on bad rows, "
        "and write curated CSV and Parquet."
    ),
    schedule="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["portfolio", "local"],
) as dag:
    ingest_task = PythonOperator(task_id="ingest", python_callable=ingest)
    quality_task = PythonOperator(task_id="quality", python_callable=quality)
    etl_task = PythonOperator(task_id="etl", python_callable=etl)

    ingest_task >> quality_task >> etl_task
