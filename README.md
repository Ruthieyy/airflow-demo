# Airflow DAG — Data Pipeline Orchestration

Apache Airflow DAG orchestrating end-to-end data pipeline.

## Pipeline Order
Ingestion → Data Quality Check → PySpark ETL

## DAG Config
- Schedule: Daily (@daily)
- Retries: 3 times per task
- Retry delay: 5 minutes

## Why Airflow?
Automates task execution, handles retries, and ensures each step
only runs after the previous one succeeds.
