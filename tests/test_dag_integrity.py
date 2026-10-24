"""Import the DAG the way Airflow's dag processor does and check its shape."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow"))
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(ROOT / "dags")
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "true"

from airflow.dag_processing.dagbag import DagBag

DAG_FILE = ROOT / "dags" / "data_pipeline_dag.py"
TASK_FILE = ROOT / "dags" / "orders_tasks.py"


def test_dagbag_imports_data_pipeline() -> None:
    dagbag = DagBag(dag_folder=str(ROOT / "dags"))

    assert dagbag.import_errors == {}
    assert list(dagbag.dags) == ["data_pipeline"]

    dag = dagbag.dags["data_pipeline"]
    assert dag.schedule == "@daily"
    assert dag.catchup is False
    assert "local" in dag.tags
    assert [task.task_id for task in dag.tasks] == ["ingest", "quality", "etl"]
    assert dag.task_dict["ingest"].downstream_task_ids == {"quality"}
    assert dag.task_dict["quality"].downstream_task_ids == {"etl"}
    assert dag.task_dict["etl"].downstream_task_ids == set()


def test_sources_use_the_airflow_3_schedule_api() -> None:
    dag_source = DAG_FILE.read_text()
    task_source = TASK_FILE.read_text()
    combined = dag_source + task_source

    assert "schedule_interval" not in combined
    assert 'schedule="@daily"' in dag_source
    for unused in ("boto3", "great_expectations", "pyspark", "SparkSession"):
        assert unused not in combined
