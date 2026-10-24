"""Set Airflow env before any test imports Airflow."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow"))
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(ROOT / "dags")
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "true"
