# Local orders pipeline

One Apache Airflow DAG, `data_pipeline`, that stages a sample orders CSV, checks it, and writes curated CSV and Parquet files on the local disk.

## What is implemented

The DAG lives in `dags/data_pipeline_dag.py`. Task functions live in `dags/orders_tasks.py`.

| Task | What it does |
| --- | --- |
| `ingest` | Reads `data/raw/orders_sample.csv` and writes an unchanged copy to `data/staging/orders.csv`. Fails if that file is missing, empty, or missing a required column. |
| `quality` | Checks the staged file. Writes `data/staging/quality_report.json`. Raises `DataQualityError` when any check fails, which fails the Airflow task. |
| `etl` | Runs the same checks, then writes `data/curated/orders.csv` and `data/curated/orders.parquet` with typed amount, date, and lowercased status. |

Task order is `ingest` → `quality` → `etl`.

Checks in `quality` and `etl`:

- Required columns: `order_id`, `customer_id`, `amount`, `order_date`, `status`
- `order_id` and `customer_id` are present
- `order_id` is unique
- `amount` is numeric and greater than or equal to 0
- `order_date` parses as `YYYY-MM-DD`
- `status` is one of `shipped`, `pending`, `cancelled`, `returned` (comparison is case-insensitive)

`data/raw/orders_sample.csv` passes these checks (five synthetic rows). `data/raw/orders_bad_sample.csv` is a fixture for the tests. The DAG does not read it.

DAG settings:

- Schedule: `@daily`, set with the Airflow 3 `schedule` argument
- Start date: 2026-01-01 UTC
- `catchup=False`, so a new local scheduler does not backfill every day since the start date
- Retries: 1, with a 1 minute delay
- Owner: `katrina`
- Tags: `portfolio`, `local`

Override the repository root with `DATA_PIPELINE_HOME` if the process working directory is not this repo. The default root is the parent of `dags/`.

## Requirements

Python 3.12. Direct packages are pinned in `requirements.txt`. The install also uses the Airflow 3.3.2 constraint file for Python 3.12 so transitive dependencies stay on the set Airflow tested.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Tests

```bash
pytest
```

`tests/test_dag_integrity.py` loads `dags/` through Airflow's `DagBag` and checks that the DAG imports, the three tasks are wired in order, and the schedule is `@daily`. `tests/test_orders_tasks.py` runs the task callables against the sample CSV and checks that the bad fixture fails `quality` before any curated file is written.

## Run locally

`airflow standalone` is the local run path. It uses SQLite and starts the API server, scheduler, dag processor, and triggerer. The UI is at <http://localhost:8080>.

Activate the virtualenv first. Standalone starts child processes by the command name `airflow`, so that executable has to be on `PATH`. Calling `.venv/bin/airflow standalone` without activating the environment fails those children with `FileNotFoundError`.

From the repository root:

```bash
source .venv/bin/activate
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"

airflow db migrate
airflow dags reserialize
airflow dags list
airflow dags test data_pipeline 2026-10-01
```

`dags list` is empty until something has parsed the folder. `dags reserialize` does that parse and writes the DAG to the metadata database. `dags test` runs one logical date without the scheduler loop, including while the DAG is paused. After it succeeds, `data/curated/orders.parquet` and `data/curated/orders.csv` are present. Staging and curated outputs are gitignored.

Leave the UI running with:

```bash
airflow standalone
```

Log in as `admin`. Airflow writes the generated password to `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated` and prints it the first time that file is created. New DAGs are paused. Unpause before expecting the daily schedule to create a run:

```bash
airflow dags unpause data_pipeline
```

Stop standalone with Ctrl-C. `.airflow/` is gitignored.

`dags` commands log a graphviz import warning. This pipeline does not render a graph; the tasks still run. Standalone also logs a SimpleAuthManager warning because the API listens on `0.0.0.0`. That warning is expected for this local process.

## Limitations / Next steps

This is a local file pipeline with one synthetic CSV. It is small enough to run in a virtualenv and to review in an interview.

- Ingest reads a file from disk. Next step: replace that read with an S3 download (Airflow S3 hook or `boto3`) once a bucket and credentials exist, and keep the same staging path.
- Quality is a pandas function with a fixed rule list. Next step: move those rules into a suite that can be versioned and run against a warehouse table.
- The transform is pandas writing one Parquet file. Next step: run the same column mapping in Spark on EMR or Glue when the input no longer fits in memory, and partition the output by `order_date`.
- The schedule does not use the data interval. Every run reads the same sample file. Next step: pass `logical_date` into the task and select the source partition for that day.
- Local execution is `airflow standalone` (SQLite, one machine). Next step: a Postgres-backed executor and a container definition when more than one person needs to run it.
- There is no catalog, no incremental merge, and no alert when `quality` fails beyond Airflow marking the task failed.
