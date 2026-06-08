FROM apache/airflow:2.7.1-python3.10 AS builder

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=airflow:airflow dags/ ./dags/
COPY --chown=airflow:airflow plugins/ ./plugins/
COPY --chown=airflow:airflow scripts/ ./scripts/

USER root
RUN chmod +x ./scripts/entrypoint.sh
USER airflow

ENTRYPOINT ["./scripts/entrypoint.sh"]
CMD ["webserver"]
