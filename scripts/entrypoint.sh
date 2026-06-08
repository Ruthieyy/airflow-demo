#!/usr/bin/env bash

if [ -n "$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" ]; then
    echo "Waiting for database..."
    while ! airflow db check; do
        sleep 1
    done
fi

airflow db upgrade

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin \
    || echo "User already exists"

exec airflow "$@"

