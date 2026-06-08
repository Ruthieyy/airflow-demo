.PHONY: help build up down test clean

help:
	@echo "Available commands:"
	@echo "  build     Build Docker image"
	@echo "  up        Start all services (detached)"
	@echo "  down      Stop all services"
	@echo "  test      Run DAG import tests locally"
	@echo "  clean     Remove volumes and containers"

build:
	docker compose build

up:
	docker compose up -d
	@echo "Airflow webserver running at http://localhost:8080 (admin/admin)"

down:
	docker compose down

test:
	python -c "from airflow.models import DagBag; dag_bag = DagBag(include_examples=False); assert len(dag_bag.import_errors)==0, dag_bag.import_errors"

clean:
	docker compose down -v
