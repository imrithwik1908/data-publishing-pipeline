.PHONY: install test lint fmt fetch gate clean demo reproduce report airflow-install airflow-test airflow
MONTHS ?= 2024-11 2024-12 2025-01

install:
	uv sync

test:
	uv run pytest -q

lint:
	uv run ruff check . && uv run ruff format --check .

fmt:
	uv run ruff format . && uv run ruff check --fix .

fetch:  ## download raw TLC files (about 60 MB each) and the zone lookup
	mkdir -p data/raw
	for m in $(MONTHS); do \
	  test -f data/raw/yellow_tripdata_$$m.parquet || curl -sfo data/raw/yellow_tripdata_$$m.parquet \
	    https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$$m.parquet; done
	test -f data/raw/taxi_zone_lookup.csv || curl -sfo data/raw/taxi_zone_lookup.csv \
	  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

gate: fetch  ## Day-3 gate: real months end to end
	for m in $(MONTHS); do uv run pipeline run $$m; done
	uv run pipeline status

clean:
	rm -rf data/warehouse.duckdb data/warehouse.lock data/landing data/quarantine

demo:  ## fresh-clone walkthrough on real data (downloads about 180 MB once)
	uv sync -q
	uv run python scripts/demo.py

reproduce:  ## measure every number in the README -> results/
	uv sync -q
	uv run python scripts/reproduce.py

report:
	uv run pipeline report

airflow-install:  ## optional: Airflow in its own venv (about 370 MB)
	uv venv -q --python 3.12 .venv-airflow
	VIRTUAL_ENV=.venv-airflow uv pip install -q "apache-airflow==3.4.0" -e . \
	  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt

AIRFLOW_ENV = AIRFLOW_HOME=$(CURDIR)/data/airflow AIRFLOW__CORE__DAGS_FOLDER=$(CURDIR)/dags \
	AIRFLOW__CORE__LOAD_EXAMPLES=False DPP_DATA_DIR=$(CURDIR)/data

airflow-test:  ## run the DAG once, no scheduler: make airflow-test MONTH=2025-02
	$(AIRFLOW_ENV) .venv-airflow/bin/airflow db migrate >/dev/null
	$(AIRFLOW_ENV) .venv-airflow/bin/airflow dags test monthly_trips_wap --conf '{"month":"$(MONTH)"}'

airflow:  ## Airflow UI at http://localhost:8080 (demo only)
	$(AIRFLOW_ENV) .venv-airflow/bin/airflow standalone
