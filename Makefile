.PHONY: install test lint fmt fetch gate clean
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
