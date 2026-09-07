.PHONY: install lint test spark-check pipeline-dev clean

PYTHON ?= python
VENV ?= .venv
PIP := $(VENV)/Scripts/pip
PY := $(VENV)/Scripts/python

install:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

lint:
	$(PY) -m ruff check src tests mentoring

test:
	$(PY) -m pytest -q

spark-check:
	$(PY) -c "from saas_pipeline.spark import build_spark; s=build_spark('spark-check'); print(s.version); s.stop()"

pipeline-dev:
	$(PY) -m saas_pipeline.cli --env dev --tenant all --start-date 2025-01-01 --end-date 2025-06-30 --layer all

clean:
	rm -rf data spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
