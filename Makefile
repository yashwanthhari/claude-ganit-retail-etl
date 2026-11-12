.PHONY: setup data run report test clean

PY ?= python

setup:
	$(PY) -m venv .venv
	.venv/bin/pip install -e ".[dev]"

data:
	$(PY) -m lakehouse.pipeline generate

run:
	$(PY) -m lakehouse.pipeline generate bronze silver gold warehouse report

report:
	$(PY) -m lakehouse.pipeline report

test:
	$(PY) -m pytest -q

clean:
	rm -rf data spark-warehouse metastore_db derby.log
