# Retail Lakehouse Lab

A small but realistic data-engineering project to get comfortable with the kind of stack and flow used on
consulting data-platform projects: **PySpark + Delta Lake medallion pipeline, a SQL warehouse, data-quality
quarantine, orchestration and CI**. Everything runs locally on free tools.
Background on the company and how this maps to its stack: [docs/ganit-analysis.md](docs/ganit-analysis.md).

## What it does

A fictional retailer's systems drop messy files into a landing zone. The pipeline turns them into
analytics-ready tables.

```
 generate_data.py         bronze.py            silver.py              gold.py            warehouse.py
 ┌──────────────┐   ┌────────────────┐   ┌─────────────────┐   ┌───────────────┐   ┌───────────────────┐
 │ landing/     │   │ Delta, raw     │   │ Delta, typed,   │   │ Delta star    │   │ DuckDB            │
 │  customers   │──▶│ strings +      │──▶│ cleaned, deduped│──▶│ schema:       │──▶│ core.* (gold)     │
 │  products    │   │ lineage cols,  │   │ MERGE upserts,  │   │ dim_* + fact  │   │ quarantine.*      │
 │  stores      │   │ append-only,   │   │ DQ rules ──▶    │   │ surrogate     │   │ marts.* (SQL)     │
 │  orders/*.jsonl│ │ file manifest  │   │ quarantine      │   │ keys, parquet │   │ daily_sales, RFM… │
 └──────────────┘   └────────────────┘   └─────────────────┘   └───────────────┘   └───────────────────┘
```

| Concept | Where |
|---|---|
| Metadata-driven, re-runnable ingestion | `bronze.py`, `state.py`, `config/pipeline.yaml` |
| Medallion layers on Delta Lake | `bronze.py`, `silver.py`, `gold.py` |
| Cleaning, typing, dedup (window functions) | `silver.py` |
| MERGE upserts with late-arriving updates | `delta_utils.py` |
| Declarative data quality and quarantine | `dq.py`, `marts.dq_summary` |
| Star schema, surrogate keys, unknown member | `gold.py` |
| Warehouse load + SQL marts (window functions, RFM) | `warehouse.py`, `sql/marts/` |
| Orchestration | `pipeline.py` CLI, `orchestration/airflow_dag.py` |
| Tests and CI | `tests/`, `.github/workflows/ci.yml` |

The generated data deliberately includes problems you will meet in real projects: duplicate deliveries,
mixed date formats, malformed emails, missing keys, negative quantities, unknown products and customers,
and status changes that arrive a day late.

## Requirements (all free)

- Python **3.10 – 3.12** (PySpark 3.5 does not support 3.13 yet)
- Java **17** (`java -version`). Install Temurin/OpenJDK 17 if missing.
- Internet on the **first** run only: Spark downloads the Delta jars from Maven.
- Windows users: use WSL2 (Ubuntu). Spark on native Windows needs extra setup.

## Quick start

```bash
git clone <your-repo-url> ganit-retail-lakehouse-lab
cd ganit-retail-lakehouse-lab
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

python -m lakehouse.pipeline          # generate -> bronze -> silver -> gold -> warehouse -> report
```

The first run takes a few minutes (jar download, Spark start-up). A smaller run:
`python -m lakehouse.pipeline --days 3 --orders-per-day 100`.

Run single stages: `python -m lakehouse.pipeline silver gold`. Stages: `generate bronze silver gold warehouse report`.

### Explore the results

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("data/warehouse/retail.duckdb", read_only=True)
con.sql("SELECT * FROM marts.dq_summary").show()
con.sql("SELECT category, SUM(net_sales) AS net FROM marts.category_performance GROUP BY 1 ORDER BY 2 DESC").show()
PY
```

Or use the DuckDB CLI / DBeaver on `data/warehouse/retail.duckdb`. Delta tables are under `data/lake/`; in a
PySpark shell try `spark.read.format("delta").load("data/lake/silver/orders")` and
`DESCRIBE HISTORY delta.\`data/lake/silver/orders\``.

### Tests

```bash
pytest -q
```

Generator and SQL-mart tests need no Spark; Spark/Delta tests start a local session (and skip themselves if
PySpark is not installed).

## Layout

```
config/pipeline.yaml        paths, generator size, Spark settings
src/lakehouse/              pipeline code (one module per layer)
sql/marts/                  warehouse SQL models, run in filename order
sql/legacy/                 T-SQL stored procedure used as a migration exercise
orchestration/              optional Airflow DAG
tests/                      pytest
docs/                       company analysis, exercises
data/                       generated at run time (git-ignored)
```

## Next steps

Work through [docs/exercises.md](docs/exercises.md): idempotency checks, SCD2, migrating the stored procedure,
Delta `OPTIMIZE`/time travel, dbt, Airflow, streaming.

## Troubleshooting

- `JAVA_HOME is not set` / `Unsupported class file major version`: use Java 17.
- `ModuleNotFoundError: delta`: reinstall with `pip install -e ".[dev]"` inside the venv.
- Out-of-memory: lower `generator.orders_per_day`, or set `spark.driver_memory` in `config/pipeline.yaml`.
- Start clean: `make clean` (or delete `data/`).
