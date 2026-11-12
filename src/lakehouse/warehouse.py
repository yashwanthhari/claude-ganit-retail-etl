"""Load gold into DuckDB (the free stand-in for Snowflake/BigQuery) and build SQL marts."""
from __future__ import annotations

import logging
from pathlib import Path

from .config import SQL_MARTS_DIR

log = logging.getLogger(__name__)

REPORT_QUERIES = {
    "Daily sales (last 7 days)": "SELECT * FROM marts.daily_sales ORDER BY order_date DESC LIMIT 7",
    "Category ranking, latest month": (
        "SELECT * FROM marts.category_performance "
        "WHERE month = (SELECT MAX(month) FROM marts.category_performance) ORDER BY sales_rank"
    ),
    "Top 10 customers by net sales": "SELECT * FROM marts.customer_value ORDER BY net_sales DESC LIMIT 10",
    "Data quality: rows quarantined by rule": "SELECT * FROM marts.dq_summary",
}


def run_sql_text(con, text: str) -> None:
    """Run a script with several statements (split on ';', so avoid ';' in comments/strings)."""
    for statement in text.split(";"):
        if statement.strip():
            con.execute(statement)


def build_marts(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS marts")
    for sql_file in sorted(SQL_MARTS_DIR.glob("*.sql")):
        run_sql_text(con, sql_file.read_text(encoding="utf-8"))
        log.info("mart built from %s", sql_file.name)


def _build_dq_summary(con) -> None:
    tables = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'quarantine' ORDER BY 1"
        ).fetchall()
    ]
    if not tables:
        con.execute("CREATE OR REPLACE TABLE marts.dq_summary (entity VARCHAR, rule VARCHAR, rows_quarantined BIGINT)")
        return
    union = " UNION ALL ".join(
        f"SELECT '{t}' AS entity, unnest(_dq_failed) AS rule FROM quarantine.{t}" for t in tables
    )
    con.execute(
        "CREATE OR REPLACE TABLE marts.dq_summary AS "
        f"SELECT entity, rule, COUNT(*) AS rows_quarantined FROM ({union}) "
        "GROUP BY entity, rule ORDER BY entity, rows_quarantined DESC"
    )


def load_warehouse(cfg: dict) -> None:
    import duckdb

    db_path = Path(cfg["paths"]["warehouse"])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    export = Path(cfg["paths"]["export"])

    con = duckdb.connect(str(db_path))
    try:
        for schema in ("core", "quarantine", "marts"):
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for schema in ("core", "quarantine"):
            for table_dir in sorted((export / schema).glob("*")):
                if table_dir.is_dir():
                    con.execute(
                        f"CREATE OR REPLACE TABLE {schema}.{table_dir.name} AS "
                        f"SELECT * FROM read_parquet('{table_dir.as_posix()}/*.parquet')"
                    )
                    log.info("loaded %s.%s", schema, table_dir.name)
        build_marts(con)
        _build_dq_summary(con)
    finally:
        con.close()


def report(cfg: dict) -> None:
    import duckdb

    con = duckdb.connect(cfg["paths"]["warehouse"], read_only=True)
    try:
        for title, query in REPORT_QUERIES.items():
            print(f"\n=== {title} ===")
            con.sql(query).show(max_width=200)
    finally:
        con.close()
