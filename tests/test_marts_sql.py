"""Exercise the SQL marts against a tiny hand-built star schema (DuckDB only, no Spark)."""
from datetime import date

import pytest

duckdb = pytest.importorskip("duckdb")

from lakehouse.warehouse import build_marts  # noqa: E402


@pytest.fixture()
def con():
    c = duckdb.connect(":memory:")
    c.execute("CREATE SCHEMA core")
    c.execute(
        """CREATE TABLE core.fact_sales (
            order_id VARCHAR, line_no INTEGER, order_date DATE, date_key INTEGER,
            customer_key BIGINT, product_key BIGINT, store_key BIGINT, status VARCHAR,
            is_cancelled BOOLEAN, qty INTEGER, unit_price DECIMAL(12,2), discount_pct DECIMAL(5,2),
            gross_amount DECIMAL(14,2), discount_amount DECIMAL(14,2), net_amount DECIMAL(14,2))"""
    )
    c.execute("CREATE TABLE core.dim_product (product_key BIGINT, category VARCHAR)")
    c.execute(
        "CREATE TABLE core.dim_customer (customer_key BIGINT, customer_id VARCHAR, name VARCHAR, city VARCHAR, segment VARCHAR)"
    )
    c.execute("INSERT INTO core.dim_product VALUES (1, 'Snacks'), (2, 'Dairy')")
    c.execute("INSERT INTO core.dim_customer VALUES (10, 'C1', 'A', 'Chennai', 'New'), (11, 'C2', 'B', 'Pune', 'New')")
    rows = [
        ("O1", 1, date(2026, 1, 1), 20260101, 10, 1, 5, "delivered", False, 2, 100, 10, 200, 20, 180),
        ("O1", 2, date(2026, 1, 1), 20260101, 10, 2, 5, "delivered", False, 1, 50, 0, 50, 0, 50),
        ("O2", 1, date(2026, 1, 2), 20260102, 11, 1, 5, "placed", False, 1, 100, 0, 100, 0, 100),
        ("O3", 1, date(2026, 1, 2), 20260102, 11, 1, 5, "cancelled", True, 9, 100, 0, 900, 0, 900),
    ]
    c.executemany("INSERT INTO core.fact_sales VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    build_marts(c)
    yield c
    c.close()


def test_daily_sales_excludes_cancelled(con):
    rows = con.execute("SELECT order_date, orders, net_sales FROM marts.daily_sales ORDER BY 1").fetchall()
    assert [(r[0], r[1], float(r[2])) for r in rows] == [(date(2026, 1, 1), 1, 230.0), (date(2026, 1, 2), 1, 100.0)]


def test_category_ranking(con):
    top = con.execute("SELECT category FROM marts.category_performance WHERE sales_rank = 1").fetchone()
    assert top[0] == "Snacks"


def test_customer_value_recency(con):
    rows = dict(con.execute("SELECT customer_id, recency_days FROM marts.customer_value").fetchall())
    assert rows == {"C1": 1, "C2": 0}
