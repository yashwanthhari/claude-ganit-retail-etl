import json

from lakehouse.generate_data import generate

CFG = dict(seed=7, start_date="2026-01-01", days=3, customers=100, products=20, stores=4, orders_per_day=150)


def _read_orders(landing):
    files = sorted((landing / "orders").glob("dt=*/orders.jsonl"))
    rows = [json.loads(line) for f in files for line in f.read_text().splitlines()]
    return files, rows


def test_writes_expected_files(tmp_path):
    summary = generate(CFG, tmp_path)
    assert (tmp_path / "customers" / "customers.csv").exists()
    assert (tmp_path / "products" / "products.csv").exists()
    assert (tmp_path / "stores" / "stores.csv").exists()
    files, rows = _read_orders(tmp_path)
    assert len(files) == CFG["days"]
    assert summary["order_rows"] == len(rows)
    assert all(r["items"] for r in rows)


def test_is_deterministic(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate(CFG, a)
    generate(CFG, b)
    assert (a / "customers" / "customers.csv").read_text() == (b / "customers" / "customers.csv").read_text()
    assert _read_orders(a)[1] == _read_orders(b)[1]


def test_injects_the_problems_silver_must_handle(tmp_path):
    generate(CFG, tmp_path)
    _, rows = _read_orders(tmp_path)
    ids = [r["order_id"] for r in rows]
    assert len(ids) > len(set(ids)), "expected duplicate / re-sent orders"
    assert any(r["customer_id"] is None for r in rows)
    assert any(i["qty"] < 0 for r in rows for i in r["items"])
    assert any(i["product_id"] == "P99999" for r in rows for i in r["items"])
