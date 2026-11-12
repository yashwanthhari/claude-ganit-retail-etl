from datetime import date, datetime

TS = datetime(2026, 1, 1, 12, 0, 0)


def test_apply_rules_splits_and_names_failures(spark):
    from pyspark.sql import functions as F

    from lakehouse.dq import Rule, apply_rules

    df = spark.createDataFrame([(1, "a"), (2, None), (-3, "c")], ["id", "label"])
    rules = [Rule("id_positive", F.col("id") > 0), Rule("label_present", F.col("label").isNotNull())]
    valid, invalid = apply_rules(df, rules)

    assert [r.id for r in valid.collect()] == [1]
    failed = {r.id: r._dq_failed for r in invalid.collect()}
    assert failed == {2: ["label_present"], -3: ["id_positive"]}


def test_clean_customers_normalises_and_flags(spark):
    from lakehouse.dq import apply_rules
    from lakehouse.silver import clean_customers, customer_rules

    cols = ["customer_id", "name", "email", "city", "segment", "signup_date", "_ingest_ts", "_source_file", "_batch_id"]
    raw = spark.createDataFrame(
        [
            ("C1", "  aarav sharma ", "AARAV@EXAMPLE.COM", "Chennai", "Premium", "05/03/2025", TS, "f", "b1"),
            ("C2", "Meera Iyer", "not-an-email", "Pune", "New", "2025-01-10", TS, "f", "b1"),
            ("C3", "Rohan Das", "", "Delhi", "Regular", "garbage", TS, "f", "b1"),
        ],
        cols,
    )
    valid, invalid = apply_rules(clean_customers(raw), customer_rules())

    good = valid.collect()
    assert len(good) == 1
    assert good[0].name == "Aarav Sharma"
    assert good[0].email == "aarav@example.com"
    assert good[0].signup_date == date(2025, 3, 5)

    reasons = {r.customer_id: set(r._dq_failed) for r in invalid.collect()}
    assert reasons == {"C2": {"email_format"}, "C3": {"signup_date_valid"}}


def test_upsert_updates_and_respects_stale_guard(spark, tmp_path):
    from lakehouse.delta_utils import upsert

    path = str(tmp_path / "orders")
    cols = ["order_id", "status", "updated_at"]
    upsert(spark, spark.createDataFrame([("O1", "placed", 1), ("O2", "placed", 1)], cols), path, ["order_id"])
    upsert(
        spark,
        spark.createDataFrame([("O1", "shipped", 2), ("O2", "stale", 0), ("O3", "placed", 1)], cols),
        path,
        ["order_id"],
        "s.updated_at >= t.updated_at",
    )
    result = {r.order_id: r.status for r in spark.read.format("delta").load(path).collect()}
    assert result == {"O1": "shipped", "O2": "placed", "O3": "placed"}
