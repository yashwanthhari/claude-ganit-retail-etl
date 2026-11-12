"""Silver layer: typed, cleaned, de-duplicated, validated.

Per entity: read only new bronze batches -> clean/cast -> apply DQ rules ->
quarantine failures -> de-duplicate -> MERGE (upsert) into Delta.
Orders are split into a header table and an exploded order_items table.
"""
from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .delta_utils import append, upsert
from .dq import Rule, apply_rules
from .state import StateStore

log = logging.getLogger(__name__)

ORDER_STATUSES = ["placed", "shipped", "delivered", "cancelled"]
LINEAGE = ["_ingest_ts", "_source_file", "_batch_id"]


# ---------------------------------------------------------------- helpers
def _blank_to_null(name: str) -> Column:
    c = F.trim(F.col(name))
    return F.when(c == "", F.lit(None)).otherwise(c)


def _parse_date(c: Column) -> Column:
    return F.coalesce(F.to_date(c, "yyyy-MM-dd"), F.to_date(c, "dd/MM/yyyy"))


def latest_per_key(df: DataFrame, keys: list[str], order_cols: list[str]) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


# ---------------------------------------------------------------- cleaning
def clean_customers(df: DataFrame) -> DataFrame:
    return df.select(
        _blank_to_null("customer_id").alias("customer_id"),
        F.initcap(_blank_to_null("name")).alias("name"),
        F.lower(_blank_to_null("email")).alias("email"),
        _blank_to_null("city").alias("city"),
        _blank_to_null("segment").alias("segment"),
        _parse_date(F.trim(F.col("signup_date"))).alias("signup_date"),
        *LINEAGE,
    )


def customer_rules() -> list[Rule]:
    return [
        Rule("customer_id_present", F.col("customer_id").isNotNull()),
        Rule("signup_date_valid", F.col("signup_date").isNotNull()),
        # A missing email is allowed; a malformed one is not.
        Rule("email_format", F.col("email").isNull() | F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")),
    ]


def clean_products(df: DataFrame) -> DataFrame:
    return df.select(
        _blank_to_null("product_id").alias("product_id"),
        F.upper(_blank_to_null("sku")).alias("sku"),
        F.initcap(_blank_to_null("category")).alias("category"),
        _blank_to_null("brand").alias("brand"),
        F.trim(F.col("mrp")).cast("decimal(12,2)").alias("mrp"),
        *LINEAGE,
    )


def product_rules() -> list[Rule]:
    return [
        Rule("product_id_present", F.col("product_id").isNotNull()),
        Rule("mrp_positive", F.col("mrp") > 0),
    ]


def clean_stores(df: DataFrame) -> DataFrame:
    return df.select(
        _blank_to_null("store_id").alias("store_id"),
        _blank_to_null("city").alias("city"),
        _blank_to_null("region").alias("region"),
        _blank_to_null("format").alias("format"),
        *LINEAGE,
    )


def store_rules() -> list[Rule]:
    return [Rule("store_id_present", F.col("store_id").isNotNull())]


def clean_orders(df: DataFrame) -> DataFrame:
    ts = F.to_timestamp(F.trim(F.col("order_ts")))
    return df.select(
        _blank_to_null("order_id").alias("order_id"),
        _blank_to_null("customer_id").alias("customer_id"),
        _blank_to_null("store_id").alias("store_id"),
        ts.alias("order_ts"),
        F.to_date(ts).alias("order_date"),
        F.to_timestamp(F.trim(F.col("updated_at"))).alias("updated_at"),
        F.lower(_blank_to_null("status")).alias("status"),
        "items",
        *LINEAGE,
    )


def order_rules() -> list[Rule]:
    return [
        Rule("order_id_present", F.col("order_id").isNotNull()),
        Rule("customer_id_present", F.col("customer_id").isNotNull()),
        Rule("store_id_present", F.col("store_id").isNotNull()),
        Rule("order_ts_valid", F.col("order_ts").isNotNull()),
        Rule("updated_at_valid", F.col("updated_at").isNotNull()),
        Rule("status_valid", F.col("status").isin(ORDER_STATUSES)),
    ]


def explode_items(orders: DataFrame) -> DataFrame:
    exploded = orders.select(
        "order_id", "order_date", "updated_at", *LINEAGE, F.posexplode("items").alias("pos", "item")
    )
    return exploded.select(
        "order_id",
        (F.col("pos") + 1).alias("line_no"),
        "order_date",
        "updated_at",
        F.trim(F.col("item.product_id")).alias("product_id"),
        F.trim(F.col("item.qty")).cast("int").alias("qty"),
        F.trim(F.col("item.unit_price")).cast("decimal(12,2)").alias("unit_price"),
        F.trim(F.col("item.discount_pct")).cast("decimal(5,2)").alias("discount_pct"),
        *LINEAGE,
    )


def item_rules() -> list[Rule]:
    return [
        Rule("product_id_present", F.col("product_id").isNotNull()),
        Rule("product_exists", F.col("_product_found")),
        Rule("qty_positive", F.col("qty") > 0),
        Rule("unit_price_non_negative", F.col("unit_price") >= 0),
        Rule("discount_in_range", F.col("discount_pct").between(0, 100)),
    ]


# ---------------------------------------------------------------- orchestration
def _read_new(spark: SparkSession, bronze: Path, entity: str, state: StateStore) -> DataFrame | None:
    path = str(bronze / entity)
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        log.info("silver.%s: bronze table not found, skipping", entity)
        return None
    df = spark.read.format("delta").load(path)
    watermark = state.get(f"silver_watermark.{entity}")
    return df.filter(F.col("_batch_id") > watermark) if watermark else df


def _quarantine(bad: DataFrame, silver: Path, name: str) -> None:
    append(bad.withColumn("_quarantined_at", F.current_timestamp()), str(silver / "_quarantine" / name))


def _process_simple(spark, cfg, state, entity, clean, rules, keys) -> None:
    bronze, silver = Path(cfg["paths"]["bronze"]), Path(cfg["paths"]["silver"])
    new = _read_new(spark, bronze, entity, state)
    if new is None:
        return
    max_batch = new.agg(F.max("_batch_id")).first()[0]
    if max_batch is None:
        log.info("silver.%s: nothing new", entity)
        return

    valid, bad = apply_rules(clean(new), rules())
    valid = latest_per_key(valid, keys, ["_batch_id"])
    upsert(spark, valid, str(silver / entity), keys)
    _quarantine(bad, silver, entity)
    state.set(f"silver_watermark.{entity}", max_batch)
    log.info("silver.%s: processed batches up to %s", entity, max_batch)


def _process_orders(spark, cfg, state) -> None:
    bronze, silver = Path(cfg["paths"]["bronze"]), Path(cfg["paths"]["silver"])
    new = _read_new(spark, bronze, "orders", state)
    if new is None:
        return
    max_batch = new.agg(F.max("_batch_id")).first()[0]
    if max_batch is None:
        log.info("silver.orders: nothing new")
        return

    valid, bad_orders = apply_rules(clean_orders(new), order_rules())
    # Keep the most recent version of each order (late status updates win).
    valid = latest_per_key(valid, ["order_id"], ["updated_at", "_batch_id"]).cache()

    items = explode_items(valid)
    known_products = (
        spark.read.format("delta")
        .load(str(silver / "products"))
        .select("product_id")
        .distinct()
        .withColumn("_product_found", F.lit(True))
    )
    items = items.join(F.broadcast(known_products), "product_id", "left").withColumn(
        "_product_found", F.coalesce(F.col("_product_found"), F.lit(False))
    )
    good_items, bad_items = apply_rules(items, item_rules())
    good_items = good_items.drop("_product_found")

    stale_guard = "s.updated_at >= t.updated_at"
    upsert(spark, valid.drop("items"), str(silver / "orders"), ["order_id"], stale_guard)
    upsert(spark, good_items, str(silver / "order_items"), ["order_id", "line_no"], stale_guard)
    _quarantine(bad_orders, silver, "orders")
    _quarantine(bad_items.drop("_product_found"), silver, "order_items")
    valid.unpersist()

    state.set("silver_watermark.orders", max_batch)
    log.info("silver.orders: processed batches up to %s", max_batch)


def run_silver(spark: SparkSession, cfg: dict) -> None:
    state = StateStore(Path(cfg["paths"]["state"]) / "state.json")
    # Products must be processed before orders (items are checked against them).
    _process_simple(spark, cfg, state, "customers", clean_customers, customer_rules, ["customer_id"])
    _process_simple(spark, cfg, state, "products", clean_products, product_rules, ["product_id"])
    _process_simple(spark, cfg, state, "stores", clean_stores, store_rules, ["store_id"])
    _process_orders(spark, cfg, state)
