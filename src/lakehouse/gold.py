"""Gold layer: a star schema ready for BI, plus parquet exports for the warehouse load."""
from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .delta_utils import overwrite

log = logging.getLogger(__name__)

UNKNOWN_KEY = -1
QUARANTINE_TABLES = ["customers", "products", "stores", "orders", "order_items"]


def _read(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.format("delta").load(str(path))


def build_dim_customer(customers: DataFrame, spark: SparkSession) -> DataFrame:
    dim = customers.select(
        F.xxhash64("customer_id").alias("customer_key"),
        "customer_id", "name", "email", "city", "segment", "signup_date",
    )
    # "Unknown member" row so facts with an unmatched customer still join.
    unknown = spark.range(1).select(
        F.lit(UNKNOWN_KEY).cast("long").alias("customer_key"),
        F.lit("UNKNOWN").alias("customer_id"),
        F.lit("Unknown").alias("name"),
        F.lit(None).cast("string").alias("email"),
        F.lit(None).cast("string").alias("city"),
        F.lit(None).cast("string").alias("segment"),
        F.lit(None).cast("date").alias("signup_date"),
    )
    return dim.unionByName(unknown)


def build_fact_sales(items: DataFrame, orders: DataFrame, dim_customer: DataFrame) -> DataFrame:
    o = orders.select("order_id", "customer_id", "store_id", "order_date", "status")
    i = items.select("order_id", "line_no", "product_id", "qty", "unit_price", "discount_pct")
    customer_map = dim_customer.filter(F.col("customer_key") != UNKNOWN_KEY).select("customer_id", "customer_key")

    gross = (F.col("qty") * F.col("unit_price")).cast("decimal(14,2)")
    discount = F.round(F.col("gross_amount") * F.col("discount_pct") / 100, 2).cast("decimal(14,2)")
    return (
        i.join(o, "order_id")
        .join(F.broadcast(customer_map), "customer_id", "left")
        .withColumn("gross_amount", gross)
        .withColumn("discount_amount", discount)
        .withColumn("net_amount", (F.col("gross_amount") - F.col("discount_amount")).cast("decimal(14,2)"))
        .select(
            "order_id",
            "line_no",
            "order_date",
            F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.coalesce("customer_key", F.lit(UNKNOWN_KEY).cast("long")).alias("customer_key"),
            F.xxhash64("product_id").alias("product_key"),
            F.xxhash64("store_id").alias("store_key"),
            "status",
            (F.col("status") == "cancelled").alias("is_cancelled"),
            "qty",
            "unit_price",
            "discount_pct",
            "gross_amount",
            "discount_amount",
            "net_amount",
        )
    )


def build_dim_date(spark: SparkSession, orders: DataFrame) -> DataFrame | None:
    lo, hi = orders.agg(F.min("order_date"), F.max("order_date")).first()
    if lo is None:
        return None
    return (
        spark.sql(f"SELECT explode(sequence(to_date('{lo}'), to_date('{hi}'), interval 1 day)) AS date")
        .select(
            F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
            "date",
            F.year("date").alias("year"),
            F.month("date").alias("month"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.dayofmonth("date").alias("day"),
            F.date_format("date", "EEEE").alias("day_name"),
            F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        )
    )


def run_gold(spark: SparkSession, cfg: dict) -> None:
    silver, gold = Path(cfg["paths"]["silver"]), Path(cfg["paths"]["gold"])

    customers = _read(spark, silver / "customers")
    products = _read(spark, silver / "products")
    stores = _read(spark, silver / "stores")
    orders = _read(spark, silver / "orders")
    items = _read(spark, silver / "order_items")

    dim_customer = build_dim_customer(customers, spark)
    dim_product = products.select(
        F.xxhash64("product_id").alias("product_key"), "product_id", "sku", "category", "brand", "mrp"
    )
    dim_store = stores.select(F.xxhash64("store_id").alias("store_key"), "store_id", "city", "region", "format")
    dim_date = build_dim_date(spark, orders)
    fact_sales = build_fact_sales(items, orders, dim_customer)

    tables = {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_store": dim_store,
        "fact_sales": fact_sales,
    }
    if dim_date is not None:
        tables["dim_date"] = dim_date
    for name, df in tables.items():
        overwrite(df, str(gold / name))
        log.info("gold.%s written", name)


def export_to_parquet(spark: SparkSession, cfg: dict) -> None:
    """Snapshot gold + quarantine tables as parquet for the warehouse loader."""
    from delta.tables import DeltaTable

    gold, silver, export = (Path(cfg["paths"][k]) for k in ("gold", "silver", "export"))
    jobs = {
        ("core", p.name): p for p in sorted(gold.glob("*")) if p.is_dir()
    }
    for name in QUARANTINE_TABLES:
        jobs[("quarantine", name)] = silver / "_quarantine" / name

    for (schema, name), path in jobs.items():
        if not DeltaTable.isDeltaTable(spark, str(path)):
            continue
        target = export / schema / name
        spark.read.format("delta").load(str(path)).write.mode("overwrite").parquet(str(target))
        log.info("exported %s.%s", schema, name)
