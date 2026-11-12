"""Bronze layer: land raw data as-is into Delta, plus lineage columns.

Rules of bronze: no cleaning, no typing (everything stays a string), append only,
keep enough metadata to trace any row back to its file and batch.
A manifest of already-ingested files makes the step safe to re-run
(the same idea as a metadata-driven Azure Data Factory copy).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from .delta_utils import append
from .state import StateStore

log = logging.getLogger(__name__)

CSV_ENTITIES = {
    "customers": "customers/*.csv",
    "products": "products/*.csv",
    "stores": "stores/*.csv",
}
ORDERS_PATTERN = "orders/dt=*/orders.jsonl"

ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("store_id", StringType()),
        StructField("order_ts", StringType()),
        StructField("updated_at", StringType()),
        StructField("status", StringType()),
        StructField(
            "items",
            ArrayType(
                StructType(
                    [
                        StructField("product_id", StringType()),
                        StructField("qty", StringType()),
                        StructField("unit_price", StringType()),
                        StructField("discount_pct", StringType()),
                    ]
                )
            ),
        ),
    ]
)


def _with_lineage(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def _signature(landing: Path, p: Path) -> str:
    return f"{p.relative_to(landing).as_posix()}|{p.stat().st_size}"


def _new_files(landing: Path, pattern: str, entity: str, state: StateStore) -> tuple[list[Path], list[str]]:
    done = set(state.get(f"bronze_files.{entity}", []))
    files = sorted(landing.glob(pattern))
    new = [p for p in files if _signature(landing, p) not in done]
    return new, sorted(done | {_signature(landing, p) for p in new})


def run_bronze(spark: SparkSession, cfg: dict) -> str:
    landing = Path(cfg["paths"]["landing"])
    bronze = Path(cfg["paths"]["bronze"])
    state = StateStore(Path(cfg["paths"]["state"]) / "state.json")
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")

    for entity, pattern in CSV_ENTITIES.items():
        new, signatures = _new_files(landing, pattern, entity, state)
        if not new:
            log.info("bronze.%s: no new files", entity)
            continue
        df = spark.read.option("header", "true").option("inferSchema", "false").csv([str(p) for p in new])
        append(_with_lineage(df, batch_id), str(bronze / entity))
        state.set(f"bronze_files.{entity}", signatures)
        log.info("bronze.%s: ingested %d file(s)", entity, len(new))

    new, signatures = _new_files(landing, ORDERS_PATTERN, "orders", state)
    if new:
        df = spark.read.schema(ORDER_SCHEMA).json([str(p) for p in new])
        append(_with_lineage(df, batch_id), str(bronze / "orders"))
        state.set("bronze_files.orders", signatures)
        log.info("bronze.orders: ingested %d file(s)", len(new))
    else:
        log.info("bronze.orders: no new files")
    return batch_id
