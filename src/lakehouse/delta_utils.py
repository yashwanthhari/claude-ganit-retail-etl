"""Delta Lake helpers."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def upsert(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: list[str],
    update_condition: str | None = None,
) -> None:
    """MERGE `df` into the Delta table at `path` (create it on first run).

    `update_condition` lets you refuse stale updates, e.g. "s.updated_at >= t.updated_at".
    The source must be unique on `keys`.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("overwrite").save(path)
        return

    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    merge = DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), on)
    merge = (
        merge.whenMatchedUpdateAll(condition=update_condition)
        if update_condition
        else merge.whenMatchedUpdateAll()
    )
    merge.whenNotMatchedInsertAll().execute()


def overwrite(df: DataFrame, path: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


def append(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
