"""Local SparkSession with Delta Lake enabled (the free stand-in for a Databricks cluster)."""
from __future__ import annotations


def get_spark(cfg: dict, app_name: str | None = None, shuffle_partitions: int | None = None):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    spark_cfg = cfg.get("spark", {})
    builder = (
        SparkSession.builder.appName(app_name or spark_cfg.get("app_name", "lakehouse"))
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", spark_cfg.get("driver_memory", "2g"))
        .config(
            "spark.sql.shuffle.partitions",
            str(shuffle_partitions or spark_cfg.get("shuffle_partitions", 4)),
        )
        # Make unparseable dates return NULL instead of raising, so DQ rules can catch them.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    # Downloads the Delta jars from Maven the first time it runs (needs internet once).
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
