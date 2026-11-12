"""Command-line entry point.

    python -m lakehouse.pipeline                 # run everything
    python -m lakehouse.pipeline bronze silver   # run selected stages
    python -m lakehouse.pipeline --days 3        # smaller dataset

Stages: generate -> bronze -> silver -> gold -> warehouse -> report
"""
from __future__ import annotations

import argparse
import logging
import time

from .config import load_config

STAGES = ["generate", "bronze", "silver", "gold", "warehouse", "report"]
SPARK_STAGES = {"bronze", "silver", "gold"}
log = logging.getLogger("lakehouse")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("stages", nargs="*", help=f"any of: {', '.join(STAGES)} (default: all)")
    parser.add_argument("--config", help="path to a pipeline YAML (default: config/pipeline.yaml)")
    parser.add_argument("--days", type=int, help="override generator.days")
    parser.add_argument("--orders-per-day", type=int, help="override generator.orders_per_day")
    args = parser.parse_args(argv)

    stages = STAGES if not args.stages or args.stages == ["all"] else args.stages
    unknown = [s for s in stages if s not in STAGES]
    if unknown:
        parser.error(f"unknown stage(s): {unknown}")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = load_config(args.config)
    if args.days:
        cfg["generator"]["days"] = args.days
    if args.orders_per_day:
        cfg["generator"]["orders_per_day"] = args.orders_per_day

    spark = None
    if SPARK_STAGES & set(stages) or "gold" in stages:
        from .spark_session import get_spark

        spark = get_spark(cfg)

    try:
        for stage in STAGES:  # always run in dependency order
            if stage not in stages:
                continue
            started = time.perf_counter()
            log.info("=== stage: %s ===", stage)
            if stage == "generate":
                from .generate_data import generate

                log.info("generated: %s", generate(cfg["generator"], cfg["paths"]["landing"]))
            elif stage == "bronze":
                from .bronze import run_bronze

                run_bronze(spark, cfg)
            elif stage == "silver":
                from .silver import run_silver

                run_silver(spark, cfg)
            elif stage == "gold":
                from .gold import export_to_parquet, run_gold

                run_gold(spark, cfg)
                export_to_parquet(spark, cfg)
            elif stage == "warehouse":
                from .warehouse import load_warehouse

                load_warehouse(cfg)
            elif stage == "report":
                from .warehouse import report

                report(cfg)
            log.info("stage %s finished in %.1fs", stage, time.perf_counter() - started)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
