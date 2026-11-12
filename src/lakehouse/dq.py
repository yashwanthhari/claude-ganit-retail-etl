"""Declarative data-quality rules with a quarantine pattern.

Each Rule is a boolean Spark expression that is True when the row is GOOD.
Rows failing any rule are routed to a quarantine table together with the names
of the rules they broke, so nothing is silently dropped and bad data is
reportable. (Same idea as Great Expectations / dbt tests, but kept explicit.)
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    check: Column  # True => row passes. NULL is treated as a failure.


def apply_rules(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame]:
    """Return (valid_rows, quarantined_rows). Quarantined rows carry `_dq_failed`."""
    failures = [F.when(~F.coalesce(r.check, F.lit(False)), F.lit(r.name)) for r in rules]
    tagged = df.withColumn("_dq_failed", F.array_compact(F.array(*failures)))
    valid = tagged.filter(F.size("_dq_failed") == 0).drop("_dq_failed")
    invalid = tagged.filter(F.size("_dq_failed") > 0)
    return valid, invalid
