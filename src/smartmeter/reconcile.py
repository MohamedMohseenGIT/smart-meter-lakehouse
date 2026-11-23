"""Reconciliation between two datasets (layer-to-layer, source-to-target, legacy-vs-new).

Used three ways in this project:
* upstream control totals  -> bronze row counts (did we load every row the source delivered?)
* silver hourly readings   -> gold daily facts (did aggregation lose or double-count energy?)
* legacy Glue logic        -> new Databricks logic (parallel run during the migration)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def reconcile(
    source: DataFrame,
    target: DataFrame,
    keys: list[str],
    measures: list[str],
    abs_tolerance: float = 0.0,
    rel_tolerance: float = 0.0,
    compare_counts: bool = True,
) -> DataFrame:
    """Compare row counts and sums of `measures` per `keys`.

    Returns one row per key with source/target values, differences and a `status` of
    MATCH, MISMATCH, MISSING_IN_TARGET or MISSING_IN_SOURCE. Set `compare_counts=False` when the
    two sides have a different grain by design (e.g. hourly vs daily) and only measures matter.
    """
    def agg(df: DataFrame, side: str) -> DataFrame:
        exprs = [F.count(F.lit(1)).alias(f"{side}_rows")]
        exprs += [F.sum(m).alias(f"{side}_{m}") for m in measures]
        return df.groupBy(*keys).agg(*exprs).withColumn(f"_in_{side}", F.lit(True))

    joined = agg(source, "src").join(agg(target, "tgt"), on=keys, how="full_outer")

    within = F.lit(True)
    for m in (["rows"] if compare_counts else []) + list(measures):
        s, t = F.coalesce(F.col(f"src_{m}"), F.lit(0)), F.coalesce(F.col(f"tgt_{m}"), F.lit(0))
        joined = joined.withColumn(f"diff_{m}", t - s)
        allowed = F.greatest(F.lit(abs_tolerance), F.abs(s) * F.lit(rel_tolerance))
        within = within & (F.abs(t - s) <= allowed)

    status = (F.when(F.col("_in_tgt").isNull(), "MISSING_IN_TARGET")
              .when(F.col("_in_src").isNull(), "MISSING_IN_SOURCE")
              .when(within, "MATCH")
              .otherwise("MISMATCH"))
    return joined.withColumn("status", status).drop("_in_src", "_in_tgt")


def summary(result: DataFrame) -> dict:
    counts = {r["status"]: r["count"] for r in result.groupBy("status").count().collect()}
    return {s: int(counts.get(s, 0)) for s in ("MATCH", "MISMATCH", "MISSING_IN_TARGET", "MISSING_IN_SOURCE")}
