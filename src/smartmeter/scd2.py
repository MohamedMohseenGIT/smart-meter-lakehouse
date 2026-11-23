"""Slowly Changing Dimension type 2 for daily snapshots, in a single set-based MERGE.

Instead of merging snapshots one day at a time (N MERGEs for an N-day backfill), all new snapshots
are compared with the current dimension rows using window functions. Only *change points* survive,
validity ranges are derived with `lead()`, and the result is applied with one MERGE:

* `close`  -> existing current row gets valid_to / is_current = false
* `insert` -> new version (intermediate versions are inserted already closed)

Idempotency: the caller only passes snapshots newer than the dimension's max(valid_from), so a
re-run after a partial failure never rewrites history.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def row_hash(cols: list[str]):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)


def compute_scd2_changes(
    current: DataFrame,
    snapshots: DataFrame,
    key: str,
    tracked_cols: list[str],
    date_col: str = "snapshot_date",
) -> DataFrame:
    """Return the rows to MERGE, with an `_action` column ('close' or 'insert').

    current   : current dimension rows (is_current = true) with key, tracked cols, valid_from, row_hash
    snapshots : new source snapshots with key, tracked cols and `date_col`
    """
    snap = (snapshots
            .select(key, *tracked_cols, F.col(date_col).cast("date").alias("valid_from"))
            .dropDuplicates([key, "valid_from"])
            .withColumn("row_hash", row_hash(tracked_cols))
            .withColumn("_existing", F.lit(False)))
    cur = (current
           .select(key, *tracked_cols, F.col("valid_from").cast("date").alias("valid_from"), "row_hash")
           .withColumn("_existing", F.lit(True)))

    w = Window.partitionBy(key).orderBy(F.col("valid_from"), F.col("_existing").desc())
    points = (cur.unionByName(snap)
              .withColumn("_prev_hash", F.lag("row_hash").over(w))
              .where(F.col("_existing") | F.col("_prev_hash").isNull() | (F.col("row_hash") != F.col("_prev_hash"))))

    w2 = Window.partitionBy(key).orderBy("valid_from")
    versions = (points
                .withColumn("valid_to", F.lead("valid_from").over(w2))
                .withColumn("is_current", F.col("valid_to").isNull()))

    action = (F.when(F.col("_existing") & F.col("valid_to").isNotNull(), "close")
              .when(~F.col("_existing"), "insert"))
    return (versions
            .withColumn("_action", action)
            .where(F.col("_action").isNotNull())
            .drop("_existing", "_prev_hash"))


def merge_scd2(spark, target_table: str, changes: DataFrame, key: str, insert_cols: list[str]) -> None:
    """Apply `compute_scd2_changes` output to a Delta table with one MERGE statement."""
    changes.createOrReplaceTempView("_scd2_changes")
    cols = ", ".join(insert_cols)
    vals = ", ".join(f"s.{c}" for c in insert_cols)
    spark.sql(f"""
        MERGE INTO {target_table} AS t
        USING _scd2_changes AS s
          ON t.{key} = s.{key} AND t.valid_from = s.valid_from
        WHEN MATCHED AND s._action = 'close' THEN
          UPDATE SET t.valid_to = s.valid_to, t.is_current = false
        WHEN NOT MATCHED AND s._action = 'insert' THEN
          INSERT ({cols}) VALUES ({vals})
    """)
