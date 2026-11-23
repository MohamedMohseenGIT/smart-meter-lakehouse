"""Faithful port of the legacy AWS Glue job (see legacy/glue_daily_consumption_job.py).

During a migration the old logic is re-implemented *as is* - including its known flaws - so a
parallel run can reconcile old vs new output and explain every difference before cut-over.

Known legacy behaviours reproduced here:
1. aggregates by UTC date instead of Belgian local date,
2. does not de-duplicate re-delivered rows,
3. only filters negative values (NULLs silently disappear in SUM, unknown meters are kept).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def legacy_daily_consumption(bronze_readings: DataFrame) -> DataFrame:
    df = (bronze_readings
          .withColumn("reading_ts", F.to_timestamp("reading_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
          .where(F.col("kwh_import") >= 0))
    return (df.groupBy("meter_id", F.to_date("reading_ts").alias("reading_date"))
            .agg(F.sum("kwh_import").alias("kwh_import"), F.sum("kwh_export").alias("kwh_export")))
