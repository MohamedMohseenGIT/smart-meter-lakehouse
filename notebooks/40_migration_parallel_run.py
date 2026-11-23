# Databricks notebook source
# MAGIC %md
# MAGIC # 40 · Migration: AWS Glue → Databricks parallel run
# MAGIC
# MAGIC Before cutting over, old and new pipelines run side by side on the same input and are reconciled
# MAGIC per meter and day. The goal is not "zero differences" but **every difference explained**.
# MAGIC
# MAGIC The legacy job (`legacy/glue_daily_consumption_job.py`) is ported as-is in `smartmeter.legacy`.
# MAGIC Its known defects are then fixed one at a time; the mismatch count should fall to zero, proving
# MAGIC that the remaining differences are exactly those defects and nothing else.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from pyspark.sql import functions as F

from smartmeter.config import LakehouseNames
from smartmeter.legacy import legacy_daily_consumption
from smartmeter.quality import READING_RULES, all_pass
from smartmeter.reconcile import reconcile, summary

n = LakehouseNames.from_widgets(dbutils)
dbutils.widgets.text("days", "14")
DAYS = int(dbutils.widgets.get("days"))

new = (spark.table(n.gold("fact_daily_consumption"))
       .where(F.col("reading_date").between(F.date_sub(F.current_date(), DAYS), F.date_sub(F.current_date(), 3)))
       .select("meter_id", "reading_date", "kwh_import"))
dates = new.select("reading_date").distinct()
bronze = spark.table(n.bronze("meter_readings"))


def compare(label: str, legacy_df):
    legacy_df = legacy_df.join(dates, "reading_date", "left_semi").select("meter_id", "reading_date", "kwh_import")
    res = reconcile(legacy_df, new, ["meter_id", "reading_date"], ["kwh_import"], abs_tolerance=0.001,
                    compare_counts=False)
    s = summary(res)
    print(f"{label:<55} {s}")
    return label, res, s

# COMMAND ----------

ts = F.to_timestamp("reading_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'")
typed = bronze.withColumn("reading_ts", ts)
local_date = F.to_date(F.from_utc_timestamp("reading_ts", "Europe/Brussels"))

steps = [
    compare("0. legacy logic as-is", legacy_daily_consumption(bronze)),
    compare("1. + Belgian local date instead of UTC date",
            typed.where("kwh_import >= 0").groupBy("meter_id", local_date.alias("reading_date"))
            .agg(F.sum("kwh_import").alias("kwh_import"))),
    compare("2. + de-duplicate re-delivered rows",
            typed.where("kwh_import >= 0").dropDuplicates(["meter_id", "reading_ts"])
            .groupBy("meter_id", local_date.alias("reading_date")).agg(F.sum("kwh_import").alias("kwh_import"))),
    compare("3. + shared data-quality rules (= new logic)",
            typed.where(all_pass(READING_RULES)).dropDuplicates(["meter_id", "reading_ts"])
            .groupBy("meter_id", local_date.alias("reading_date")).agg(F.sum("kwh_import").alias("kwh_import"))),
]

# COMMAND ----------

report = spark.createDataFrame(
    [(label, s["MATCH"], s["MISMATCH"], s["MISSING_IN_TARGET"], s["MISSING_IN_SOURCE"]) for label, _, s in steps],
    "step string, match long, mismatch long, missing_in_new long, missing_in_legacy long")
report.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(n.ops("migration_reconciliation"))
display(report)

# Drill into the raw legacy differences, for the sign-off document.
display(steps[0][1].where("status <> 'MATCH'").orderBy(F.abs("diff_kwh_import").desc()).limit(20))
