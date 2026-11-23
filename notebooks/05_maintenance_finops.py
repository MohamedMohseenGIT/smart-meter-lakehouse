# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Table maintenance and cost hygiene (weekly)
# MAGIC
# MAGIC * `OPTIMIZE` compacts small files produced by incremental MERGEs (liquid clustering keys are
# MAGIC   declared on the tables, so no manual Z-ORDER tuning is needed).
# MAGIC * `VACUUM` removes files older than the 7-day time-travel window → lower storage cost.
# MAGIC * `ANALYZE` refreshes statistics for better join planning (fewer shuffles, shorter runtimes).
# MAGIC
# MAGIC Pipeline-managed tables (bronze/silver) are maintained automatically by Lakeflow, so only
# MAGIC job-managed gold tables are handled here. Before/after sizes are recorded to show the effect.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from smartmeter.config import LakehouseNames

n = LakehouseNames.from_widgets(dbutils)
TABLES = [n.gold("fact_daily_consumption"), n.gold("dim_customer"), n.gold("meter_anomalies"),
          n.gold("churn_scores"), n.silver("customer_pii")]


def detail(table):
    d = spark.sql(f"DESCRIBE DETAIL {table}").first()
    return {"table": table, "files": d["numFiles"], "mb": round((d["sizeInBytes"] or 0) / 1e6, 2)}

# COMMAND ----------

report = []
for t in TABLES:
    before = detail(t)
    spark.sql(f"OPTIMIZE {t}")
    spark.sql(f"VACUUM {t} RETAIN 168 HOURS")
    spark.sql(f"ANALYZE TABLE {t} COMPUTE STATISTICS FOR ALL COLUMNS")
    after = detail(t)
    report.append((t, before["files"], after["files"], before["mb"], after["mb"]))

display(spark.createDataFrame(report, "table string, files_before long, files_after long, mb_before double, "
                                      "mb_after double"))
