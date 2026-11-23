# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Freshness SLOs
# MAGIC
# MAGIC Each critical table has an explicit freshness SLO. The check records the observed lag and fails
# MAGIC the task on a breach, which triggers the job's failure notification. Results land in
# MAGIC `ops.freshness_checks`, which feeds the operations dashboard (SLO attainment over time).

# COMMAND ----------

import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from pyspark.sql import functions as F

from smartmeter.config import LakehouseNames
from smartmeter.monitoring import BREACH, FreshnessSLO, evaluate_freshness

n = LakehouseNames.from_widgets(dbutils)
dbutils.widgets.dropdown("fail_on_breach", "true", ["true", "false"])

# Data for day D is delivered on D+1 morning, so 30-48h lags are normal for daily batch feeds.
SLOS = [
    FreshnessSLO(n.bronze("meter_readings"), "_ingested_at", 26, "a delivery was ingested in the last day"),
    FreshnessSLO(n.silver("meter_readings"), "reading_ts", 36, "latest validated hourly reading"),
    FreshnessSLO(n.silver("grid_market"), "ts_utc", 36, "latest market hour"),
    FreshnessSLO(n.gold("fact_daily_consumption"), "reading_date", 48, "latest complete reporting day"),
]

# COMMAND ----------

now = datetime.now(timezone.utc).replace(tzinfo=None)
run_id = str(uuid.uuid4())
results = []
for slo in SLOS:
    latest = spark.table(slo.table).agg(F.max(F.col(slo.timestamp_column).cast("timestamp"))).first()[0]
    results.append({"run_id": run_id, **evaluate_freshness(slo, latest, now)})

df = (spark.createDataFrame(results, "run_id string, table_name string, latest_ts timestamp, lag_hours double, "
                                     "max_lag_hours double, status string")
      .withColumn("checked_at", F.current_timestamp())
      .select("run_id", "checked_at", "table_name", "latest_ts", "lag_hours", "max_lag_hours", "status"))
df.write.mode("append").saveAsTable(n.ops("freshness_checks"))
display(df)

breaches = [r["table_name"] for r in results if r["status"] == BREACH]
if breaches and dbutils.widgets.get("fail_on_breach") == "true":
    raise AssertionError(f"Freshness SLO breached for: {breaches}")
