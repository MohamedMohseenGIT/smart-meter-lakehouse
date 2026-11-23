# Databricks notebook source
# MAGIC %md
# MAGIC # 12 · Meter anomaly detection, validated against ground truth
# MAGIC
# MAGIC Each meter is compared with **its own** trailing 28-day history (z-score AND ratio to the mean for
# MAGIC spikes, rule for flatlines), which scales to millions of meters with plain window functions.
# MAGIC Because the generator records which meter-days were corrupted (`_truth/`), precision and recall are measured and
# MAGIC logged to MLflow on every run, so threshold changes are evidence-based.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
import mlflow
from pyspark.sql import functions as F

from smartmeter.config import LakehouseNames
from smartmeter.transforms import robust_daily_anomalies

n = LakehouseNames.from_widgets(dbutils)
dbutils.widgets.text("z_threshold", "4.0")
dbutils.widgets.text("lookback_days", "45")
dbutils.widgets.text("min_ratio", "2.5")
Z = float(dbutils.widgets.get("z_threshold"))
LOOKBACK = int(dbutils.widgets.get("lookback_days"))
RATIO = float(dbutils.widgets.get("min_ratio"))
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/smart-meter-anomalies")

# COMMAND ----------

daily = (spark.table(n.gold("fact_daily_consumption"))
         .where((F.col("customer_sk") != -1) & (F.col("reading_date") >= F.date_sub(F.current_date(), LOOKBACK + 28))))
scored = (robust_daily_anomalies(daily, z_threshold=Z, min_ratio=RATIO)
          .where(F.col("reading_date") >= F.date_sub(F.current_date(), LOOKBACK)))
anomalies = (scored.where(F.col("anomaly_type").isNotNull())
             .select("meter_id", "customer_key", "reading_date", "kwh_import",
                     F.round("hist_mean", 3).alias("hist_mean"), "z_score", "anomaly_type")
             .withColumn("detected_at", F.current_timestamp()))
(anomalies.write.mode("overwrite")
 .option("replaceWhere", f"reading_date >= date_sub(current_date(), {LOOKBACK})")
 .saveAsTable(n.gold("meter_anomalies")))
display(anomalies.groupBy("anomaly_type").count())

# COMMAND ----------

# MAGIC %md ## Validation against injected ground truth

# COMMAND ----------

truth = (spark.read.option("header", "true").csv(f"{n.raw_root}/_truth/anomalies/*.csv")
         .select("meter_id", F.to_date("reading_date").alias("reading_date"))
         .join(scored.where(F.col("hist_days") >= 14).select("meter_id", "reading_date"),
               ["meter_id", "reading_date"]))  # only days the detector could judge
detected = anomalies.select("meter_id", "reading_date")
tp = detected.join(truth, ["meter_id", "reading_date"]).count()
precision = tp / max(1, detected.count())
recall = tp / max(1, truth.count())

with mlflow.start_run(run_name=f"anomaly-z{Z}"):
    mlflow.log_params({"z_threshold": Z, "min_ratio": RATIO, "window_days": 28, "lookback_days": LOOKBACK})
    mlflow.log_metrics({"precision": precision, "recall": recall, "detected": detected.count(),
                        "true_anomalies": truth.count()})
print(f"precision={precision:.2f} recall={recall:.2f}")
