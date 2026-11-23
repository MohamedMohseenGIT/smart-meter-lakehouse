# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Batch scoring with the @champion model + feature drift monitoring
# MAGIC
# MAGIC Scores every active customer as of today, writes `gold.churn_scores` (idempotent per cutoff),
# MAGIC and compares today's feature distributions with the training reference using the
# MAGIC **Population Stability Index**. PSI > 0.2 on any feature is a signal to retrain.

# COMMAND ----------

# MAGIC %pip install -q "mlflow>=2.16" "scikit-learn>=1.4"
# MAGIC %restart_python

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F

from smartmeter.config import LakehouseNames
from smartmeter.features import FEATURES, NUMERIC, customer_features
from smartmeter.monitoring import drift_status, psi

n = LakehouseNames.from_widgets(dbutils)
mlflow.set_registry_uri("databricks-uc")
MODEL_NAME = n.ml("churn_model")
champion = MlflowClient().get_model_version_by_alias(MODEL_NAME, "champion")
model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@champion")

# COMMAND ----------

fact = spark.table(n.gold("fact_daily_consumption")).where("customer_sk <> -1")
cutoff = fact.agg(F.date_add(F.max("reading_date"), 1)).first()[0]
cutoffs = spark.createDataFrame([(cutoff,)], "cutoff_date date")
current = customer_features(fact, spark.table(n.gold("dim_customer")), cutoffs).toPandas()

current["churn_probability"] = model.predict_proba(current[FEATURES])[:, 1]
current["risk_band"] = current["churn_probability"].map(
    lambda p: "high" if p >= 0.6 else ("medium" if p >= 0.3 else "low"))
scores = (spark.createDataFrame(current[["customer_key", "cutoff_date", "churn_probability", "risk_band"]])
          .withColumn("cutoff_date", F.to_date("cutoff_date"))
          .withColumn("model_version", F.lit(str(champion.version)))
          .withColumn("scored_at", F.current_timestamp()))
(scores.write.mode("overwrite").option("replaceWhere", f"cutoff_date = DATE'{cutoff}'")
 .saveAsTable(n.gold("churn_scores")))
display(scores.groupBy("risk_band").count())

# COMMAND ----------

reference = spark.table(n.ml("churn_training_reference")).toPandas()
rows = []
for feature in NUMERIC:
    value = psi(reference[feature], current[feature].astype(float))
    rows.append((str(champion.version), feature, float(value), drift_status(value)))
drift = (spark.createDataFrame(rows, "model_version string, feature string, psi double, status string")
         .withColumn("checked_at", F.current_timestamp())
         .select("checked_at", "model_version", "feature", "psi", "status"))
drift.write.mode("append").saveAsTable(n.ml("drift_metrics"))
display(drift.orderBy(F.col("psi").desc()))

drifted = [r["feature"] for r in drift.where("status = 'DRIFT'").collect()]
if drifted:
    print(f"WARNING: significant drift on {drifted} - schedule retraining")
