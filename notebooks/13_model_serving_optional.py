# Databricks notebook source
# MAGIC %md
# MAGIC # 13 · (Optional) Real-time model serving for the @champion churn model
# MAGIC
# MAGIC Batch scoring covers the daily use case. This notebook shows the online path: a serverless
# MAGIC Model Serving endpoint with scale-to-zero (no cost when idle) serving the UC-registered champion.
# MAGIC Free Edition limits the number of active endpoints; delete the endpoint when you are done.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.30" "mlflow>=2.16"
# MAGIC %restart_python

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
from mlflow.tracking import MlflowClient

from smartmeter.config import LakehouseNames

n = LakehouseNames.from_widgets(dbutils)
MODEL_NAME = n.ml("churn_model")
ENDPOINT = "smart-meter-churn"
version = MlflowClient(registry_uri="databricks-uc").get_model_version_by_alias(MODEL_NAME, "champion").version

w = WorkspaceClient()
entity = ServedEntityInput(entity_name=MODEL_NAME, entity_version=str(version), workload_size="Small",
                           scale_to_zero_enabled=True)
existing = [e.name for e in w.serving_endpoints.list()]
if ENDPOINT in existing:
    w.serving_endpoints.update_config_and_wait(name=ENDPOINT, served_entities=[entity])
else:
    w.serving_endpoints.create_and_wait(name=ENDPOINT, config=EndpointCoreConfigInput(served_entities=[entity]))
print(f"Endpoint {ENDPOINT} serves {MODEL_NAME} v{version}")

# COMMAND ----------

sample = spark.sql(f"SELECT * FROM {n.ml('churn_training_reference')} LIMIT 1").toPandas()
record = {"region": "Flanders", "tariff_plan": "variable", **{k: float(v) for k, v in sample.iloc[0].items()
                                                              if k != "model_version"}}
print(w.serving_endpoints.query(name=ENDPOINT, dataframe_records=[record]).predictions)

# COMMAND ----------

# Clean up to stay within Free Edition limits:
# w.serving_endpoints.delete(ENDPOINT)
