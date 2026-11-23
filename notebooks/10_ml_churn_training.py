# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Churn model: point-in-time features, out-of-time validation, gated promotion
# MAGIC
# MAGIC * **Features** from gold with no look-ahead (`smartmeter.features`): usage in the 60 days before a
# MAGIC   cutoff + the customer attributes valid on that cutoff (SCD2 as-of join).
# MAGIC * **Validation** on the most recent cutoffs (out-of-time), not a random split, because the model
# MAGIC   is used to predict the future.
# MAGIC * **MLflow** tracks params, metrics and the model; the model is registered in **Unity Catalog**.
# MAGIC * **Promotion gate**: the new version becomes `@champion` only if it beats the minimum AUC and is
# MAGIC   not worse than the current champion; otherwise it is tagged `@challenger`.

# COMMAND ----------

# MAGIC %pip install -q "mlflow>=2.16" "scikit-learn>=1.4"
# MAGIC %restart_python

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from smartmeter.config import LakehouseNames
from smartmeter.features import CATEGORICAL, FEATURES, LABEL, NUMERIC, training_set

n = LakehouseNames.from_widgets(dbutils)
dbutils.widgets.text("min_auc", "0.65")
MIN_AUC = float(dbutils.widgets.get("min_auc"))
MODEL_NAME = n.ml("churn_model")
mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/smart-meter-churn")

# COMMAND ----------

fact = spark.table(n.gold("fact_daily_consumption")).where("customer_sk <> -1")
dim = spark.table(n.gold("dim_customer"))
first_day, last_day = fact.agg(F.min("reading_date"), F.max("reading_date")).first()
cutoffs = (spark.sql(f"""SELECT explode(sequence(date_add(DATE'{first_day}', 60), date_sub(DATE'{last_day}', 30),
                                                 INTERVAL 7 DAY)) AS cutoff_date"""))
data = training_set(fact, dim, cutoffs).toPandas()
data["cutoff_date"] = pd.to_datetime(data["cutoff_date"])
print(f"{len(data):,} examples, {data['cutoff_date'].nunique()} cutoffs, churn rate {data[LABEL].mean():.2%}")

# Out-of-time split: the latest 2 cutoffs are the test set.
test_cutoffs = sorted(data["cutoff_date"].unique())[-2:]
train, test = data[~data["cutoff_date"].isin(test_cutoffs)], data[data["cutoff_date"].isin(test_cutoffs)]

# COMMAND ----------

def recall_at_top(y_true, scores, share=0.1):
    k = max(1, int(len(scores) * share))
    top = np.argsort(-scores)[:k]
    return float(np.asarray(y_true)[top].sum() / max(1, np.asarray(y_true).sum()))


model = Pipeline([
    ("prep", ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL),
        ("num", SimpleImputer(strategy="median"), NUMERIC),
    ])),
    ("clf", HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_leaf_nodes=15,
                                           class_weight="balanced", random_state=42)),
])

with mlflow.start_run(run_name="churn-hgb") as run:
    model.fit(train[FEATURES], train[LABEL])
    scores = model.predict_proba(test[FEATURES])[:, 1]
    metrics = {
        "test_roc_auc": roc_auc_score(test[LABEL], scores),
        "test_pr_auc": average_precision_score(test[LABEL], scores),
        "test_recall_top10pct": recall_at_top(test[LABEL].to_numpy(), scores),
        "test_base_rate": float(test[LABEL].mean()),
        "train_rows": len(train), "test_rows": len(test),
    }
    mlflow.log_params({"model": "HistGradientBoosting", "features": len(FEATURES), "obs_days": 60,
                       "label_days": 30, "test_cutoffs": ",".join(str(c.date()) for c in test_cutoffs)})
    mlflow.log_metrics(metrics)
    info = mlflow.sklearn.log_model(
        model, artifact_path="model", registered_model_name=MODEL_NAME,
        signature=infer_signature(test[FEATURES], scores), input_example=test[FEATURES].head(5))
    print(metrics)

# COMMAND ----------

# MAGIC %md ## Promotion gate

# COMMAND ----------

client = MlflowClient()
new_version = str(client.search_model_versions(f"name = '{MODEL_NAME}' and run_id = '{run.info.run_id}'")[0].version)
try:
    champion = client.get_model_version_by_alias(MODEL_NAME, "champion")
    champion_auc = client.get_run(champion.run_id).data.metrics.get("test_roc_auc", 0.0)
except Exception:
    champion, champion_auc = None, 0.0

new_auc = metrics["test_roc_auc"]
promote = new_auc >= MIN_AUC and new_auc >= champion_auc - 0.01
client.set_registered_model_alias(MODEL_NAME, "champion" if promote else "challenger", new_version)
client.set_model_version_tag(MODEL_NAME, new_version, "validation_status", "approved" if promote else "rejected")
print(f"version {new_version}: auc={new_auc:.3f} vs champion={champion_auc:.3f} -> "
      f"{'PROMOTED to @champion' if promote else 'kept as @challenger'}")

# Reference distribution for drift monitoring (what the model was trained on).
(spark.createDataFrame(train[NUMERIC].astype(float))
 .withColumn("model_version", F.lit(new_version))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(n.ml("churn_training_reference")))
