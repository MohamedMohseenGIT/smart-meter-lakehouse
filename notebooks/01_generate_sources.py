# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Land synthetic upstream files (interactive alternative)
# MAGIC
# MAGIC The scheduled job runs the same code as a **Python wheel task** (`generate_sources` entry point).
# MAGIC This notebook is handy for exploring or backfilling by hand. It is incremental: it continues
# MAGIC from the last generated day stored in `_state/manifest.json` in the raw volume.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from smartmeter.config import LakehouseNames
from smartmeter.generator import GeneratorConfig, run

n = LakehouseNames.from_widgets(dbutils)
dbutils.widgets.text("customers", "1000")
dbutils.widgets.text("backfill_days", "120")

# COMMAND ----------

cfg = GeneratorConfig(n_customers=int(dbutils.widgets.get("customers")),
                      backfill_days=int(dbutils.widgets.get("backfill_days")))
result = run(n.raw_root, cfg)
print(result)

# COMMAND ----------

display(dbutils.fs.ls(n.raw_root))
