# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · GDPR right to erasure (Art. 17)
# MAGIC
# MAGIC Because direct identifiers live **only** in the PII vault (`silver.customer_pii`), erasure is a
# MAGIC targeted delete there plus a purge of old file versions, instead of a hunt across every table.
# MAGIC Gold keeps the pseudonymous `customer_key`; once the vault row is gone it can no longer be linked
# MAGIC back to a person.
# MAGIC
# MAGIC Raw CRM files and bronze/silver CRM snapshots still contain the identifiers until their retention
# MAGIC window expires (see `docs/adr/0002-pii-isolation-and-masking.md` for the retention design).

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from smartmeter.config import LakehouseNames

n = LakehouseNames.from_widgets(dbutils)
dbutils.widgets.text("customer_id", "")
customer_id = dbutils.widgets.get("customer_id").strip()
assert customer_id, "Provide the customer_id to erase"

try:
    salt = dbutils.secrets.get("smartmeter", "customer_key_salt")
except Exception:
    salt = "demo-salt-change-me"

# COMMAND ----------

key = spark.sql("SELECT sha2(concat(:cid, :salt), 256) AS k", args={"cid": customer_id, "salt": salt}).first()["k"]
deleted = spark.sql(f"DELETE FROM {n.silver('customer_pii')} WHERE customer_key = :k", args={"k": key}).first()
rows = deleted["num_affected_rows"] if deleted else 0
spark.sql(f"""INSERT INTO {n.ops('gdpr_erasure_log')}
              SELECT :k, current_timestamp(), :rows, current_user()""", args={"k": key, "rows": rows})
# Physically remove the deleted rows from older file versions once the time-travel window has passed.
spark.sql(f"REORG TABLE {n.silver('customer_pii')} APPLY (PURGE)")
print(f"Erased {rows} PII row(s) for the requested customer")
