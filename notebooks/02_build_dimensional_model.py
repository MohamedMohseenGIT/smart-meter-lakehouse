# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Dimensional model: SCD2 customer dimension + incremental daily fact
# MAGIC
# MAGIC 1. **PII vault** – latest direct identifiers per customer are upserted into the masked
# MAGIC    `silver.customer_pii`; gold only ever sees a salted pseudonym (`customer_key`).
# MAGIC 2. **dim_customer (SCD2)** – all CRM snapshots newer than the dimension are turned into versions
# MAGIC    with window functions and applied in **one MERGE** (see `smartmeter.scd2`).
# MAGIC 3. **fact_daily_consumption** – only *dates touched by newly ingested rows* are recomputed
# MAGIC    (late-arriving data included) and merged on `(meter_id, reading_date)`. A watermark on
# MAGIC    `_ingested_at` makes the step incremental and safely re-runnable.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from pyspark.sql import Window
from pyspark.sql import functions as F

from smartmeter.config import LakehouseNames
from smartmeter.scd2 import compute_scd2_changes, merge_scd2
from smartmeter.transforms import add_local_time, attach_customer_sk, daily_consumption, hourly_cost

n = LakehouseNames.from_widgets(dbutils)

# The salt makes customer_key non-reversible. Store it in a secret scope; fall back for demos.
try:
    SALT = dbutils.secrets.get("smartmeter", "customer_key_salt")
except Exception:
    SALT = "demo-salt-change-me"
    print("WARNING: secret smartmeter/customer_key_salt not found, using demo salt")


def customer_key(col):
    return F.sha2(F.concat(col, F.lit(SALT)), 256)

# COMMAND ----------

# MAGIC %md ## 1-2 · PII vault and SCD2 customer dimension

# COMMAND ----------

dim_table = n.gold("dim_customer")
last_version = spark.table(dim_table).agg(F.max("valid_from")).first()[0]
snapshots = spark.table(n.silver("crm_customers")).withColumn("customer_key", customer_key(F.col("customer_id")))
if last_version is not None:
    snapshots = snapshots.where(F.col("snapshot_date") > F.lit(last_version))
snapshots = snapshots.cache()
new_snapshot_days = snapshots.select("snapshot_date").distinct().count()
print(f"Dimension valid up to {last_version}; {new_snapshot_days} new snapshot day(s) to process")

# COMMAND ----------

if new_snapshot_days:
    latest_pii = (snapshots
                  .withColumn("_rn", F.row_number().over(
                      Window.partitionBy("customer_key").orderBy(F.col("snapshot_date").desc())))
                  .where("_rn = 1")
                  .select("customer_key", "customer_id", "full_name", "email", "phone", "street",
                          "postal_code", "city", "snapshot_date"))
    latest_pii.createOrReplaceTempView("_latest_pii")
    spark.sql(f"""
        MERGE INTO {n.silver('customer_pii')} t USING _latest_pii s ON t.customer_key = s.customer_key
        WHEN MATCHED AND s.snapshot_date > t.snapshot_date THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    tracked = ["meter_id", "region", "postal_code", "city", "tariff_plan", "has_solar", "has_ev",
               "household_size", "contract_start", "status", "churn_date"]
    current = spark.table(dim_table).where("is_current")
    changes = (compute_scd2_changes(current, snapshots, key="customer_key", tracked_cols=tracked)
               .withColumn("customer_sk", F.xxhash64("customer_key", "valid_from"))
               .withColumn("household_size", F.col("household_size").cast("int")))
    insert_cols = ["customer_sk", "customer_key", *tracked, "row_hash", "valid_from", "valid_to", "is_current"]
    merge_scd2(spark, dim_table, changes, key="customer_key", insert_cols=insert_cols)

display(spark.sql(f"""
  SELECT count(*) AS versions, count_if(is_current) AS current_rows,
         count(DISTINCT customer_key) AS customers, max(valid_from) AS latest_change
  FROM {dim_table}"""))

# COMMAND ----------

# MAGIC %md ## 3 · Incremental fact load driven by an ingestion watermark

# COMMAND ----------

wm_row = spark.sql(f"SELECT value FROM {n.ops('watermarks')} WHERE name = 'fact_daily_consumption'").first()
watermark = wm_row[0] if wm_row else None

silver = spark.table(n.silver("meter_readings"))
new_rows = silver if watermark is None else silver.where(F.col("_ingested_at") > F.lit(watermark))
new_watermark = new_rows.agg(F.max("_ingested_at")).first()[0]

if new_watermark is None:
    dbutils.notebook.exit("No new silver rows since last run - nothing to do")

affected_dates = new_rows.select("reading_date").distinct().cache()
print(f"Recomputing {affected_dates.count()} reading date(s); watermark {watermark} -> {new_watermark}")

# COMMAND ----------

hourly = (silver.join(F.broadcast(affected_dates), "reading_date", "left_semi")
          .transform(add_local_time))
hourly = attach_customer_sk(hourly, spark.table(dim_table))
hourly = hourly_cost(hourly, spark.table(n.silver("grid_market")), spark.table(n.gold("dim_tariff")))
daily = (daily_consumption(hourly, extra_keys=("customer_sk", "customer_key", "region", "tariff_plan"))
         .withColumn("_updated_at", F.current_timestamp()))
daily.createOrReplaceTempView("_daily_updates")

spark.sql(f"""
  MERGE INTO {n.gold('fact_daily_consumption')} t
  USING _daily_updates s
    ON t.meter_id = s.meter_id AND t.reading_date = s.reading_date
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")

# Advance the watermark only after the MERGE succeeded (at-least-once + idempotent MERGE = exactly-once result).
spark.sql(f"""
  MERGE INTO {n.ops('watermarks')} t
  USING (SELECT 'fact_daily_consumption' AS name, TIMESTAMP'{new_watermark}' AS value,
                current_timestamp() AS updated_at) s
  ON t.name = s.name
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md ## Grid summary (small table, rebuilt from silver)

# COMMAND ----------

(spark.table(n.silver("grid_market"))
 .withColumn("market_local_date", F.to_date(F.from_utc_timestamp("ts_utc", "Europe/Brussels")))
 .groupBy("market_local_date")
 .agg(F.round(F.avg("day_ahead_price_eur_mwh"), 2).alias("avg_price_eur_mwh"),
      F.max("day_ahead_price_eur_mwh").alias("max_price_eur_mwh"),
      F.min("day_ahead_price_eur_mwh").alias("min_price_eur_mwh"),
      F.round(F.sum(F.col("solar_mw") + F.col("wind_mw")) / F.sum("total_load_mw"), 4).alias("renewable_share"))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(n.gold("grid_daily")))

display(spark.table(n.gold("fact_daily_consumption")).groupBy("reading_date")
        .agg(F.count("*").alias("meters"), F.round(F.sum("kwh_import"), 1).alias("kwh"))
        .orderBy(F.col("reading_date").desc()).limit(10))
