# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Data quality and reconciliation gate
# MAGIC
# MAGIC Runs after the gold load and **fails the job** when a blocking check fails, so bad data never
# MAGIC silently reaches dashboards or models. Every result is persisted for trend analysis.
# MAGIC
# MAGIC | Check | Compares | Severity |
# MAGIC |---|---|---|
# MAGIC | Upstream control totals | rows the head-end says it delivered vs rows in bronze | fail |
# MAGIC | Energy balance | silver hourly kWh vs gold daily kWh per date | fail |
# MAGIC | Rule pass rates | `READING_RULES`, `CUSTOMER_RULES` on recent data | warn |
# MAGIC | Referential integrity | readings from meters unknown to the CRM | warn / fail |
# MAGIC | Completeness | received vs expected hourly readings (DST aware) | warn |
# MAGIC | Quarantine rate | rejected rows per delivery | warn |

# COMMAND ----------

import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from pyspark.sql import functions as F

from smartmeter.config import LakehouseNames
from smartmeter.quality import CUSTOMER_RULES, READING_RULES, evaluate
from smartmeter.reconcile import reconcile, summary

n = LakehouseNames.from_widgets(dbutils)
dbutils.widgets.text("lookback_days", "7")
LOOKBACK = int(dbutils.widgets.get("lookback_days"))
RUN_ID = str(uuid.uuid4())
blocking_failures: list[str] = []
recon_rows = []

# COMMAND ----------

# MAGIC %md ## 1 · Upstream control totals → bronze

# COMMAND ----------

# One row per delivery on both sides: the declared row count vs the loaded row count.
control = (spark.read.json(f"{n.raw_root}/_audit/*/control_totals.json", multiLine=True)
           .select(F.to_date("delivery_date").alias("delivery_date"),
                   F.col("meter_readings_rows").cast("long").alias("row_count")))
bronze = (spark.table(n.bronze("meter_readings")).groupBy("delivery_date")
          .agg(F.count("*").alias("row_count")))
res = reconcile(control, bronze, ["delivery_date"], ["row_count"], compare_counts=False)
s = summary(res)
print("control totals vs bronze:", s)
for r in res.collect():
    recon_rows.append((RUN_ID, "control_totals_vs_bronze", str(r["delivery_date"]),
                       float(r["src_row_count"] or 0), float(r["tgt_row_count"] or 0),
                       float(r["diff_row_count"] or 0), r["status"]))
if s["MISMATCH"] or s["MISSING_IN_TARGET"]:
    blocking_failures.append(f"control totals vs bronze: {s}")

# COMMAND ----------

# MAGIC %md ## 2 · Energy balance: silver hourly → gold daily

# COMMAND ----------

since = F.date_sub(F.current_date(), LOOKBACK)
silver_energy = spark.table(n.silver("meter_readings")).where(F.col("reading_date") >= since).select(
    "reading_date", "kwh_import")
gold_energy = spark.table(n.gold("fact_daily_consumption")).where(F.col("reading_date") >= since).select(
    "reading_date", "kwh_import")
# Row counts differ by design (hourly vs daily grain): judge on energy only. Gold rounds to 4 decimals
# per meter-day, hence a small absolute tolerance.
energy = reconcile(silver_energy, gold_energy, ["reading_date"], ["kwh_import"], abs_tolerance=0.5,
                   rel_tolerance=1e-6, compare_counts=False)
s = summary(energy)
print("silver vs gold energy:", s)
for r in energy.collect():
    recon_rows.append((RUN_ID, "silver_vs_gold_kwh", str(r["reading_date"]), float(r["src_kwh_import"] or 0),
                       float(r["tgt_kwh_import"] or 0), float(r["diff_kwh_import"] or 0), r["status"]))
if s["MISMATCH"] or s["MISSING_IN_TARGET"]:
    blocking_failures.append(f"energy balance silver vs gold: {s}")

# COMMAND ----------

# MAGIC %md ## 3 · Rule pass rates, referential integrity, completeness, quarantine

# COMMAND ----------

recent = spark.table(n.silver("meter_readings")).where(F.col("reading_date") >= since)
latest_snapshot = spark.table(n.silver("crm_customers")).agg(F.max("snapshot_date")).first()[0]
customers = spark.table(n.silver("crm_customers")).where(F.col("snapshot_date") == F.lit(latest_snapshot))

results = evaluate(recent, READING_RULES, n.silver("meter_readings")).unionByName(
    evaluate(customers, CUSTOMER_RULES, n.silver("crm_customers")))

known = spark.table(n.gold("dim_customer")).select("meter_id").distinct()
total = recent.count()
orphans = recent.join(known, "meter_id", "left_anti").count()
orphan_rate = orphans / total if total else 0.0

completeness = (spark.table(n.gold("fact_daily_consumption"))
                .where((F.col("reading_date") >= since) & (F.col("customer_sk") != -1))
                .agg(F.avg("completeness_pct")).first()[0] or 0.0)

quarantined = spark.table(n.silver("meter_readings_quarantine")).where(
    F.col("delivery_date") >= since).count()
bronze_recent = spark.table(n.bronze("meter_readings")).where(F.col("delivery_date") >= since).count()
quarantine_rate = quarantined / bronze_recent if bronze_recent else 0.0

table_checks = spark.createDataFrame([
    (n.silver("meter_readings"), "referential_integrity_unknown_meters", "fail" if orphan_rate > 0.02 else "warn",
     "share of readings from meters unknown to CRM <= 0.5%", total, orphans, 1 - orphan_rate),
    (n.gold("fact_daily_consumption"), "avg_completeness_pct >= 97", "warn",
     "received / expected hourly readings", 100, int(round(100 - completeness)), completeness / 100),
    (n.bronze("meter_readings"), "quarantine_rate <= 1%", "warn", "rows rejected by DROP rules",
     bronze_recent, quarantined, 1 - quarantine_rate),
], "table_name string, rule string, severity string, expression string, total_rows long, failed_rows long, "
   "pass_rate double")

thresholds = (F.when(F.col("rule").startswith("referential"), 0.995)
              .when(F.col("rule").startswith("avg_completeness"), 0.97)
              .when(F.col("rule").startswith("quarantine"), 0.99)
              .otherwise(0.999))
dq = (results.unionByName(table_checks)
      .withColumn("status", F.when(F.col("pass_rate") >= thresholds, "PASS")
                  .when(F.col("severity") == "fail", "FAIL").otherwise("WARN"))
      .withColumn("run_id", F.lit(RUN_ID)).withColumn("checked_at", F.current_timestamp()))
dq.select("run_id", "checked_at", "table_name", "rule", "severity", "expression", "total_rows", "failed_rows",
          "pass_rate", "status").write.mode("append").saveAsTable(n.ops("dq_results"))
display(dq.orderBy("status", "table_name"))
blocking_failures += [f"{r['table_name']}.{r['rule']}" for r in dq.where("status = 'FAIL'").collect()]

# COMMAND ----------

(spark.createDataFrame(recon_rows, "run_id string, check_name string, key string, source_value double, "
                                   "target_value double, difference double, status string")
 .withColumn("checked_at", F.current_timestamp())
 .select("run_id", "checked_at", "check_name", "key", "source_value", "target_value", "difference", "status")
 .write.mode("append").saveAsTable(n.ops("reconciliation_results")))

if blocking_failures:
    raise AssertionError(f"Data quality gate failed (run {RUN_ID}): {blocking_failures}")
print(f"Data quality gate passed (run {RUN_ID})")
