# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup and governance (idempotent)
# MAGIC
# MAGIC Creates the gold/ops/ml tables, the PII vault table, column masks, governance tags and the
# MAGIC static dimensions. Safe to run on every job execution: everything is `IF NOT EXISTS` / `OR REPLACE`.
# MAGIC
# MAGIC Schemas and the raw volume themselves are declared in the bundle (`resources/unity_catalog.yml`).

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from smartmeter.config import LakehouseNames

n = LakehouseNames.from_widgets(dbutils)
print(n)

# COMMAND ----------

# MAGIC %md ## PII vault: direct identifiers live in one masked table, never in gold

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {n.silver('customer_pii')} (
  customer_key STRING NOT NULL COMMENT 'Salted SHA-256 pseudonym, the only customer identifier used in gold',
  customer_id STRING COMMENT 'CRM business key (personal data)',
  full_name STRING, email STRING, phone STRING, street STRING,
  postal_code STRING, city STRING,
  snapshot_date DATE COMMENT 'CRM snapshot the values come from'
) COMMENT 'PII vault. Direct identifiers are masked for everyone outside the pii_readers group.'
""")

# Column mask: only members of `pii_readers` see clear values. In Free Edition there is no account
# console to create groups, so everyone sees masked values, which is exactly the safe default.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {n.ops('mask_pii')}(value STRING)
RETURNS STRING
COMMENT 'Shows PII only to members of pii_readers'
RETURN CASE WHEN is_account_group_member('pii_readers') THEN value
            WHEN value IS NULL THEN NULL
            ELSE concat(left(value, 1), '*****') END
""")
for col in ("customer_id", "full_name", "email", "phone", "street"):
    spark.sql(f"ALTER TABLE {n.silver('customer_pii')} ALTER COLUMN {col} SET MASK {n.ops('mask_pii')}")
    spark.sql(f"ALTER TABLE {n.silver('customer_pii')} ALTER COLUMN {col} SET TAGS ('pii' = 'direct_identifier')")
spark.sql(f"ALTER TABLE {n.silver('customer_pii')} "
          "SET TAGS ('data_classification' = 'confidential', 'gdpr' = 'personal_data')")

# COMMAND ----------

# MAGIC %md ## Gold star schema

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {n.gold('dim_customer')} (
  customer_sk BIGINT NOT NULL COMMENT 'Surrogate key of the version',
  customer_key STRING NOT NULL COMMENT 'Pseudonymous customer key (no direct PII in gold)',
  meter_id STRING, region STRING, postal_code STRING, city STRING, tariff_plan STRING,
  has_solar BOOLEAN, has_ev BOOLEAN, household_size INT, contract_start DATE,
  status STRING, churn_date DATE,
  row_hash STRING COMMENT 'SHA-256 of tracked attributes, used to detect changes',
  valid_from DATE NOT NULL, valid_to DATE COMMENT 'Exclusive end date; NULL for the current version',
  is_current BOOLEAN NOT NULL
) CLUSTER BY (customer_key)
COMMENT 'Customer dimension, SCD type 2, one row per version.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {n.gold('fact_daily_consumption')} (
  date_key INT, reading_date DATE, customer_sk BIGINT, customer_key STRING, meter_id STRING,
  region STRING, tariff_plan STRING,
  kwh_import DOUBLE COMMENT 'Energy taken from the grid (kWh)',
  kwh_export DOUBLE COMMENT 'Solar energy injected into the grid (kWh)',
  net_kwh DOUBLE, peak_hour_kwh DOUBLE, evening_peak_kwh DOUBLE COMMENT '17:00-21:59 local time',
  energy_cost_eur DOUBLE, readings_count BIGINT,
  expected_readings INT COMMENT '23/24/25 depending on daylight-saving changes',
  completeness_pct DOUBLE, _updated_at TIMESTAMP
) CLUSTER BY (reading_date, meter_id)
COMMENT 'Daily consumption per meter (grain: meter x local day). Incrementally merged.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {n.gold('meter_anomalies')} (
  meter_id STRING, customer_key STRING, reading_date DATE, kwh_import DOUBLE, hist_mean DOUBLE,
  z_score DOUBLE, anomaly_type STRING, detected_at TIMESTAMP
) COMMENT 'Meter-days with abnormal consumption (spikes, flatlines).'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {n.gold('churn_scores')} (
  customer_key STRING, cutoff_date DATE, churn_probability DOUBLE, risk_band STRING,
  model_version STRING, scored_at TIMESTAMP
) COMMENT 'Latest churn propensity per customer, produced by the champion model.'
""")

# Static dimensions (tiny, rebuilt every run).
spark.sql(f"""
CREATE OR REPLACE TABLE {n.gold('dim_tariff')} AS
SELECT * FROM VALUES
  ('variable', 'Variable (monthly indexed)', 0.12, CAST(NULL AS DOUBLE)),
  ('fixed',    'Fixed price 1 year',         CAST(NULL AS DOUBLE), 0.32),
  ('dynamic',  'Dynamic hourly (day-ahead)', 0.05, CAST(NULL AS DOUBLE))
AS t(tariff_plan, tariff_name, margin_eur_kwh, fixed_price_eur_kwh)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {n.gold('dim_date')} AS
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS calendar_date,
       year(d) AS year, quarter(d) AS quarter, month(d) AS month, date_format(d, 'MMMM') AS month_name,
       weekofyear(d) AS iso_week, date_format(d, 'EEEE') AS day_name, dayofweek(d) IN (1, 7) AS is_weekend,
       date_format(d, 'MM-dd') IN ('01-01','05-01','07-21','08-15','11-01','11-11','12-25') AS is_be_public_holiday
FROM (SELECT explode(sequence(DATE'2024-01-01', DATE'2028-12-31', INTERVAL 1 DAY)) AS d)
""")

# COMMAND ----------

# MAGIC %md ## Operations and ML tables

# COMMAND ----------

ddl = {
    n.ops("watermarks"): "name STRING, value TIMESTAMP, updated_at TIMESTAMP",
    n.ops("dq_results"): ("run_id STRING, checked_at TIMESTAMP, table_name STRING, rule STRING, severity STRING, "
                          "expression STRING, total_rows BIGINT, failed_rows BIGINT, pass_rate DOUBLE, status STRING"),
    n.ops("reconciliation_results"): ("run_id STRING, checked_at TIMESTAMP, check_name STRING, key STRING, "
                                      "source_value DOUBLE, target_value DOUBLE, difference DOUBLE, status STRING"),
    n.ops("freshness_checks"): ("run_id STRING, checked_at TIMESTAMP, table_name STRING, latest_ts TIMESTAMP, "
                                "lag_hours DOUBLE, max_lag_hours DOUBLE, status STRING"),
    n.ops("gdpr_erasure_log"): "customer_key STRING, requested_at TIMESTAMP, rows_deleted BIGINT, requested_by STRING",
    n.ml("drift_metrics"): ("checked_at TIMESTAMP, model_version STRING, feature STRING, psi DOUBLE, status STRING"),
}
for table, cols in ddl.items():
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({cols})")

# COMMAND ----------

# MAGIC %md ## Semantic views for BI and Genie

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {n.gold('v_daily_region_consumption')}
COMMENT 'Daily energy, cost and solar injection per Belgian region. Use for regional trend questions.'
AS SELECT f.reading_date, d.day_name, d.is_weekend, f.region, f.tariff_plan,
          count(DISTINCT f.customer_key) AS customers,
          round(sum(f.kwh_import), 1) AS kwh_import, round(sum(f.kwh_export), 1) AS kwh_export,
          round(sum(f.energy_cost_eur), 2) AS energy_cost_eur,
          round(avg(f.completeness_pct), 2) AS avg_completeness_pct
FROM {n.gold('fact_daily_consumption')} f
JOIN {n.gold('dim_date')} d ON d.date_key = f.date_key
WHERE f.customer_sk <> -1
GROUP BY ALL
""")
print("Setup complete")
