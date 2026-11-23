# Lakeflow Declarative Pipeline (formerly Delta Live Tables): bronze -> silver.
#
# * Bronze: Auto Loader incrementally ingests new files from the raw volume (exactly-once file
#   tracking via checkpoints), keeps lineage columns and rescues malformed fields instead of failing.
# * Silver: typed, validated, de-duplicated data. Rules come from smartmeter.quality so the pipeline,
#   the quarantine table and the scheduled DQ job share one definition.
# * Quarantine: rows that failed a DROP rule, with the reason, for investigation and replay.
#
# Gold (dimensional model with SCD2 and incremental MERGE) is built by a job task, see ADR-0001.

import dlt
from pyspark.sql import functions as F

from smartmeter.config import LakehouseNames
from smartmeter.quality import (
    CUSTOMER_RULES,
    DROP,
    GRID_RULES,
    READING_RULES,
    WARN,
    all_pass,
    expectations,
    failed_rules,
)

names = LakehouseNames.from_spark_conf(spark)

READING_SCHEMA = ("meter_id string, reading_ts string, kwh_import double, kwh_export double, "
                  "voltage_v double, firmware_version string")


def autoload(source: str, fmt: str, schema: str = None):
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("rescuedDataColumn", "_rescued_data"))
    if fmt == "csv":
        reader = reader.option("header", "true")
    if schema:
        reader = reader.schema(schema)
    else:
        reader = reader.option("cloudFiles.inferColumnTypes", "true")
    return (reader.load(names.raw_path(source))
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp()))


# ------------------------------------------------------------------------------- bronze
@dlt.table(name=names.bronze("meter_readings"),
           comment="Raw smart-meter telemetry as delivered by the head-end system.",
           cluster_by=["delivery_date"])
def bronze_meter_readings():
    return (autoload("meter_readings", "json", READING_SCHEMA)
            .withColumn("delivery_date", F.to_date(F.regexp_extract("_source_file", r"delivery_date=([0-9-]+)", 1))))


@dlt.table(name=names.bronze("crm_customers"), comment="Daily CRM customer snapshots (contains PII).",
           table_properties={"contains_pii": "true"})
def bronze_crm_customers():
    return autoload("crm_customers", "csv")


@dlt.table(name=names.bronze("grid_market"), comment="Hourly day-ahead price and grid generation mix.")
def bronze_grid_market():
    return autoload("grid_market", "csv")


# ------------------------------------------------------------------------------- silver
def typed_readings():
    return (dlt.read_stream(names.bronze("meter_readings"))
            .withColumn("reading_ts", F.to_timestamp("reading_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'")))


@dlt.table(name=names.silver("meter_readings"),
           comment="Validated hourly readings, de-duplicated on (meter_id, reading_ts) within a 3-day watermark.",
           cluster_by=["reading_date", "meter_id"])
@dlt.expect_all_or_drop(expectations(READING_RULES, DROP))
@dlt.expect_all(expectations(READING_RULES, WARN))
def silver_meter_readings():
    return (typed_readings()
            .withWatermark("reading_ts", "3 days")
            .dropDuplicatesWithinWatermark(["meter_id", "reading_ts"])
            .withColumn("reading_date", F.to_date(F.from_utc_timestamp("reading_ts", "Europe/Brussels")))
            .select("meter_id", "reading_ts", "reading_date", "kwh_import", "kwh_export", "voltage_v",
                    "firmware_version", "delivery_date", "_source_file", "_ingested_at"))


@dlt.table(name=names.silver("meter_readings_quarantine"),
           comment="Rows rejected by DROP rules, with the failed rule names, kept for triage and replay.")
def silver_meter_readings_quarantine():
    return (typed_readings()
            .where(~all_pass(READING_RULES, DROP))
            .withColumn("failed_rules", failed_rules(READING_RULES, DROP)))


@dlt.table(name=names.silver("crm_customers"), comment="Typed CRM snapshots. PII is isolated downstream.",
           table_properties={"contains_pii": "true"})
@dlt.expect_all_or_drop(expectations(CUSTOMER_RULES, DROP))
@dlt.expect_all(expectations(CUSTOMER_RULES, WARN))
def silver_crm_customers():
    return (dlt.read_stream(names.bronze("crm_customers"))
            .withColumn("snapshot_date", F.to_date("snapshot_date"))
            .withColumn("contract_start", F.to_date("contract_start"))
            .withColumn("churn_date", F.to_date("churn_date"))
            .withColumn("has_solar", F.col("has_solar").cast("boolean"))
            .withColumn("has_ev", F.col("has_ev").cast("boolean"))
            .withColumn("household_size", F.col("household_size").cast("int"))
            .withColumn("postal_code", F.lpad(F.col("postal_code").cast("string"), 4, "0"))
            .drop("_rescued_data"))


@dlt.table(name=names.silver("grid_market"), comment="Hourly market prices, one row per hour.")
@dlt.expect_all_or_drop(expectations(GRID_RULES, DROP))
def silver_grid_market():
    return (dlt.read_stream(names.bronze("grid_market"))
            .withColumn("ts_utc", F.to_timestamp("ts_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            .withWatermark("ts_utc", "3 days")
            .dropDuplicatesWithinWatermark(["ts_utc"])
            .drop("_rescued_data"))
