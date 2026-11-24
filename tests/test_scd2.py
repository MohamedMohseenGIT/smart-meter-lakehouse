import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F

from smartmeter.scd2 import compute_scd2_changes, row_hash

TRACKED = ["tariff_plan", "region"]
SNAP = "customer_key string, tariff_plan string, region string, snapshot_date string"


def current_dim(spark, rows):
    df = spark.createDataFrame(rows, "customer_key string, tariff_plan string, region string, valid_from string")
    return df.withColumn("valid_from", F.to_date("valid_from")).withColumn("row_hash", row_hash(TRACKED))


def as_dict(df):
    return sorted((r.customer_key, str(r.valid_from), str(r.valid_to), r.is_current, r._action) for r in df.collect())

def test_changes_unchanged_and_new_customers(spark):
    current = current_dim(spark, [("c1", "fixed", "Flanders", "2026-01-01"),
                                  ("c2", "variable", "Brussels", "2026-01-01")])
    snapshots = spark.createDataFrame([
        ("c1", "fixed", "Flanders", "2026-02-01"),      # unchanged
        ("c1", "dynamic", "Flanders", "2026-02-02"),    # tariff switch
        ("c1", "dynamic", "Flanders", "2026-02-03"),    # unchanged again
        ("c2", "variable", "Brussels", "2026-02-02"),   # never changes
        ("c3", "fixed", "Wallonia", "2026-02-02"),      # new customer
    ], SNAP)
    out = as_dict(compute_scd2_changes(current, snapshots, "customer_key", TRACKED))
    assert out == [
        ("c1", "2026-01-01", "2026-02-02", False, "close"),
        ("c1", "2026-02-02", "None", True, "insert"),
        ("c3", "2026-02-02", "None", True, "insert"),
    ]


def test_multiple_changes_in_one_batch_produce_closed_history(spark):
    current = current_dim(spark, [("x", "fixed", "Flanders", "2025-12-01")]).limit(0)
    snapshots = spark.createDataFrame([
        ("c9", "fixed", "Flanders", "2026-03-01"),
        ("c9", "variable", "Flanders", "2026-03-02"),
        ("c9", "fixed", "Flanders", "2026-03-03"),   # back to the original value = a new version
    ], SNAP)
    out = as_dict(compute_scd2_changes(current, snapshots, "customer_key", TRACKED))
    assert out == [
        ("c9", "2026-03-01", "2026-03-02", False, "insert"),
        ("c9", "2026-03-02", "2026-03-03", False, "insert"),
        ("c9", "2026-03-03", "None", True, "insert"),
    ]


def test_no_new_information_produces_no_changes(spark):
    current = current_dim(spark, [("c1", "fixed", "Flanders", "2026-01-01")])
    snapshots = spark.createDataFrame([("c1", "fixed", "Flanders", "2026-01-05")], SNAP)
    assert compute_scd2_changes(current, snapshots, "customer_key", TRACKED).count() == 0
