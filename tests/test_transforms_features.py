import pytest

pytest.importorskip("pyspark")

from datetime import date, timedelta

from pyspark.sql import functions as F

from smartmeter.features import churn_labels, customer_features
from smartmeter.transforms import add_local_time, attach_customer_sk, daily_consumption, robust_daily_anomalies


def test_local_date_and_dst_completeness(spark):
    # 2026-10-25 has 25 local hours: 22:00Z on the 24th .. 22:00Z on the 25th (CEST->CET at 01:00Z)
    hours = [("M000001", f"2026-10-24 {h:02d}:00:00") for h in (22, 23)] + \
            [("M000001", f"2026-10-25 {h:02d}:00:00") for h in range(0, 23)]
    df = (spark.createDataFrame(hours, "meter_id string, ts string")
          .withColumn("reading_ts", F.to_timestamp("ts"))
          .withColumn("kwh_import", F.lit(1.0)).withColumn("kwh_export", F.lit(0.0))
          .withColumn("energy_cost_eur", F.lit(0.3)))
    daily = daily_consumption(add_local_time(df)).collect()
    assert len(daily) == 1
    row = daily[0]
    assert str(row.reading_date) == "2026-10-25"
    assert row.readings_count == 25 and row.expected_readings == 25 and row.completeness_pct == 100.0
    assert row.kwh_import == 25.0 and row.date_key == 20261025


def test_as_of_lookup_picks_version_valid_on_the_day(spark):
    dim = spark.createDataFrame([
        ("M1", 11, "k1", "fixed", "Flanders", "2026-01-01", "2026-02-01"),
        ("M1", 12, "k1", "dynamic", "Flanders", "2026-02-01", None),
    ], "meter_id string, customer_sk long, customer_key string, tariff_plan string, region string, "
       "valid_from string, valid_to string").selectExpr(
        "meter_id", "customer_sk", "customer_key", "tariff_plan", "region",
        "to_date(valid_from) valid_from", "to_date(valid_to) valid_to")
    facts = spark.createDataFrame([("M1", "2026-01-31"), ("M1", "2026-02-01"), ("M9", "2026-02-01")],
                                  "meter_id string, d string").selectExpr("meter_id", "to_date(d) reading_date")
    out = {(r.meter_id, str(r.reading_date)): r.customer_sk for r in attach_customer_sk(facts, dim).collect()}
    assert out == {("M1", "2026-01-31"): 11, ("M1", "2026-02-01"): 12, ("M9", "2026-02-01"): -1}


def test_spike_detection_uses_meter_history(spark):
    start = date(2026, 5, 1)
    rows = [("M1", start + timedelta(days=i), 10.0 + (i % 3) * 0.5, 100.0) for i in range(30)]
    rows.append(("M1", start + timedelta(days=30), 60.0, 100.0))   # spike
    df = spark.createDataFrame(rows, "meter_id string, reading_date date, kwh_import double, completeness_pct double")
    flagged = {(str(r.reading_date), r.anomaly_type) for r in robust_daily_anomalies(df).collect() if r.anomaly_type}
    assert flagged == {(str(start + timedelta(days=30)), "spike")}


def _dim(spark):
    return spark.createDataFrame([
        ("k1", "Flanders", "fixed", 2, True, False, "2024-01-01", "active", None, "2026-01-01", "2026-03-10", False),
        ("k1", "Flanders", "fixed", 2, True, False, "2024-01-01", "churned", "2026-03-10", "2026-03-10", None, True),
        ("k2", "Brussels", "variable", 1, False, False, "2025-01-01", "active", None, "2026-01-01", None, True),
    ], "customer_key string, region string, tariff_plan string, household_size int, has_solar boolean, "
       "has_ev boolean, contract_start string, status string, churn_date string, valid_from string, "
       "valid_to string, is_current boolean").withColumn(
        "valid_from", F.to_date("valid_from")).withColumn("valid_to", F.to_date("valid_to"))


def test_labels_and_point_in_time_features(spark):
    dim = _dim(spark)
    cutoffs = spark.createDataFrame([(date(2026, 3, 1),)], "cutoff_date date")
    labels = {r.customer_key: r.churned_next_30d for r in churn_labels(dim, cutoffs).collect()}
    assert labels == {"k1": 1, "k2": 0}

    fact = spark.createDataFrame(
        [("k1", date(2026, 2, 20), 10.0, 0.0, 3.0, 2.0), ("k1", date(2026, 3, 5), 999.0, 0.0, 3.0, 2.0)],
        "customer_key string, reading_date date, kwh_import double, kwh_export double, "
        "evening_peak_kwh double, energy_cost_eur double")
    feats = {r.customer_key: r for r in customer_features(fact, dim, cutoffs).collect()}
    assert set(feats) == {"k1", "k2"}                 # both active on the cutoff date
    assert feats["k1"].avg_daily_kwh == 10.0          # the post-cutoff 999 kWh is not used (no leakage)
    assert feats["k2"].days_observed == 0
