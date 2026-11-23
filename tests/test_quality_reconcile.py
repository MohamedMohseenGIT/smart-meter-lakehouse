import pytest

pytest.importorskip("pyspark")

from smartmeter.quality import READING_RULES, Rule, evaluate, expectations, split_valid_invalid
from smartmeter.reconcile import reconcile, summary

SCHEMA = "meter_id string, reading_ts string, kwh_import double, kwh_export double, voltage_v double"


def test_rules_split_valid_and_invalid_with_reasons(spark):
    df = spark.createDataFrame([
        ("M000001", "2026-06-01", 0.5, 0.0, 230.0),
        ("M000002", "2026-06-01", -1.0, 0.0, 230.0),   # negative import
        ("BAD", "2026-06-01", 0.5, 0.0, 230.0),        # bad meter id
        ("M000003", "2026-06-01", None, 0.0, 230.0),   # null treated as failure
        ("M000004", "2026-06-01", 0.4, 0.0, 300.0),    # voltage only warns -> stays valid
    ], SCHEMA)
    valid, invalid = split_valid_invalid(df, READING_RULES)
    assert {r.meter_id for r in valid.collect()} == {"M000001", "M000004"}
    reasons = {r.meter_id: r.failed_rules for r in invalid.collect()}
    assert reasons["M000002"] == ["valid_import"] and reasons["BAD"] == ["valid_meter_id"]


def test_evaluate_reports_pass_rate_per_rule(spark):
    df = spark.createDataFrame([(1,), (2,), (-3,), (None,)], "x int")
    out = {r.rule: r for r in evaluate(df, [Rule("positive", "x > 0")], "t").collect()}
    assert out["positive"].failed_rows == 2 and out["positive"].pass_rate == 0.5


def test_expectations_dict_by_severity():
    assert "plausible_voltage" in expectations(READING_RULES, "warn")
    assert "plausible_voltage" not in expectations(READING_RULES, "drop")


def test_reconcile_statuses(spark):
    src = spark.createDataFrame([("a", 1.0), ("a", 2.0), ("b", 5.0), ("c", 1.0)], "k string, v double")
    tgt = spark.createDataFrame([("a", 3.0), ("b", 4.0), ("d", 1.0)], "k string, v double")
    res = {r.k: r.status for r in reconcile(src, tgt, ["k"], ["v"], compare_counts=False).collect()}
    assert res == {"a": "MATCH", "b": "MISMATCH", "c": "MISSING_IN_TARGET", "d": "MISSING_IN_SOURCE"}
    strict = {r.k: r.status for r in reconcile(src, tgt, ["k"], ["v"]).collect()}
    assert strict["a"] == "MISMATCH"  # 2 rows vs 1 row when counts are compared
    assert summary(reconcile(src, tgt, ["k"], ["v"], abs_tolerance=1.0, compare_counts=False))["MATCH"] == 2
