"""Data-quality rules as code.

Rules are plain SQL boolean expressions, defined once and reused by:
* Lakeflow Declarative Pipelines expectations (drop / warn at ingestion time),
* the quarantine table (rows that failed, with the reason),
* the scheduled data-quality job (pass rates persisted to ops.dq_results).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

DROP = "drop"  # invalid rows are removed from silver and routed to quarantine
WARN = "warn"  # rows are kept, failures are measured and reported
FAIL = "fail"  # the run fails (used for table-level checks in the DQ job)


@dataclass(frozen=True)
class Rule:
    name: str
    expression: str
    severity: str = DROP
    description: str = ""

    def column(self) -> Column:
        # NULL counts as failure: a rule is only satisfied when the expression is TRUE.
        return F.coalesce(F.expr(self.expression), F.lit(False))


READING_RULES = [
    Rule("valid_meter_id", "meter_id RLIKE '^M[0-9]{6}$'", DROP, "Meter id follows head-end format"),
    Rule("valid_timestamp", "reading_ts IS NOT NULL", DROP, "Reading timestamp parsed"),
    Rule("valid_import", "kwh_import IS NOT NULL AND kwh_import >= 0 AND kwh_import < 50", DROP,
         "Imported energy is present and physically plausible for one hour"),
    Rule("valid_export", "kwh_export IS NOT NULL AND kwh_export >= 0 AND kwh_export < 50", DROP,
         "Exported (solar) energy is present and plausible"),
    Rule("plausible_voltage", "voltage_v BETWEEN 207 AND 253", WARN, "Within EN 50160 +/-10% of 230 V"),
]

CUSTOMER_RULES = [
    Rule("valid_customer_id", "customer_id RLIKE '^C[0-9]{6}$'", DROP),
    Rule("valid_region", "region IN ('Flanders', 'Wallonia', 'Brussels')", DROP),
    Rule("valid_tariff", "tariff_plan IN ('variable', 'fixed', 'dynamic')", DROP),
    Rule("valid_email", "email RLIKE '^[^@]+@[^@]+$'", WARN),
]

GRID_RULES = [
    Rule("valid_ts", "ts_utc IS NOT NULL", DROP),
    Rule("valid_price", "day_ahead_price_eur_mwh IS NOT NULL AND day_ahead_price_eur_mwh BETWEEN -500 AND 4000", DROP),
]


def expectations(rules: list[Rule], severity: str) -> dict:
    """Rules in the dict form expected by `@dlt.expect_all*` decorators."""
    return {r.name: r.expression for r in rules if r.severity == severity}


def all_pass(rules: list[Rule], severity: str = DROP) -> Column:
    cols = [r.column() for r in rules if r.severity == severity]
    out = F.lit(True)
    for c in cols:
        out = out & c
    return out


def failed_rules(rules: list[Rule], severity: str = DROP) -> Column:
    """Array with the names of the rules a row violates."""
    checks = [F.when(~r.column(), F.lit(r.name)) for r in rules if r.severity == severity]
    return F.filter(F.array(*checks), lambda x: x.isNotNull())


def split_valid_invalid(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame]:
    valid = df.where(all_pass(rules))
    invalid = df.where(~all_pass(rules)).withColumn("failed_rules", failed_rules(rules))
    return valid, invalid


def evaluate(df: DataFrame, rules: list[Rule], table: str) -> DataFrame:
    """One row per rule with failure counts and pass rate (single pass over the data)."""
    aggs = [F.count(F.lit(1)).alias("_total")]
    aggs += [F.sum(F.when(r.column(), 0).otherwise(1)).alias(r.name) for r in rules]
    row = df.agg(*aggs).collect()[0].asDict()
    total = int(row.pop("_total") or 0)
    spark = df.sparkSession
    data = []
    for r in rules:
        failed = int(row[r.name] or 0)
        rate = 1.0 if total == 0 else round(1 - failed / total, 6)
        data.append((table, r.name, r.severity, r.expression, total, failed, rate))
    return spark.createDataFrame(
        data, "table_name string, rule string, severity string, expression string, "
              "total_rows long, failed_rows long, pass_rate double")
