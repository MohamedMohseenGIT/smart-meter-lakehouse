"""Churn feature engineering with point-in-time correctness.

For every cutoff date C:
* features use only facts strictly before C (observation window [C - obs_days, C)),
* customer attributes come from the SCD2 version that was valid on C (no look-ahead),
* the label is 1 when the customer churned in [C, C + label_days).

Using several cutoffs (e.g. weekly) multiplies training examples and mirrors how the model is
scored in production (cutoff = today, label unknown).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CATEGORICAL = ["region", "tariff_plan"]
NUMERIC = ["tenure_days", "household_size", "has_solar", "has_ev", "avg_daily_kwh", "std_daily_kwh",
           "max_daily_kwh", "avg_export_kwh", "export_ratio", "avg_evening_peak_kwh", "avg_daily_cost_eur",
           "trend_ratio", "days_observed", "plan_changes_90d"]
FEATURES = CATEGORICAL + NUMERIC
LABEL = "churned_next_30d"


def customer_features(fact: DataFrame, dim: DataFrame, cutoffs: DataFrame, obs_days: int = 60) -> DataFrame:
    """One row per (customer_key, cutoff_date) for customers active on the cutoff date."""
    asof = (dim.join(cutoffs, (dim["valid_from"] <= cutoffs["cutoff_date"])
                     & (dim["valid_to"].isNull() | (cutoffs["cutoff_date"] < dim["valid_to"])))
            .where(F.col("status") == "active")
            .select("customer_key", "cutoff_date", "region", "tariff_plan", "household_size",
                    F.col("has_solar").cast("int").alias("has_solar"),
                    F.col("has_ev").cast("int").alias("has_ev"),
                    F.datediff("cutoff_date", F.to_date("contract_start")).alias("tenure_days")))

    plan_changes = (dim.join(cutoffs, (dim["valid_from"] < cutoffs["cutoff_date"])
                             & (dim["valid_from"] >= F.date_sub(cutoffs["cutoff_date"], 90)))
                    .groupBy("customer_key", "cutoff_date")
                    .agg((F.countDistinct("tariff_plan") - 1).alias("plan_changes_90d")))

    window = fact.join(cutoffs, (fact["reading_date"] < cutoffs["cutoff_date"])
                       & (fact["reading_date"] >= F.date_sub(cutoffs["cutoff_date"], obs_days)))
    recent = F.col("reading_date") >= F.date_sub(F.col("cutoff_date"), 14)
    usage = (window.groupBy("customer_key", "cutoff_date")
             .agg(F.avg("kwh_import").alias("avg_daily_kwh"),
                  F.stddev_samp("kwh_import").alias("std_daily_kwh"),
                  F.max("kwh_import").alias("max_daily_kwh"),
                  F.avg("kwh_export").alias("avg_export_kwh"),
                  (F.sum("kwh_export") / F.greatest(F.sum("kwh_import"), F.lit(0.001))).alias("export_ratio"),
                  F.avg("evening_peak_kwh").alias("avg_evening_peak_kwh"),
                  F.avg("energy_cost_eur").alias("avg_daily_cost_eur"),
                  (F.avg(F.when(recent, F.col("kwh_import")))
                   / F.greatest(F.avg(F.when(~recent, F.col("kwh_import"))), F.lit(0.001))).alias("trend_ratio"),
                  F.countDistinct("reading_date").alias("days_observed")))

    return (asof.join(usage, ["customer_key", "cutoff_date"], "left")
            .join(plan_changes, ["customer_key", "cutoff_date"], "left")
            .fillna(0, subset=[c for c in NUMERIC if c not in ("tenure_days", "household_size")]))


def churn_labels(dim: DataFrame, cutoffs: DataFrame, label_days: int = 30) -> DataFrame:
    churn = (dim.where(F.col("is_current"))
             .select("customer_key", F.to_date("churn_date").alias("churn_dt")))
    return (churn.crossJoin(cutoffs)
            .withColumn(LABEL, F.when(F.col("churn_dt").isNotNull()
                                      & (F.col("churn_dt") >= F.col("cutoff_date"))
                                      & (F.col("churn_dt") < F.date_add(F.col("cutoff_date"), label_days)), 1)
                        .otherwise(0))
            .select("customer_key", "cutoff_date", LABEL))


def training_set(fact: DataFrame, dim: DataFrame, cutoffs: DataFrame, label_days: int = 30) -> DataFrame:
    return (customer_features(fact, dim, cutoffs)
            .join(churn_labels(dim, cutoffs, label_days), ["customer_key", "cutoff_date"], "inner"))
