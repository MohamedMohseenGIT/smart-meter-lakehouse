"""Gold-layer transformations shared by the dimensional-model job and the tests."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

TZ = "Europe/Brussels"
UNKNOWN_SK = -1


def add_local_time(readings: DataFrame) -> DataFrame:
    """Belgian local date/hour from UTC timestamps (handles 23h and 25h DST days)."""
    local = F.from_utc_timestamp(F.col("reading_ts"), TZ)
    return readings.withColumn("reading_date", F.to_date(local)).withColumn("local_hour", F.hour(local))


def hourly_cost(readings: DataFrame, grid: DataFrame, tariffs: DataFrame) -> DataFrame:
    """Join each hourly reading to the market price of that hour and the customer's tariff margin."""
    price = grid.select(F.col("ts_utc").alias("reading_ts"), "day_ahead_price_eur_mwh")
    return (readings
            .join(price, "reading_ts", "left")
            .join(F.broadcast(tariffs.select("tariff_plan", "margin_eur_kwh", "fixed_price_eur_kwh")),
                  "tariff_plan", "left")
            .withColumn("energy_cost_eur",
                        F.col("kwh_import") * F.coalesce(
                            F.col("fixed_price_eur_kwh"),
                            F.col("day_ahead_price_eur_mwh") / 1000 + F.col("margin_eur_kwh"))))


def attach_customer_sk(facts: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """As-of lookup: the dimension version valid on the reading date (unknown member = -1)."""
    dim = dim_customer.select(
        "meter_id", "customer_sk", "customer_key", "tariff_plan", "region",
        F.col("valid_from").alias("_vf"), F.col("valid_to").alias("_vt"))
    cond = ((facts["meter_id"] == dim["meter_id"])
            & (facts["reading_date"] >= dim["_vf"])
            & (dim["_vt"].isNull() | (facts["reading_date"] < dim["_vt"])))
    return (facts.join(dim, cond, "left")
            .drop(dim["meter_id"]).drop("_vf", "_vt")
            .withColumn("customer_sk", F.coalesce("customer_sk", F.lit(UNKNOWN_SK))))


def daily_consumption(hourly: DataFrame, extra_keys: tuple = ()) -> DataFrame:
    """Grain: one row per meter per local day, with completeness against the DST-aware hour count.

    `extra_keys` carries attributes that are constant within a meter-day (e.g. customer_sk).
    """
    return (hourly.groupBy("meter_id", "reading_date", *extra_keys)
            .agg(F.round(F.sum("kwh_import"), 4).alias("kwh_import"),
                 F.round(F.sum("kwh_export"), 4).alias("kwh_export"),
                 F.round(F.max("kwh_import"), 4).alias("peak_hour_kwh"),
                 F.round(F.coalesce(F.sum(F.when(F.col("local_hour").between(17, 21), F.col("kwh_import"))),
                                    F.lit(0.0)), 4).alias("evening_peak_kwh"),
                 F.round(F.sum("energy_cost_eur"), 4).alias("energy_cost_eur"),
                 F.count(F.lit(1)).alias("readings_count"))
            .withColumn("net_kwh", F.round(F.col("kwh_import") - F.col("kwh_export"), 4))
            .withColumn("expected_readings", expected_hours(F.col("reading_date")))
            .withColumn("completeness_pct",
                        F.round(F.least(F.lit(100.0),
                                        100.0 * F.col("readings_count") / F.col("expected_readings")), 2))
            .withColumn("date_key", F.date_format("reading_date", "yyyyMMdd").cast("int")))


def expected_hours(local_date):
    """Number of local hours in a Belgian day: 23 (last Sunday of March), 25 (last Sunday of October)."""
    start_utc = F.to_utc_timestamp(F.to_timestamp(local_date), TZ)
    end_utc = F.to_utc_timestamp(F.to_timestamp(F.date_add(local_date, 1)), TZ)
    return ((F.unix_timestamp(end_utc) - F.unix_timestamp(start_utc)) / 3600).cast("int")


def robust_daily_anomalies(daily: DataFrame, window_days: int = 28, min_history: int = 14,
                           z_threshold: float = 4.0, min_ratio: float = 2.5) -> DataFrame:
    """Flag meter-days whose consumption deviates strongly from that meter's own trailing history.

    A spike needs BOTH a high z-score and a large ratio to the meter's mean: households with an
    occasional EV-charging day have bimodal consumption, where a z-score alone raises false alarms.
    Flatlines (zero consumption on a meter that normally consumes) are a separate rule because a
    z-score is weak on the low side for skewed data.
    """
    day_num = F.datediff(F.col("reading_date"), F.lit("2000-01-01"))
    w = Window.partitionBy("meter_id").orderBy(day_num).rangeBetween(-window_days, -1)
    scored = (daily
              .withColumn("hist_mean", F.avg("kwh_import").over(w))
              .withColumn("hist_std", F.stddev_samp("kwh_import").over(w))
              .withColumn("hist_days", F.count("kwh_import").over(w))
              .withColumn("z_score", F.round((F.col("kwh_import") - F.col("hist_mean"))
                                             / F.greatest(F.col("hist_std"), F.lit(0.05)), 3)))
    kind = (F.when(F.col("hist_days") < min_history, None)
            .when((F.col("z_score") >= z_threshold) & (F.col("kwh_import") >= min_ratio * F.col("hist_mean")), "spike")
            .when((F.col("kwh_import") <= 0.01) & (F.col("hist_mean") > 1.0) & (F.col("completeness_pct") >= 90),
                  "flatline"))
    return scored.withColumn("anomaly_type", kind)
