import json
from datetime import date

import pandas as pd

from smartmeter.generator import (
    GeneratorConfig,
    generate_grid,
    generate_readings,
    init_customers,
    inject_defects,
    local_hours,
    run,
)

DAY = date(2026, 6, 15)


def test_local_hours_follow_belgian_dst():
    assert len(local_hours(date(2026, 3, 29))) == 23   # spring forward
    assert len(local_hours(date(2026, 10, 25))) == 25  # fall back
    assert len(local_hours(DAY)) == 24


def test_customers_have_unique_keys_and_valid_domains():
    c = init_customers(GeneratorConfig(n_customers=300), DAY)
    assert c["customer_id"].is_unique and c["meter_id"].is_unique
    assert set(c["region"]) <= {"Flanders", "Wallonia", "Brussels"}
    assert set(c["tariff_plan"]) <= {"variable", "fixed", "dynamic"}


def test_readings_are_physical_and_deterministic():
    cfg = GeneratorConfig(n_customers=50)
    c = init_customers(cfg, DAY)
    r1, _ = generate_readings(c, DAY, cfg)
    r2, _ = generate_readings(c, DAY, cfg)
    assert len(r1) == 50 * 24
    assert (r1["kwh_import"] >= 0).all() and (r1["kwh_export"] >= 0).all()
    pd.testing.assert_frame_equal(r1, r2)


def test_solar_customers_export_at_midday_only():
    cfg = GeneratorConfig(n_customers=200)
    c = init_customers(cfg, DAY)
    r, _ = generate_readings(c, DAY, cfg)
    solar_meters = set(c.loc[c["has_solar"], "meter_id"])
    exports = r[r["kwh_export"] > 0]
    assert set(exports["meter_id"]) <= solar_meters
    assert r[~r["meter_id"].isin(solar_meters)]["kwh_export"].sum() == 0


def test_defects_are_injected_and_counted():
    cfg = GeneratorConfig(n_customers=200, duplicate_rate=0.02, late_rate=0.02, bad_value_rate=0.01,
                          unknown_meter_rate=0.01)
    c = init_customers(cfg, DAY)
    clean, _ = generate_readings(c, DAY, cfg)
    file_rows, late, counts = inject_defects(clean, DAY, cfg)
    assert counts["duplicate_rows"] > 0 and counts["late_rows_held_back"] == len(late) > 0
    assert file_rows.duplicated(["meter_id", "reading_ts"]).sum() >= counts["duplicate_rows"]
    assert (file_rows["meter_id"].str[1:].astype(int) >= 900000).sum() == counts["unknown_meter_rows"]


def test_grid_feed_has_one_row_per_local_hour():
    g = generate_grid(date(2026, 10, 25), GeneratorConfig())
    assert len(g) == 25 and g["ts_utc"].is_unique


def test_run_is_incremental_and_control_totals_match(tmp_path):
    root = str(tmp_path)
    cfg = GeneratorConfig(n_customers=40, backfill_days=3)
    first = run(root, cfg, until=date(2026, 6, 3))
    assert first["generated_days"] == 3 and first["first_day"] == "2026-06-01"

    again = run(root, cfg, until=date(2026, 6, 3))
    assert again["generated_days"] == 0  # idempotent re-run

    more = run(root, cfg, until=date(2026, 6, 5))
    assert (more["first_day"], more["last_day"]) == ("2026-06-04", "2026-06-05")

    for d in ("2026-06-01", "2026-06-05"):
        rows = pd.read_json(f"{root}/meter_readings/delivery_date={d}/readings.json.gz", lines=True)
        with open(f"{root}/_audit/delivery_date={d}/control_totals.json") as fh:
            assert json.load(fh)["meter_readings_rows"] == len(rows)
        snap = pd.read_csv(f"{root}/crm_customers/snapshot_date={d}/customers.csv")
        assert "price_sensitivity" not in snap.columns  # latent drivers never leak into the CRM feed
