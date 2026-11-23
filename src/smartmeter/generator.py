"""Synthetic upstream systems for a Belgian energy supplier.

Databricks Free Edition restricts outbound internet, so instead of calling external APIs the platform
simulates three upstream systems that land files in a Unity Catalog volume, exactly like vendors
dropping files in S3/ADLS:

* CRM               -> daily customer snapshot (CSV, contains PII)
* Meter head-end    -> hourly smart-meter telemetry (gzipped JSON lines)
* Grid / market     -> hourly day-ahead price and grid mix (CSV)

The generator deliberately injects real-world defects (duplicates, late arrivals, negative or
missing values, unknown meters, gaps, consumption spikes) and writes *control totals* per file so
downstream reconciliation can prove nothing was lost. It is deterministic per (seed, day), so every
run is reproducible and incremental: it continues from the last generated day.

Pure pandas/numpy: it runs locally in tests and on Databricks serverless without Spark.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd

TZ = "Europe/Brussels"

REGIONS = {
    # region: (share, [(postal_code, city), ...])
    "Flanders": (0.58, [("2000", "Antwerpen"), ("3000", "Leuven"), ("9000", "Gent"), ("8000", "Brugge"),
                        ("3500", "Hasselt"), ("2800", "Mechelen")]),
    "Wallonia": (0.31, [("4000", "Liege"), ("5000", "Namur"), ("6000", "Charleroi"), ("1300", "Wavre")]),
    "Brussels": (0.11, [("1000", "Brussel"), ("1050", "Elsene"), ("1180", "Ukkel")]),
}
TARIFF_PLANS = {"variable": 0.45, "fixed": 0.35, "dynamic": 0.20}
TARIFF_CHURN_MULT = {"variable": 1.6, "fixed": 0.6, "dynamic": 1.0}
FIRST_NAMES = ["Lucas", "Emma", "Noah", "Olivia", "Arthur", "Louise", "Liam", "Mila", "Jules", "Elena",
               "Victor", "Marie", "Adam", "Nora", "Finn", "Lina", "Mohamed", "Sara", "Tibo", "Ella"]
LAST_NAMES = ["Peeters", "Janssens", "Maes", "Jacobs", "Mertens", "Willems", "Claes", "Goossens",
              "Wouters", "De Smet", "Dubois", "Lambert", "Dupont", "Martin", "Simon", "Laurent"]
STREETS = ["Kerkstraat", "Stationsstraat", "Dorpstraat", "Molenstraat", "Rue de la Station",
           "Chaussee de Bruxelles", "Avenue Louise", "Nieuwstraat", "Schoolstraat", "Rue Haute"]

# Typical Belgian residential load shape by local hour (relative units).
LOAD_PROFILE = np.array([0.55, 0.48, 0.45, 0.44, 0.45, 0.55, 0.80, 1.05, 0.95, 0.80, 0.75, 0.78,
                         0.85, 0.80, 0.75, 0.80, 0.95, 1.25, 1.55, 1.60, 1.45, 1.20, 0.95, 0.70])

SOURCES = ("meter_readings", "crm_customers", "grid_market")


@dataclass
class GeneratorConfig:
    n_customers: int = 1000
    seed: int = 42
    backfill_days: int = 120
    max_days_per_run: int = 150
    duplicate_rate: float = 0.004
    late_rate: float = 0.01
    bad_value_rate: float = 0.002
    unknown_meter_rate: float = 0.001
    missing_meter_day_rate: float = 0.004
    anomaly_meter_day_rate: float = 0.0015
    tariff_switch_rate: float = 0.002
    move_rate: float = 0.0005
    new_customer_rate: float = 0.0012
    base_churn_hazard: float = 0.0006


# --------------------------------------------------------------------------------------------
# Customers (CRM)
# --------------------------------------------------------------------------------------------
def _day_rng(cfg: GeneratorConfig, day: date, salt: int = 0) -> np.random.Generator:
    return np.random.default_rng([cfg.seed, day.toordinal(), salt])


def _new_customers(n: int, start_seq: int, day: date, rng: np.random.Generator, initial: bool) -> pd.DataFrame:
    seq = np.arange(start_seq, start_seq + n)
    regions = rng.choice(list(REGIONS), size=n, p=[v[0] for v in REGIONS.values()])
    postal, city = [], []
    for r in regions:
        pc, c = REGIONS[r][1][rng.integers(len(REGIONS[r][1]))]
        postal.append(pc)
        city.append(c)
    first = rng.choice(FIRST_NAMES, size=n)
    last = rng.choice(LAST_NAMES, size=n)
    household = rng.integers(1, 6, size=n)
    base_load = np.round(0.18 + 0.07 * household + rng.gamma(2.0, 0.05, size=n), 3)  # kWh per hour
    load_z = (base_load - base_load.mean()) / (base_load.std() + 1e-9) if n > 1 else np.zeros(n)
    tenure_days = rng.integers(30, 3000, size=n) if initial else np.zeros(n, dtype=int)
    has_solar = rng.random(n) < 0.35
    return pd.DataFrame({
        "customer_id": [f"C{s:06d}" for s in seq],
        "meter_id": [f"M{s:06d}" for s in seq],
        "full_name": [f"{a} {b}" for a, b in zip(first, last)],
        "email": [f"{a.lower()}.{b.lower().replace(' ', '')}{s}@example.com" for a, b, s in zip(first, last, seq)],
        "phone": [f"+32 4{rng.integers(70, 99)} {rng.integers(10, 99)} {rng.integers(10, 99)} {rng.integers(10, 99)}"
                  for _ in range(n)],
        "street": [f"{rng.choice(STREETS)} {rng.integers(1, 200)}" for _ in range(n)],
        "postal_code": postal,
        "city": city,
        "region": regions,
        "tariff_plan": rng.choice(list(TARIFF_PLANS), size=n, p=list(TARIFF_PLANS.values())),
        "has_solar": has_solar,
        "solar_kwp": np.where(has_solar, np.round(rng.uniform(2.5, 8.0, size=n), 1), 0.0),
        "has_ev": rng.random(n) < 0.15,
        "household_size": household,
        "contract_start": [(day - timedelta(days=int(t))).isoformat() for t in tenure_days],
        "status": "active",
        "churn_date": None,
        # latent (never exported): drives behaviour so ML has real signal to learn
        "base_load_kwh": base_load,
        "price_sensitivity": np.clip(0.35 + 0.18 * load_z + rng.normal(0, 0.15, size=n), 0, 1),
    })


LATENT_COLUMNS = ["base_load_kwh", "price_sensitivity", "solar_kwp"]


def init_customers(cfg: GeneratorConfig, day: date) -> pd.DataFrame:
    return _new_customers(cfg.n_customers, 1, day, _day_rng(cfg, day, salt=1), initial=True)


def evolve_customers(customers: pd.DataFrame, day: date, cfg: GeneratorConfig) -> pd.DataFrame:
    """Apply one day of CRM changes: churn, tariff switches, moves and new sign-ups."""
    rng = _day_rng(cfg, day, salt=2)
    c = customers.copy()
    active = c["status"].eq("active").to_numpy()
    tenure = (pd.Timestamp(day) - pd.to_datetime(c["contract_start"])).dt.days.to_numpy()

    hazard = (cfg.base_churn_hazard
              * c["tariff_plan"].map(TARIFF_CHURN_MULT).to_numpy()
              * (1 + 3 * c["price_sensitivity"].to_numpy())
              * np.where(c["has_solar"].to_numpy(), 0.5, 1.0)
              * np.where(tenure < 365, 1.8, 1.0))
    churn = active & (rng.random(len(c)) < hazard)
    c.loc[churn, "status"] = "churned"
    c.loc[churn, "churn_date"] = day.isoformat()

    still_active = c["status"].eq("active").to_numpy()
    switch = still_active & (rng.random(len(c)) < cfg.tariff_switch_rate)
    c.loc[switch, "tariff_plan"] = rng.choice(list(TARIFF_PLANS), size=int(switch.sum()))
    move = still_active & (rng.random(len(c)) < cfg.move_rate)
    c.loc[move, "street"] = [f"{rng.choice(STREETS)} {rng.integers(1, 200)}" for _ in range(int(move.sum()))]

    n_new = rng.binomial(int(still_active.sum()), cfg.new_customer_rate)
    if n_new:
        next_seq = int(c["customer_id"].str[1:].astype(int).max()) + 1
        c = pd.concat([c, _new_customers(n_new, next_seq, day, rng, initial=False)], ignore_index=True)
    return c


def crm_snapshot(customers: pd.DataFrame, day: date) -> pd.DataFrame:
    snap = customers.drop(columns=[x for x in LATENT_COLUMNS if x != "solar_kwp"]).copy()
    snap.insert(0, "snapshot_date", day.isoformat())
    return snap


# --------------------------------------------------------------------------------------------
# Meter telemetry (head-end system)
# --------------------------------------------------------------------------------------------
def local_hours(day: date) -> pd.DatetimeIndex:
    """Local hours of a Belgian day: 23 on the spring DST switch, 25 in autumn."""
    start = pd.Timestamp(day).tz_localize(TZ)
    end = pd.Timestamp(day + timedelta(days=1)).tz_localize(TZ)
    return pd.date_range(start, end, freq="h", inclusive="left")


def generate_readings(customers: pd.DataFrame, day: date, cfg: GeneratorConfig):
    """Return (clean_readings, anomaly_truth) for all customers active on `day`."""
    rng = _day_rng(cfg, day, salt=3)
    act = customers[customers["status"].eq("active") | customers["churn_date"].eq(day.isoformat())]
    act = act[pd.to_datetime(act["contract_start"]) <= pd.Timestamp(day)].reset_index(drop=True)
    hours = local_hours(day)
    n, h = len(act), len(hours)
    doy = day.timetuple().tm_yday
    season_load = 1 + 0.30 * np.cos(2 * np.pi * (doy - 15) / 365)
    season_solar = 0.55 + 0.45 * np.cos(2 * np.pi * (doy - 172) / 365)
    hour_of_day = hours.hour.to_numpy()

    noise = rng.lognormal(0, 0.15, size=(n, h))
    load = act["base_load_kwh"].to_numpy()[:, None] * LOAD_PROFILE[hour_of_day][None, :] * season_load * noise
    ev_today = act["has_ev"].to_numpy() & (rng.random(n) < 0.45)
    ev_profile = np.isin(hour_of_day, [19, 20, 21, 22]).astype(float) * 2.4
    load += ev_today[:, None] * ev_profile[None, :]

    sun = np.clip(np.sin(np.pi * (hour_of_day - 6) / 14), 0, None)
    weather = rng.uniform(0.15, 1.0)  # one weather factor per day, shared nationally
    solar = act["solar_kwp"].to_numpy()[:, None] * sun[None, :] * season_solar * weather * 0.8
    kwh_import = np.clip(load - 0.6 * solar, 0, None)
    kwh_export = np.clip(solar - 0.4 * load, 0, None)

    truth = []
    anomalous = np.flatnonzero(rng.random(n) < cfg.anomaly_meter_day_rate)
    for i in anomalous:
        kind = "spike" if rng.random() < 0.7 else "flatline"
        kwh_import[i] = kwh_import[i] * 6 if kind == "spike" else 0.0
        truth.append({"meter_id": act.at[i, "meter_id"], "reading_date": day.isoformat(), "anomaly_type": kind})

    ts_utc = hours.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ").to_numpy()
    df = pd.DataFrame({
        "meter_id": np.repeat(act["meter_id"].to_numpy(), h),
        "reading_ts": np.tile(ts_utc, n),
        "kwh_import": np.round(kwh_import.ravel(), 4),
        "kwh_export": np.round(kwh_export.ravel(), 4),
        "voltage_v": np.round(rng.normal(231, 3, size=n * h), 1),
        "firmware_version": np.repeat(rng.choice(["5.2.1", "5.3.0", "6.0.2"], size=n), h),
    })
    return df, pd.DataFrame(truth, columns=["meter_id", "reading_date", "anomaly_type"])


def inject_defects(df: pd.DataFrame, day: date, cfg: GeneratorConfig):
    """Return (file_rows, late_rows, defect_counts). late_rows are delivered with the next file."""
    rng = _day_rng(cfg, day, salt=4)
    df = df.copy()
    counts = {}

    meters = df["meter_id"].drop_duplicates()
    gap_meters = meters.sample(frac=cfg.missing_meter_day_rate, random_state=int(rng.integers(1_000_000_000)))
    counts["missing_meter_days"] = int(len(gap_meters))
    df = df[~df["meter_id"].isin(gap_meters)]

    bad = rng.random(len(df)) < cfg.bad_value_rate
    df["kwh_import"] = df["kwh_import"].astype("float64")
    df.loc[bad, "kwh_import"] = np.where(rng.random(int(bad.sum())) < 0.5, -1.0, np.nan)
    counts["bad_values"] = int(bad.sum())

    unknown = rng.random(len(df)) < cfg.unknown_meter_rate
    df.loc[unknown, "meter_id"] = [f"M{x:06d}" for x in rng.integers(900000, 999999, size=int(unknown.sum()))]
    counts["unknown_meter_rows"] = int(unknown.sum())

    late_mask = rng.random(len(df)) < cfg.late_rate
    late = df[late_mask]
    df = df[~late_mask]
    counts["late_rows_held_back"] = int(len(late))

    dups = df.sample(frac=cfg.duplicate_rate, random_state=int(rng.integers(1_000_000_000)))
    counts["duplicate_rows"] = int(len(dups))
    df = pd.concat([df, dups], ignore_index=True)
    return df, late, counts


# --------------------------------------------------------------------------------------------
# Grid / market feed
# --------------------------------------------------------------------------------------------
def generate_grid(day: date, cfg: GeneratorConfig) -> pd.DataFrame:
    rng = _day_rng(cfg, day, salt=5)
    hours = local_hours(day)
    hod = hours.hour.to_numpy()
    doy = day.timetuple().tm_yday
    solar_peak_mw = 2500 + 3500 * np.cos(2 * np.pi * (doy - 172) / 365) ** 2
    solar_mw = np.clip(np.sin(np.pi * (hod - 6) / 14), 0, None) * solar_peak_mw
    wind_mw = np.clip(rng.normal(1800, 700) + rng.normal(0, 150, size=len(hod)), 50, None)
    load_mw = 8000 + 1500 * LOAD_PROFILE[hod] + 900 * np.cos(2 * np.pi * (doy - 15) / 365)
    residual = load_mw - solar_mw - wind_mw
    price = np.round(np.clip(25 + 0.012 * residual + rng.normal(0, 8, size=len(hod)), -50, None), 2)
    return pd.DataFrame({
        "market_date": day.isoformat(),
        "ts_utc": hours.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "day_ahead_price_eur_mwh": price,
        "total_load_mw": np.round(load_mw, 1),
        "solar_mw": np.round(solar_mw, 1),
        "wind_mw": np.round(wind_mw, 1),
    })


# --------------------------------------------------------------------------------------------
# Orchestration: incremental, idempotent day-by-day landing
# --------------------------------------------------------------------------------------------
def _write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(payload, fh, indent=2, default=str)


def land_day(root: str, customers: pd.DataFrame, late_in: pd.DataFrame, day: date, cfg: GeneratorConfig):
    """Write one day of files for all three sources. Returns (customers, late_out)."""
    customers = evolve_customers(customers, day, cfg)
    d = day.isoformat()

    snap = crm_snapshot(customers, day)
    os.makedirs(f"{root}/crm_customers/snapshot_date={d}", exist_ok=True)
    snap.to_csv(f"{root}/crm_customers/snapshot_date={d}/customers.csv", index=False)

    readings, truth = generate_readings(customers, day, cfg)
    file_rows, late_out, counts = inject_defects(readings, day, cfg)
    file_rows = pd.concat([file_rows, late_in], ignore_index=True)
    os.makedirs(f"{root}/meter_readings/delivery_date={d}", exist_ok=True)
    file_rows.to_json(f"{root}/meter_readings/delivery_date={d}/readings.json.gz", orient="records",
                      lines=True, compression="gzip")

    grid = generate_grid(day, cfg)
    os.makedirs(f"{root}/grid_market/market_date={d}", exist_ok=True)
    grid.to_csv(f"{root}/grid_market/market_date={d}/grid.csv", index=False)

    # Control totals: what the upstream system claims it delivered. Used for reconciliation.
    _write_json(f"{root}/_audit/delivery_date={d}/control_totals.json", {
        "delivery_date": d,
        "meter_readings_rows": int(len(file_rows)),
        "late_rows_from_previous_delivery": int(len(late_in)),
        "crm_customers_rows": int(len(snap)),
        "grid_market_rows": int(len(grid)),
        **counts,
    })
    if len(truth):
        os.makedirs(f"{root}/_truth/anomalies", exist_ok=True)
        truth.to_csv(f"{root}/_truth/anomalies/{d}.csv", index=False)
    return customers, late_out


def run(root: str, cfg: GeneratorConfig | None = None, until: date | None = None) -> dict:
    """Generate all missing days up to `until` (default: yesterday UTC). Safe to re-run."""
    cfg = cfg or GeneratorConfig()
    until = until or (pd.Timestamp.utcnow().date() - timedelta(days=1))
    state_dir = f"{root}/_state"
    manifest_path = f"{state_dir}/manifest.json"

    if os.path.exists(manifest_path):
        with open(manifest_path) as fh:
            manifest = json.load(fh)
        customers = pd.read_parquet(f"{state_dir}/customers.parquet")
        late = pd.read_parquet(f"{state_dir}/late_buffer.parquet")
        start = date.fromisoformat(manifest["last_date"]) + timedelta(days=1)
    else:
        start = until - timedelta(days=cfg.backfill_days - 1)
        customers = init_customers(cfg, start - timedelta(days=1))
        late = pd.DataFrame()

    days = [start + timedelta(days=i) for i in range((until - start).days + 1)][: cfg.max_days_per_run]
    for day in days:
        customers, late = land_day(root, customers, late, day, cfg)

    if days:
        os.makedirs(state_dir, exist_ok=True)
        customers.to_parquet(f"{state_dir}/customers.parquet", index=False)
        late.to_parquet(f"{state_dir}/late_buffer.parquet", index=False)
        _write_json(manifest_path, {"last_date": days[-1].isoformat(), "config": asdict(cfg)})
    return {"generated_days": len(days),
            "first_day": str(days[0]) if days else None,
            "last_day": str(days[-1]) if days else None,
            "active_customers": int(customers["status"].eq("active").sum())}
