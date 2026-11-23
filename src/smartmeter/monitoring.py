"""Operational checks that do not need Spark: freshness SLOs and feature drift (PSI)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import numpy as np

OK, BREACH, NO_DATA = "OK", "BREACH", "NO_DATA"


@dataclass(frozen=True)
class FreshnessSLO:
    table: str
    timestamp_column: str
    max_lag_hours: float
    description: str = ""


def evaluate_freshness(slo: FreshnessSLO, latest: datetime | None, now: datetime) -> dict:
    if latest is None:
        return {"table_name": slo.table, "latest_ts": None, "lag_hours": None,
                "max_lag_hours": slo.max_lag_hours, "status": NO_DATA}
    lag = round((now - latest).total_seconds() / 3600, 2)
    return {"table_name": slo.table, "latest_ts": latest, "lag_hours": lag,
            "max_lag_hours": slo.max_lag_hours, "status": OK if lag <= slo.max_lag_hours else BREACH}


def psi(reference, current, bins: int = 10, eps: float = 1e-4) -> float:
    """Population Stability Index. Rule of thumb: <0.1 stable, 0.1-0.2 moderate, >0.2 significant drift."""
    ref = np.asarray(reference, dtype=float)
    cur = np.asarray(current, dtype=float)
    ref, cur = ref[~np.isnan(ref)], cur[~np.isnan(cur)]
    if len(ref) == 0 or len(cur) == 0:
        return float("nan")
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, bins + 1)))
    if len(edges) < 2:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    ref_pct = np.histogram(ref, edges)[0] / len(ref)
    cur_pct = np.histogram(cur, edges)[0] / len(cur)
    ref_pct, cur_pct = np.clip(ref_pct, eps, None), np.clip(cur_pct, eps, None)
    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


def drift_status(value: float) -> str:
    if np.isnan(value):
        return NO_DATA
    return "STABLE" if value < 0.1 else ("MODERATE" if value < 0.2 else "DRIFT")
