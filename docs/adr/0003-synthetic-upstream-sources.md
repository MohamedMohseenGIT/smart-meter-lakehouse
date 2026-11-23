# ADR-0003: Simulated upstream systems instead of external APIs

**Status:** accepted

## Context
Databricks Free Edition restricts outbound internet to a limited set of trusted domains, so notebooks
cannot call external APIs such as an open-data portal. The project still needs realistic, evolving,
multi-source data with real-world defects to demonstrate ingestion, quality and reconciliation.

## Decision
A deterministic generator (`smartmeter.generator`) simulates three systems and lands files in a Unity
Catalog volume, exactly as vendors would drop files in S3 or ADLS:

| System | Format | Realism built in |
|---|---|---|
| CRM | daily CSV snapshot | PII, tariff switches, moves, churn, new sign-ups |
| Meter head-end | gzipped JSON lines, hourly | DST days (23/25 h), solar export, EV charging, duplicates, late rows, bad values, unknown meters, gaps, spikes, flatlines |
| Grid/market | hourly CSV | price driven by residual load (load minus solar and wind) |

It also writes **control totals** per delivery (for reconciliation) and the **ground truth** of injected
anomalies (to measure detector precision and recall). Churn is driven by hidden behaviour (price
sensitivity, tariff, tenure, solar), so the ML model has real signal to find.

## Consequences
* The full platform runs on a free workspace with no external dependency, and every run is reproducible.
* The generator is packaged as a **Python wheel** and runs as a `python_wheel_task`.
* Swapping in a real source only changes the landing step; bronze onwards is untouched.
