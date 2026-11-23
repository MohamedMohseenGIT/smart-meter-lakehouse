# Operations runbook

The daily job alerts the owner by e-mail on any task failure and when a run exceeds 1 hour.

## Data-quality gate failed (`data_quality_gate`)

1. Open `sm_ops.dq_results` and `sm_ops.reconciliation_results` for the run id in the error message.
2. **control_totals_vs_bronze mismatch** → files were partially loaded or re-delivered. Compare
   `_audit/delivery_date=*/control_totals.json` with bronze counts per `delivery_date`. If a file was
   replaced upstream, land it under a new name; Auto Loader does not re-read a file it already processed.
3. **silver_vs_gold_kwh mismatch** → the fact missed dates. Reset the watermark to before the affected
   ingestion and re-run `dimensional_model` (the MERGE is idempotent):
   `UPDATE sm_ops.watermarks SET value = '<timestamp>' WHERE name = 'fact_daily_consumption'`.
4. **High quarantine rate** → inspect `sm_silver.meter_readings_quarantine.failed_rules`, contact the
   head-end vendor, and replay corrected rows by landing a correction file.

## Missing upstream data / freshness SLO breach (`freshness_slo`)

1. Check whether files landed in the raw volume for the expected `delivery_date`.
2. No file → upstream incident: inform consumers that day D is incomplete (completeness is visible
   per meter-day in `fact_daily_consumption.completeness_pct`). Late rows are absorbed automatically
   when they arrive, because the fact recomputes every date touched by new rows.
3. File present but not ingested → check the pipeline event log for the `medallion_pipeline` task.

## Failed workflow recovery

* Use **Repair run** on the failed job run: only failed and downstream tasks re-execute.
* Every task is re-runnable: Auto Loader checkpoints, watermark-after-MERGE and `replaceWhere` writes
  make re-execution produce the same result.
* Transient failures (`generate_sources`, `dimensional_model`) retry automatically (2 retries).

## Model issues

* Drift `DRIFT` in `sm_ml.drift_metrics` → run `smart-meter-ml-weekly` now.
* Bad model in production → move the `@champion` alias back to the previous version in Unity Catalog;
  the next scoring run uses it, with no deployment needed.
