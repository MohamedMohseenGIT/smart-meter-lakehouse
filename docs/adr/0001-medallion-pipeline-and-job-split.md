# ADR-0001: Declarative pipeline for bronze/silver, job-managed MERGE for gold

**Status:** accepted

## Context
Raw files arrive daily from three upstream systems, with duplicates, late rows and bad values. Gold needs
an SCD2 customer dimension and an incrementally updated daily fact that must absorb late-arriving data.

## Decision
* **Bronze and silver** run in a Lakeflow Declarative Pipeline: Auto Loader gives exactly-once file
  ingestion with checkpoints, expectations enforce the shared rules, and watermark-based de-duplication
  removes re-delivered rows. Rejected rows go to a quarantine table with the failed rule names.
* **Gold** is built by a job task using explicit Delta `MERGE`:
  * SCD2 from *all new snapshots in one set-based MERGE* (window functions find change points),
  * the fact recomputes only the *dates touched by newly ingested rows*, tracked by an `_ingested_at`
    watermark, and merges on `(meter_id, reading_date)`.

## Consequences
* Ingestion stays declarative and self-maintaining (Lakeflow manages OPTIMIZE/VACUUM for its tables).
* Gold logic is plain PySpark in a package, so it is unit-tested locally and reused by the migration run.
* Re-runs are safe: the watermark only moves after a successful MERGE, and MERGE is idempotent.
* Trade-off: two orchestration styles to understand. Accepted because each one is used where it fits best.

## Alternatives considered
* `APPLY CHANGES` / AUTO CDC for SCD2 inside the pipeline: less code, but the incremental fact with late
  data and the as-of surrogate-key lookup are clearer and testable as a job. Free Edition also allows only
  one active pipeline per type, which favours one focused pipeline.
* Full nightly rebuild of gold (as the legacy Glue job did): simpler but cost grows linearly with history.
