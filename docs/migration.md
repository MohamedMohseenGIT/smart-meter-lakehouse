# Migration playbook: AWS Glue → Databricks

This repository includes a migrated job (`legacy/glue_daily_consumption_job.py` → medallion pipeline and
gold MERGE) and the parallel-run notebook `40_migration_parallel_run.py`. The approach follows a real Glue
to Databricks migration.

## 1. Inventory and dependency mapping
* List every Glue job, trigger, crawler and table: inputs, outputs, schedule, owner, SLA, consumers.
* Build the dependency graph (job → table → job) to define migration waves; migrate leaves first.

## 2. Metadata: Glue Data Catalog → Unity Catalog
* Map Glue databases to UC schemas; register existing S3 data as external tables during transition, then
  convert to managed Delta tables.
* Re-create permissions as UC grants; add tags and comments as part of the move.

## 3. Refactor, don't lift-and-shift
* DynamicFrames → DataFrames; job bookmarks → Auto Loader / watermarks; full reloads → incremental MERGE.
* Move business logic into a tested Python package; orchestration into Lakeflow Jobs defined as code.

## 4. Parallel run and reconciliation
* Run old and new on the same input for at least two weeks.
* Reconcile per key and measure (`smartmeter.reconcile`). Every difference must be explained.
* In this repo the legacy port differs for three known reasons, fixed one at a time in notebook 40:
  UTC date instead of local date, no de-duplication, and missing quality rules.

## 5. Cost and performance validation
* Compare runtime and cost per run (Glue DPU-hours vs Databricks DBUs) and data freshness.

## 6. Phased cut-over
* Switch consumers wave by wave; keep the legacy job read-only for a rollback window; then decommission
  the jobs, crawlers and IAM roles.
