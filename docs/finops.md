# Cost and performance choices (FinOps)

| Choice | Effect |
|---|---|
| Triggered pipeline (not continuous) | compute only runs a few minutes per day |
| Serverless jobs and pipeline | no idle clusters, no cluster sizing to maintain |
| Incremental ingestion (Auto Loader) | each file read once; cost grows with new data, not history |
| Incremental gold (watermark + date-scoped MERGE) | only touched dates are recomputed |
| One set-based SCD2 MERGE | a 120-day backfill is 1 MERGE instead of 120 |
| Liquid clustering on query keys (`reading_date`, `meter_id`, `customer_key`) | data skipping without manual Z-ORDER tuning |
| Broadcast joins for small dimensions (tariffs, affected dates) | avoids shuffles |
| Weekly OPTIMIZE / VACUUM 7 days / ANALYZE | fewer small files, lower storage, better plans |
| gzip-compressed landing files | smaller raw volume |
| Schedules paused by default, scale-to-zero serving | no surprise spend in shared or free workspaces |

**Measure, don't guess:** `05_maintenance_finops` records file counts and size before and after
maintenance. The system tables `system.billing.usage` and `system.lakeflow.job_run_timeline` (where
available) give cost and runtime per job for a FinOps dashboard.
