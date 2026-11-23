-- Datasets for the AI/BI dashboard "Smart Meter Operations" (Dashboards > Create > add each as a dataset).
-- Replace `workspace` with your catalog if different. In dev the schemas are prefixed (dev_<user>_sm_gold).

-- 1. Daily energy by region (line chart: reading_date x kwh_import, series = region)
SELECT reading_date, region, sum(kwh_import) AS kwh_import, sum(kwh_export) AS kwh_export,
       sum(energy_cost_eur) AS energy_cost_eur
FROM workspace.sm_gold.v_daily_region_consumption
GROUP BY ALL;

-- 2. Solar self-sufficiency by tariff (bar chart)
SELECT tariff_plan, round(sum(kwh_export) / sum(kwh_import), 3) AS export_to_import_ratio
FROM workspace.sm_gold.fact_daily_consumption
WHERE customer_sk <> -1
GROUP BY tariff_plan;

-- 3. Data-quality pass rates over time (line chart, filter on status)
SELECT date(checked_at) AS check_date, table_name, rule, min(pass_rate) AS pass_rate, max(status) AS status
FROM workspace.sm_ops.dq_results
GROUP BY ALL;

-- 4. Freshness SLO attainment (counter + table)
SELECT table_name, count_if(status = 'OK') / count(*) AS slo_attainment, max(lag_hours) AS worst_lag_hours
FROM workspace.sm_ops.freshness_checks
GROUP BY table_name;

-- 5. Churn risk distribution (pie chart)
SELECT risk_band, count(*) AS customers
FROM workspace.sm_gold.churn_scores
WHERE cutoff_date = (SELECT max(cutoff_date) FROM workspace.sm_gold.churn_scores)
GROUP BY risk_band;

-- 6. Meter anomalies (table)
SELECT reading_date, meter_id, anomaly_type, kwh_import, hist_mean, z_score
FROM workspace.sm_gold.meter_anomalies
ORDER BY reading_date DESC;

-- 7. Market price vs renewable share (combo chart)
SELECT market_local_date, avg_price_eur_mwh, renewable_share
FROM workspace.sm_gold.grid_daily
ORDER BY market_local_date;
