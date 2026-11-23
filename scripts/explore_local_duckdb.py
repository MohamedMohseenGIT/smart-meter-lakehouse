"""Explore the synthetic landing files locally with DuckDB, no Spark or cluster needed.

    pip install -e . duckdb
    python scripts/explore_local_duckdb.py

Useful for fast data profiling and for checking generator changes before deploying.
"""

from datetime import date, timedelta

import duckdb

from smartmeter.generator import GeneratorConfig, run

ROOT = "local_landing"
run(ROOT, GeneratorConfig(n_customers=200, backfill_days=14), until=date.today() - timedelta(days=1))

con = duckdb.connect()
con.sql(f"CREATE VIEW readings AS SELECT * FROM read_json_auto('{ROOT}/meter_readings/*/*.json.gz')")
con.sql(f"CREATE VIEW customers AS SELECT * FROM read_csv_auto('{ROOT}/crm_customers/*/*.csv')")

print("Data-quality profile of the raw meter feed:")
con.sql("""
    SELECT count(*)                                              AS rows,
           count(*) - count(DISTINCT (meter_id, reading_ts))     AS duplicate_rows,
           count_if(kwh_import IS NULL OR kwh_import < 0)        AS bad_import_values,
           count_if(CAST(substr(meter_id, 2) AS INT) >= 900000)  AS unknown_meter_rows
    FROM readings
""").show()

print("Average hourly import by local hour (Belgian load curve):")
con.sql("""
    SELECT hour(timezone('Europe/Brussels', CAST(reading_ts AS TIMESTAMPTZ))) AS local_hour,
           round(avg(kwh_import), 3) AS avg_kwh
    FROM readings WHERE kwh_import >= 0 GROUP BY 1 ORDER BY 1
""").show(max_rows=24)
