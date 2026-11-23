# Genie space: "Smart Meter Analyst" (natural-language questions / text-to-SQL)

**Tables to add:** `sm_gold.v_daily_region_consumption`, `sm_gold.fact_daily_consumption`,
`sm_gold.dim_customer`, `sm_gold.dim_date`, `sm_gold.dim_tariff`, `sm_gold.grid_daily`,
`sm_gold.churn_scores`. Never add `sm_silver.customer_pii`.

**General instructions (paste into the space):**
- Energy is in kWh, prices in euro. `kwh_export` is solar energy injected into the grid.
- Use `dim_customer` with `is_current = true` for current customer attributes.
- Exclude unknown meters with `customer_sk <> -1`.
- Dates are Belgian local dates. Weekends and Belgian public holidays are flagged in `dim_date`.
- A customer is "high risk" when `churn_scores.risk_band = 'high'` for the latest `cutoff_date`.

**Sample questions to save as examples:**
1. Which region had the highest average daily consumption per customer last month?
2. How much solar energy was injected on weekends compared to weekdays?
3. How many high-risk churn customers are on the variable tariff?
4. On which days was the average market price above 120 euro/MWh, and what was the renewable share?
5. Show the trend of energy cost per customer by tariff plan over the last 8 weeks.

Table and column comments are created in `00_setup_governance.py`; Genie uses them to understand the data.
