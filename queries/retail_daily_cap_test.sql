-- Cost of Travel Index: Retail Daily Cap Test Query
-- Branch: feature/retail-daily-cap-test
-- Purpose: Calculate retail spending with 6 transaction per cardholder per day cap
-- to prevent inflated medians from high-frequency shoppers

-- Configuration parameters
DECLARE month_start DATE DEFAULT '2024-07-01';
DECLARE month_end DATE DEFAULT '2024-07-31';
DECLARE distance_threshold FLOAT64 DEFAULT 60.0;  -- miles
DECLARE max_retail_txn_per_cardholder_per_day INT64 DEFAULT 6;
DECLARE outlier_lower_percentile FLOAT64 DEFAULT 0.05;  -- P5
DECLARE outlier_upper_percentile FLOAT64 DEFAULT 0.98;  -- P98

-- Step 1: Extract and rank retail transactions
WITH retail_transactions_raw AS (
  SELECT
    geo.admin1_abbr AS state_abbr,
    geo.admin2_id AS county_fips,
    geo.admin2 AS county_name,
    t.trans_amount,
    t.trans_date,
    t.txid,
    t.membccid,
    -- Rank transactions by cardholder and day, keeping highest amounts
    ROW_NUMBER() OVER (
      PARTITION BY t.membccid, t.trans_date
      ORDER BY t.trans_amount DESC
    ) AS daily_rank
  FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
  JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
    ON t.mtid = m.mtid
  JOIN `data-reference.Geo_Reference.admin_geo_reference` geo
    ON m.merch_city = geo.admin2_id
  WHERE t.trans_date BETWEEN month_start AND month_end
    AND t.trans_distance > distance_threshold
    AND m.merch_type = 0  -- Physical locations only
    AND geo.country = 'United States'
    -- Retail MCC codes (38 codes)
    AND m.mcc IN (
      '5971', '5977', '7230', '5942', '5994', '5611', '5621', '5631',
      '5641', '5651', '5681', '5691', '5699', '5311', '5992', '5993',
      '5309', '5310', '5931', '5943', '5947', '5950', '5995', '5999',
      '5944', '5948', '5661', '5655', '5832', '5932', '5937', '5940',
      '5941', '5945', '5946', '5949', '5970', '5972'
    )
),

-- Step 2: Apply daily transaction cap
retail_capped AS (
  SELECT
    state_abbr,
    county_fips,
    county_name,
    trans_amount,
    trans_date,
    txid,
    membccid
  FROM retail_transactions_raw
  WHERE daily_rank <= max_retail_txn_per_cardholder_per_day
),

-- Step 3: Calculate P5/P98 outlier thresholds per state
retail_outlier_thresholds AS (
  SELECT
    state_abbr,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(5)] AS p5_threshold,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(98)] AS p98_threshold,
    COUNT(*) AS total_txn_before_outlier_removal
  FROM retail_capped
  GROUP BY state_abbr
),

-- Step 4: Remove outliers (transactions outside P5-P98 range)
retail_filtered AS (
  SELECT
    r.state_abbr,
    r.county_fips,
    r.county_name,
    r.trans_amount,
    r.trans_date,
    r.txid,
    r.membccid
  FROM retail_capped r
  JOIN retail_outlier_thresholds t
    ON r.state_abbr = t.state_abbr
  WHERE r.trans_amount >= t.p5_threshold
    AND r.trans_amount <= t.p98_threshold
),

-- Step 5: Calculate statistics per state
state_retail_stats AS (
  SELECT
    state_abbr,
    APPROX_QUANTILES(trans_amount, 2)[OFFSET(1)] AS median_retail_txn,
    COUNT(*) AS txn_count_after_filtering,
    COUNT(DISTINCT membccid) AS unique_cardholders,
    COUNT(DISTINCT CONCAT(membccid, '_', CAST(trans_date AS STRING))) AS unique_cardholder_days
  FROM retail_filtered
  GROUP BY state_abbr
),

-- Step 6: Calculate statistics per county
county_retail_stats AS (
  SELECT
    state_abbr,
    county_fips,
    county_name,
    APPROX_QUANTILES(trans_amount, 2)[OFFSET(1)] AS median_retail_txn,
    COUNT(*) AS txn_count_after_filtering,
    COUNT(DISTINCT membccid) AS unique_cardholders,
    COUNT(DISTINCT CONCAT(membccid, '_', CAST(trans_date AS STRING))) AS unique_cardholder_days
  FROM retail_filtered
  GROUP BY state_abbr, county_fips, county_name
),

-- Step 7: Calculate capping impact metrics
capping_impact AS (
  SELECT
    state_abbr,
    COUNT(*) AS txn_before_cap,
    SUM(CASE WHEN daily_rank <= max_retail_txn_per_cardholder_per_day THEN 1 ELSE 0 END) AS txn_after_cap,
    COUNT(*) - SUM(CASE WHEN daily_rank <= max_retail_txn_per_cardholder_per_day THEN 1 ELSE 0 END) AS txn_capped,
    COUNT(DISTINCT membccid) AS unique_cardholders_total,
    COUNT(DISTINCT CASE WHEN daily_rank > max_retail_txn_per_cardholder_per_day THEN membccid END) AS cardholders_affected_by_cap
  FROM retail_transactions_raw
  GROUP BY state_abbr
),

-- Step 8: Final output combining all metrics
final_output AS (
  SELECT
    s.state_abbr,
    s.median_retail_txn AS state_median_retail,
    s.txn_count_after_filtering AS state_txn_count,
    s.unique_cardholders AS state_unique_cardholders,
    s.unique_cardholder_days AS state_unique_cardholder_days,

    -- County-level metrics
    c.county_fips,
    c.county_name,
    c.median_retail_txn AS county_median_retail,
    c.txn_count_after_filtering AS county_txn_count,
    c.unique_cardholders AS county_unique_cardholders,

    -- Capping impact metrics
    i.txn_before_cap,
    i.txn_after_cap,
    i.txn_capped,
    i.unique_cardholders_total,
    i.cardholders_affected_by_cap,
    ROUND(SAFE_DIVIDE(i.txn_capped, i.txn_before_cap) * 100, 2) AS pct_txn_capped,
    ROUND(SAFE_DIVIDE(i.cardholders_affected_by_cap, i.unique_cardholders_total) * 100, 2) AS pct_cardholders_affected,

    -- Calculate basket cost (2 days of retail spending)
    c.median_retail_txn * 2 AS retail_basket_cost_2day,

    -- Metadata
    month_start AS month_start_date,
    month_end AS month_end_date,
    max_retail_txn_per_cardholder_per_day AS max_daily_cap,
    CURRENT_TIMESTAMP() AS query_run_timestamp

  FROM state_retail_stats s
  LEFT JOIN county_retail_stats c
    ON s.state_abbr = c.state_abbr
  LEFT JOIN capping_impact i
    ON s.state_abbr = i.state_abbr
)

-- Final SELECT with ordering
SELECT
  state_abbr,
  county_fips,
  county_name,

  -- State-level retail metrics
  state_median_retail,
  state_txn_count,
  state_unique_cardholders,
  state_unique_cardholder_days,

  -- County-level retail metrics
  county_median_retail,
  county_txn_count,
  county_unique_cardholders,

  -- Basket costs
  retail_basket_cost_2day,

  -- Capping impact analysis
  txn_before_cap,
  txn_after_cap,
  txn_capped,
  pct_txn_capped,
  unique_cardholders_total,
  cardholders_affected_by_cap,
  pct_cardholders_affected,

  -- Metadata
  month_start_date,
  month_end_date,
  max_daily_cap,
  query_run_timestamp

FROM final_output
ORDER BY state_abbr, county_fips;

-- Summary Statistics Query (run separately for high-level overview)
-- Uncomment to get summary statistics instead of detailed results

/*
WITH summary_stats AS (
  SELECT
    COUNT(DISTINCT state_abbr) AS total_states,
    COUNT(DISTINCT county_fips) AS total_counties,

    -- State-level aggregates
    AVG(state_median_retail) AS avg_state_median_retail,
    MIN(state_median_retail) AS min_state_median_retail,
    MAX(state_median_retail) AS max_state_median_retail,
    APPROX_QUANTILES(state_median_retail, 4)[OFFSET(2)] AS median_of_state_medians,

    -- County-level aggregates
    AVG(county_median_retail) AS avg_county_median_retail,
    MIN(county_median_retail) AS min_county_median_retail,
    MAX(county_median_retail) AS max_county_median_retail,

    -- Capping impact
    AVG(pct_txn_capped) AS avg_pct_txn_capped,
    AVG(pct_cardholders_affected) AS avg_pct_cardholders_affected,
    SUM(txn_capped) AS total_txn_capped_nationwide,
    SUM(txn_after_cap) AS total_txn_retained_nationwide

  FROM final_output
)

SELECT * FROM summary_stats;
*/

-- Top 10 States by Median Retail Spending (run separately)
-- Uncomment to see states with highest retail spending

/*
SELECT DISTINCT
  state_abbr,
  state_median_retail,
  retail_basket_cost_2day,
  state_txn_count,
  pct_txn_capped,
  pct_cardholders_affected
FROM final_output
ORDER BY state_median_retail DESC
LIMIT 10;
*/

-- States Most Affected by Capping (run separately)
-- Uncomment to see which states had the most transactions capped

/*
SELECT DISTINCT
  state_abbr,
  txn_before_cap,
  txn_after_cap,
  txn_capped,
  pct_txn_capped,
  cardholders_affected_by_cap,
  pct_cardholders_affected,
  state_median_retail
FROM final_output
ORDER BY pct_txn_capped DESC
LIMIT 10;
*/
