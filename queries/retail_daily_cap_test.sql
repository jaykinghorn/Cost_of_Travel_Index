-- Cost of Travel Index: Retail Daily Cap Test Query (STATE-LEVEL ONLY)
-- Branch: feature/retail-daily-cap-test
-- Purpose: Calculate retail spending with 6 transaction per cardholder per day cap
-- to prevent inflated medians from high-frequency shoppers
-- Geographic Level: STATE-LEVEL ANALYSIS ONLY (counties in future phase)

-- Configuration parameters
DECLARE month_start DATE DEFAULT '2024-07-01';
DECLARE month_end DATE DEFAULT '2024-07-31';
DECLARE distance_threshold FLOAT64 DEFAULT 60.0;  -- miles
DECLARE max_retail_txn_per_cardholder_per_day INT64 DEFAULT 6;
DECLARE outlier_lower_percentile FLOAT64 DEFAULT 0.05;  -- P5
DECLARE outlier_upper_percentile FLOAT64 DEFAULT 0.98;  -- P98

-- Step 1: Extract and rank retail transactions (STATE-LEVEL)
WITH retail_transactions_raw AS (
  SELECT
    geo.admin1_abbr AS state_abbr,
    geo.admin1 AS state_name,
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
    state_name,
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
    r.state_name,
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

-- Step 5: Calculate state-level statistics
state_retail_stats AS (
  SELECT
    state_abbr,
    state_name,
    APPROX_QUANTILES(trans_amount, 2)[OFFSET(1)] AS median_retail_txn,
    COUNT(*) AS txn_count_after_filtering,
    COUNT(DISTINCT membccid) AS unique_cardholders,
    COUNT(DISTINCT CONCAT(membccid, '_', CAST(trans_date AS STRING))) AS unique_cardholder_days,
    MIN(trans_amount) AS min_txn_amount,
    MAX(trans_amount) AS max_txn_amount,
    AVG(trans_amount) AS avg_txn_amount
  FROM retail_filtered
  GROUP BY state_abbr, state_name
),

-- Step 6: Calculate capping impact metrics
capping_impact AS (
  SELECT
    state_abbr,
    COUNT(*) AS txn_before_cap,
    SUM(CASE WHEN daily_rank <= max_retail_txn_per_cardholder_per_day THEN 1 ELSE 0 END) AS txn_after_cap,
    COUNT(*) - SUM(CASE WHEN daily_rank <= max_retail_txn_per_cardholder_per_day THEN 1 ELSE 0 END) AS txn_capped,
    COUNT(DISTINCT membccid) AS unique_cardholders_total,
    COUNT(DISTINCT CASE WHEN daily_rank > max_retail_txn_per_cardholder_per_day THEN membccid END) AS cardholders_affected_by_cap,
    COUNT(DISTINCT CASE
      WHEN daily_rank > max_retail_txn_per_cardholder_per_day
      THEN CONCAT(membccid, '_', CAST(trans_date AS STRING))
    END) AS cardholder_days_affected
  FROM retail_transactions_raw
  GROUP BY state_abbr
),

-- Step 7: Final output combining all metrics
final_output AS (
  SELECT
    s.state_abbr,
    s.state_name,

    -- Retail spending metrics
    s.median_retail_txn,
    s.median_retail_txn * 2 AS retail_basket_cost_2day,

    -- Transaction statistics
    s.txn_count_after_filtering,
    s.unique_cardholders,
    s.unique_cardholder_days,
    s.min_txn_amount,
    s.max_txn_amount,
    ROUND(s.avg_txn_amount, 2) AS avg_txn_amount,

    -- Capping impact metrics
    i.txn_before_cap,
    i.txn_after_cap,
    i.txn_capped,
    ROUND(SAFE_DIVIDE(i.txn_capped, i.txn_before_cap) * 100, 2) AS pct_txn_capped,
    i.unique_cardholders_total,
    i.cardholders_affected_by_cap,
    ROUND(SAFE_DIVIDE(i.cardholders_affected_by_cap, i.unique_cardholders_total) * 100, 2) AS pct_cardholders_affected,
    i.cardholder_days_affected,

    -- Outlier removal metrics
    t.total_txn_before_outlier_removal,
    t.total_txn_before_outlier_removal - s.txn_count_after_filtering AS txn_removed_as_outliers,
    ROUND(SAFE_DIVIDE(
      t.total_txn_before_outlier_removal - s.txn_count_after_filtering,
      t.total_txn_before_outlier_removal
    ) * 100, 2) AS pct_txn_removed_as_outliers,

    -- Threshold values
    ROUND(t.p5_threshold, 2) AS p5_threshold,
    ROUND(t.p98_threshold, 2) AS p98_threshold,

    -- Metadata
    month_start AS month_start_date,
    month_end AS month_end_date,
    max_retail_txn_per_cardholder_per_day AS max_daily_cap,
    CURRENT_TIMESTAMP() AS query_run_timestamp

  FROM state_retail_stats s
  LEFT JOIN capping_impact i
    ON s.state_abbr = i.state_abbr
  LEFT JOIN retail_outlier_thresholds t
    ON s.state_abbr = t.state_abbr
)

-- Final SELECT with ordering
SELECT
  state_abbr,
  state_name,

  -- PRIMARY METRICS
  median_retail_txn,
  retail_basket_cost_2day,

  -- Transaction counts
  txn_count_after_filtering,
  unique_cardholders,
  unique_cardholder_days,

  -- Transaction amount statistics
  min_txn_amount,
  max_txn_amount,
  avg_txn_amount,

  -- Daily cap impact
  txn_before_cap,
  txn_after_cap,
  txn_capped,
  pct_txn_capped,
  cardholders_affected_by_cap,
  pct_cardholders_affected,
  cardholder_days_affected,

  -- Outlier removal impact
  total_txn_before_outlier_removal,
  txn_removed_as_outliers,
  pct_txn_removed_as_outliers,
  p5_threshold,
  p98_threshold,

  -- Metadata
  month_start_date,
  month_end_date,
  max_daily_cap,
  query_run_timestamp

FROM final_output
ORDER BY state_abbr;


-- =============================================================================
-- BONUS QUERIES (Uncomment to run separately)
-- =============================================================================

-- Summary Statistics Query
-- Provides high-level overview across all states
/*
WITH summary_stats AS (
  SELECT
    COUNT(DISTINCT state_abbr) AS total_states,

    -- Retail spending aggregates
    ROUND(AVG(median_retail_txn), 2) AS avg_state_median_retail,
    ROUND(MIN(median_retail_txn), 2) AS min_state_median_retail,
    ROUND(MAX(median_retail_txn), 2) AS max_state_median_retail,
    ROUND(APPROX_QUANTILES(median_retail_txn, 4)[OFFSET(2)], 2) AS median_of_state_medians,
    ROUND(APPROX_QUANTILES(median_retail_txn, 4)[OFFSET(1)], 2) AS q1_of_state_medians,
    ROUND(APPROX_QUANTILES(median_retail_txn, 4)[OFFSET(3)], 2) AS q3_of_state_medians,

    -- Basket cost aggregates
    ROUND(AVG(retail_basket_cost_2day), 2) AS avg_basket_cost,
    ROUND(MIN(retail_basket_cost_2day), 2) AS min_basket_cost,
    ROUND(MAX(retail_basket_cost_2day), 2) AS max_basket_cost,

    -- Capping impact nationwide
    ROUND(AVG(pct_txn_capped), 2) AS avg_pct_txn_capped,
    ROUND(AVG(pct_cardholders_affected), 2) AS avg_pct_cardholders_affected,
    SUM(txn_capped) AS total_txn_capped_nationwide,
    SUM(txn_after_cap) AS total_txn_retained_nationwide,

    -- Outlier removal nationwide
    ROUND(AVG(pct_txn_removed_as_outliers), 2) AS avg_pct_txn_removed_as_outliers,
    SUM(txn_removed_as_outliers) AS total_txn_removed_as_outliers

  FROM final_output
)

SELECT * FROM summary_stats;
*/


-- Top 10 States by Median Retail Spending
-- Shows which states have highest retail spending
/*
SELECT
  state_abbr,
  state_name,
  median_retail_txn,
  retail_basket_cost_2day,
  txn_count_after_filtering,
  pct_txn_capped,
  pct_cardholders_affected
FROM final_output
ORDER BY median_retail_txn DESC
LIMIT 10;
*/


-- Bottom 10 States by Median Retail Spending
-- Shows which states have lowest retail spending
/*
SELECT
  state_abbr,
  state_name,
  median_retail_txn,
  retail_basket_cost_2day,
  txn_count_after_filtering,
  pct_txn_capped,
  pct_cardholders_affected
FROM final_output
ORDER BY median_retail_txn ASC
LIMIT 10;
*/


-- States Most Affected by Daily Cap
-- Shows which states had the most transactions capped
/*
SELECT
  state_abbr,
  state_name,
  txn_before_cap,
  txn_after_cap,
  txn_capped,
  pct_txn_capped,
  cardholders_affected_by_cap,
  pct_cardholders_affected,
  median_retail_txn,
  retail_basket_cost_2day
FROM final_output
ORDER BY pct_txn_capped DESC
LIMIT 10;
*/


-- States Least Affected by Daily Cap
-- Shows states where daily cap had minimal impact
/*
SELECT
  state_abbr,
  state_name,
  txn_before_cap,
  txn_after_cap,
  txn_capped,
  pct_txn_capped,
  cardholders_affected_by_cap,
  pct_cardholders_affected,
  median_retail_txn
FROM final_output
ORDER BY pct_txn_capped ASC
LIMIT 10;
*/


-- States with Highest Outlier Removal
-- Shows which states had most transactions removed as outliers
/*
SELECT
  state_abbr,
  state_name,
  total_txn_before_outlier_removal,
  txn_removed_as_outliers,
  pct_txn_removed_as_outliers,
  p5_threshold,
  p98_threshold,
  median_retail_txn
FROM final_output
ORDER BY pct_txn_removed_as_outliers DESC
LIMIT 10;
*/


-- Distribution Analysis
-- Shows how states cluster by median retail spending
/*
SELECT
  CASE
    WHEN median_retail_txn < 50 THEN '$0-50'
    WHEN median_retail_txn < 100 THEN '$50-100'
    WHEN median_retail_txn < 150 THEN '$100-150'
    WHEN median_retail_txn < 200 THEN '$150-200'
    WHEN median_retail_txn < 300 THEN '$200-300'
    ELSE '$300+'
  END AS median_retail_bucket,
  COUNT(*) AS num_states,
  ROUND(AVG(median_retail_txn), 2) AS avg_median_in_bucket,
  ROUND(AVG(pct_txn_capped), 2) AS avg_pct_capped_in_bucket
FROM final_output
GROUP BY median_retail_bucket
ORDER BY
  CASE median_retail_bucket
    WHEN '$0-50' THEN 1
    WHEN '$50-100' THEN 2
    WHEN '$100-150' THEN 3
    WHEN '$150-200' THEN 4
    WHEN '$200-300' THEN 5
    WHEN '$300+' THEN 6
  END;
*/
