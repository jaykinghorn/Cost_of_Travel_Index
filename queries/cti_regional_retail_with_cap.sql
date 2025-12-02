-- ============================================================================
-- CTI REGIONAL RETAIL: Census Division-Level Cost Calculation WITH DAILY CAP
-- ============================================================================
-- Purpose: Calculate median per-transaction retail spending at Census Division level
--          with a daily transaction cap per cardholder to prevent inflated medians
--
-- NEW: Daily Transaction Cap - Maximum 10 transactions per cardholder per day
--      Keeps highest-value transactions when capping to preserve legitimate spending
--      Prevents "spree shoppers" from skewing median upward
--
-- Aggregation: None - individual transactions analyzed
-- Geographic Level: 9 Census Divisions (New England, Middle Atlantic, etc.)
-- Minimum Threshold: Optional minimum amount to filter very small purchases
-- Outlier Removal: P5/P98 per division per month (applied AFTER capping)
-- Date Range: Configurable
--
-- Rationale: Using individual retail transactions with daily cap provides a
--            representative sample of typical shopping purchases while preventing
--            high-frequency shoppers from inflating the median. Division-level
--            aggregation captures regional retail pricing.
--
-- MCC Codes for Retail (38 codes covering various retail categories)
-- Art supplies, cosmetics, books, sporting goods, apparel, jewelry,
-- gift shops, department stores, and specialty retail
-- ============================================================================

-- CONFIGURATION: Update these values as needed
DECLARE start_date DATE DEFAULT '2022-01-01';
DECLARE end_date DATE DEFAULT '2022-12-31';
DECLARE distance_threshold INT64 DEFAULT 60;  -- Miles for visitor classification
DECLARE min_transaction_amount FLOAT64 DEFAULT 0.0;  -- Optional: Filter out very small purchases
DECLARE max_retail_txn_per_cardholder_per_day INT64 DEFAULT 10;  -- Daily transaction cap

-- DATA QUALITY THRESHOLDS (95% CI, 5% MOE)
DECLARE min_sample_exclude INT64 DEFAULT 600;  -- Exclude geography if below this
DECLARE min_sample_rolling INT64 DEFAULT 2000; -- Use 3-month rolling if below this

-- ============================================================================
-- MAIN QUERY
-- ============================================================================

WITH retail_transactions_raw AS (
  -- Extract all retail transactions with daily ranking
  SELECT
    r.census_region_division,
    DATE_TRUNC(t.trans_date, MONTH) as month_date,
    t.trans_date,
    t.trans_amount,
    t.membccid,
    t.txid,
    -- Rank transactions by cardholder and day, keeping highest amounts
    ROW_NUMBER() OVER (
      PARTITION BY t.membccid, t.trans_date
      ORDER BY t.trans_amount DESC
    ) AS daily_rank
  FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
  JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
    ON t.mtid = m.mtid
    AND t.ref_date = m.ref_date
  JOIN `prj-sandbox-i7sk.jk_testing.us_census_region_divisions_bridge_table` r
    ON m.merch_state = r.abbreviation
  WHERE t.trans_date BETWEEN start_date AND end_date
    AND t.trans_distance > distance_threshold
    AND m.merch_type = 0  -- Physical locations only
    AND m.merch_country = 'US'
    AND m.mcc IN (
      '5971', '5977', '7230', '5942', '5994', '5611', '5621', '5631',
      '5641', '5651', '5681', '5691', '5699', '5311', '5992', '5993',
      '5309', '5310', '5931', '5943', '5947', '5950', '5995', '5999',
      '5944', '5948', '5661', '5655', '5832', '5932', '5937', '5940',
      '5941', '5945', '5946', '5949', '5970', '5972'
    )
    AND t.trans_amount >= min_transaction_amount  -- Optional minimum filter
),

-- Apply daily transaction cap
retail_transactions_capped AS (
  SELECT
    census_region_division,
    month_date,
    trans_amount,
    membccid,
    txid
  FROM retail_transactions_raw
  WHERE daily_rank <= max_retail_txn_per_cardholder_per_day
),

-- Analyze per-cardholder shopping patterns
cardholder_shopping_patterns AS (
  SELECT
    census_region_division,
    month_date,
    membccid,
    COUNT(DISTINCT trans_date) as shopping_days,
    COUNT(DISTINCT CASE WHEN daily_txn_count > max_retail_txn_per_cardholder_per_day THEN trans_date END) as days_over_cap,
    AVG(daily_txn_count) as avg_daily_txn_count,
    MAX(daily_txn_count) as max_daily_txn_count
  FROM (
    SELECT
      census_region_division,
      month_date,
      membccid,
      trans_date,
      COUNT(*) as daily_txn_count
    FROM retail_transactions_raw
    GROUP BY census_region_division, month_date, membccid, trans_date
  )
  GROUP BY census_region_division, month_date, membccid
),

-- Track capping impact per division with better shopping behavior metrics
capping_impact AS (
  SELECT
    r.census_region_division,
    r.month_date,
    COUNT(*) as txn_before_cap,
    SUM(CASE WHEN r.daily_rank <= max_retail_txn_per_cardholder_per_day THEN 1 ELSE 0 END) as txn_after_cap,
    COUNT(*) - SUM(CASE WHEN r.daily_rank <= max_retail_txn_per_cardholder_per_day THEN 1 ELSE 0 END) as txn_capped,
    COUNT(DISTINCT r.membccid) as unique_cardholders_total,

    -- Cardholders who exceeded cap on at least one day
    COUNT(DISTINCT CASE WHEN r.daily_rank > max_retail_txn_per_cardholder_per_day THEN r.membccid END) as cardholders_affected_by_cap,

    -- Average transactions per cardholder per day (across all cardholders and shopping days)
    ROUND(AVG(p.avg_daily_txn_count), 2) as avg_txn_per_cardholder_per_day,

    -- Cardholders who FREQUENTLY exceed the cap (>50% of their shopping days)
    COUNT(DISTINCT CASE
      WHEN SAFE_DIVIDE(p.days_over_cap, p.shopping_days) > 0.5 THEN p.membccid
    END) as frequent_high_volume_shoppers

  FROM retail_transactions_raw r
  JOIN cardholder_shopping_patterns p
    ON r.census_region_division = p.census_region_division
    AND r.month_date = p.month_date
    AND r.membccid = p.membccid
  GROUP BY r.census_region_division, r.month_date
),

-- Calculate P5 and P98 thresholds per division per month for outlier removal (AFTER capping)
division_thresholds AS (
  SELECT
    census_region_division,
    month_date,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(5)] as p5,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(98)] as p98
  FROM retail_transactions_capped
  GROUP BY census_region_division, month_date
),

-- Filter out outliers (transactions below P5 or above P98)
retail_no_outliers AS (
  SELECT
    r.census_region_division,
    r.month_date,
    r.trans_amount,
    r.membccid
  FROM retail_transactions_capped r
  JOIN division_thresholds t
    ON r.census_region_division = t.census_region_division
    AND r.month_date = t.month_date
  WHERE r.trans_amount >= t.p5
    AND r.trans_amount <= t.p98
)

-- Calculate median per-transaction retail cost with data quality flags and capping metrics
SELECT
  r.census_region_division,
  r.month_date,  -- First day of month for joining
  APPROX_QUANTILES(r.trans_amount, 100)[OFFSET(50)] as retail_cost,
  COUNT(*) as transaction_count_final,
  COUNT(DISTINCT r.membccid) as unique_visitors,

  -- Capping impact metrics
  i.txn_before_cap,
  i.txn_after_cap,
  i.txn_capped,
  ROUND(SAFE_DIVIDE(i.txn_capped, i.txn_before_cap) * 100, 2) as pct_txn_capped,

  -- Cardholder behavior metrics
  i.unique_cardholders_total,
  i.cardholders_affected_by_cap,
  ROUND(SAFE_DIVIDE(i.cardholders_affected_by_cap, i.unique_cardholders_total) * 100, 2) as pct_cardholders_affected_any_day,
  i.avg_txn_per_cardholder_per_day,
  i.frequent_high_volume_shoppers,
  ROUND(SAFE_DIVIDE(i.frequent_high_volume_shoppers, i.unique_cardholders_total) * 100, 2) as pct_frequent_high_volume_shoppers,

  -- Outlier removal metrics
  i.txn_after_cap as txn_before_outlier_removal,
  i.txn_after_cap - COUNT(*) as txn_removed_as_outliers,
  ROUND(SAFE_DIVIDE(i.txn_after_cap - COUNT(*), i.txn_after_cap) * 100, 2) as pct_txn_removed_as_outliers,

  -- Data quality assessment
  CASE
    WHEN COUNT(DISTINCT r.membccid) < min_sample_exclude THEN 'EXCLUDE'
    WHEN COUNT(DISTINCT r.membccid) < min_sample_rolling THEN 'ROLLING_3MO'
    ELSE 'SINGLE_MONTH'
  END as data_quality_flag,

  -- Metadata for reference
  start_date as period_start,
  end_date as period_end,
  max_retail_txn_per_cardholder_per_day as max_daily_cap,
  CURRENT_TIMESTAMP() as calculation_timestamp

FROM retail_no_outliers r
LEFT JOIN capping_impact i
  ON r.census_region_division = i.census_region_division
  AND r.month_date = i.month_date
GROUP BY r.census_region_division, r.month_date, i.txn_before_cap, i.txn_after_cap,
         i.txn_capped, i.cardholders_affected_by_cap, i.unique_cardholders_total,
         i.avg_txn_per_cardholder_per_day, i.frequent_high_volume_shoppers
ORDER BY r.month_date, transaction_count_final DESC;
