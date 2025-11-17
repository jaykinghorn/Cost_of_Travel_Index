-- ============================================================================
-- RETAIL: State-Level Cost Calculation
-- ============================================================================
-- Purpose: Calculate median per-transaction retail spending at the state level
--
-- Aggregation: None - individual transactions analyzed
-- Minimum Threshold: Optional minimum amount to filter very small purchases
-- Outlier Removal: P5/P98 per state
-- Date Range: Configurable (default 2-week test period)
--
-- Rationale: Using individual retail transactions provides a representative
--            sample of typical shopping purchases without over-aggregating
--            daily spending patterns.
-- ============================================================================

-- CONFIGURATION: Update these values as needed
DECLARE start_date DATE DEFAULT '2025-07-01';
DECLARE end_date DATE DEFAULT '2025-07-31';
DECLARE distance_threshold INT64 DEFAULT 60;  -- Miles for visitor classification
DECLARE min_transaction_amount FLOAT64 DEFAULT 0.0;  -- Optional: Filter out very small purchases

-- DATA QUALITY THRESHOLDS (95% CI, 5% MOE)
DECLARE min_sample_exclude INT64 DEFAULT 600;  -- Exclude geography if below this
DECLARE min_sample_rolling INT64 DEFAULT 2000; -- Use 3-month rolling if below this

-- MCC Codes for Retail (38 codes covering various retail categories)
-- Art supplies, cosmetics, books, sporting goods, apparel, jewelry,
-- gift shops, department stores, and specialty retail

-- ============================================================================
-- MAIN QUERY
-- ============================================================================

WITH retail_transactions AS (
  -- Extract all retail transactions (no aggregation)
  SELECT
    m.merch_state,
    DATE_TRUNC(t.trans_date, MONTH) as month_date,
    t.trans_amount,
    t.membccid
  FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
  JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
    ON t.mtid = m.mtid
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

-- Calculate P5 and P98 thresholds per state per month for outlier removal
state_thresholds AS (
  SELECT
    merch_state,
    month_date,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(5)] as p5,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(98)] as p98
  FROM retail_transactions
  GROUP BY merch_state, month_date
),

-- Filter out outliers (transactions below P5 or above P98)
retail_no_outliers AS (
  SELECT
    r.merch_state,
    r.month_date,
    r.trans_amount,
    r.membccid
  FROM retail_transactions r
  JOIN state_thresholds t
    ON r.merch_state = t.merch_state
    AND r.month_date = t.month_date
  WHERE r.trans_amount >= t.p5
    AND r.trans_amount <= t.p98
)

-- Calculate median per-transaction retail cost with data quality flags
SELECT
  merch_state,
  month_date,  -- First day of month for joining
  APPROX_QUANTILES(trans_amount, 100)[OFFSET(50)] as retail_cost,
  COUNT(*) as transaction_count,
  COUNT(DISTINCT membccid) as unique_visitors,

  -- Data quality assessment
  CASE
    WHEN COUNT(*) < min_sample_exclude THEN 'EXCLUDE'
    WHEN COUNT(*) < min_sample_rolling THEN 'ROLLING_3MO'
    ELSE 'SINGLE_MONTH'
  END as data_quality_flag,

  -- Metadata for reference
  MIN(start_date) as period_start,
  MAX(end_date) as period_end,
  CURRENT_TIMESTAMP() as calculation_timestamp
FROM retail_no_outliers
GROUP BY merch_state, month_date
ORDER BY month_date, transaction_count DESC
