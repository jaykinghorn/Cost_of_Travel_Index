-- ============================================================================
-- CTI NATIONAL RETAIL: National-Level Cost Calculation
-- ============================================================================
-- Purpose: Calculate median per-transaction retail spending at the national level
--
-- Aggregation: None - individual transactions analyzed
-- Geographic Level: United States (all states combined)
-- Minimum Threshold: Optional minimum amount to filter very small purchases
-- Outlier Removal: P5/P98 nationally per month
-- Date Range: Configurable
--
-- Rationale: National aggregation provides a single U.S. baseline for
--            retail costs. Using individual retail transactions provides
--            a representative sample of typical shopping purchases without
--            over-aggregating daily spending patterns.
--
-- MCC Codes for Retail (38 codes covering various retail categories)
-- Art supplies, cosmetics, books, sporting goods, apparel, jewelry,
-- gift shops, department stores, and specialty retail
-- ============================================================================

-- CONFIGURATION: Update these values as needed
DECLARE start_date DATE DEFAULT '2024-01-01';
DECLARE end_date DATE DEFAULT '2025-10-31';
DECLARE distance_threshold INT64 DEFAULT 60;  -- Miles for visitor classification
DECLARE min_transaction_amount FLOAT64 DEFAULT 0.0;  -- Optional: Filter out very small purchases

-- DATA QUALITY THRESHOLDS (95% CI, 5% MOE)
DECLARE min_sample_exclude INT64 DEFAULT 600;  -- Exclude geography if below this
DECLARE min_sample_rolling INT64 DEFAULT 2000; -- Use 3-month rolling if below this

-- ============================================================================
-- MAIN QUERY
-- ============================================================================

WITH retail_transactions AS (
  -- Extract all retail transactions (no aggregation, no state grouping)
  SELECT
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

-- Calculate P5 and P98 thresholds nationally per month for outlier removal
national_thresholds AS (
  SELECT
    month_date,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(5)] as p5,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(98)] as p98
  FROM retail_transactions
  GROUP BY month_date
),

-- Filter out outliers (transactions below P5 or above P98)
retail_no_outliers AS (
  SELECT
    r.month_date,
    r.trans_amount,
    r.membccid
  FROM retail_transactions r
  JOIN national_thresholds t
    ON r.month_date = t.month_date
  WHERE r.trans_amount >= t.p5
    AND r.trans_amount <= t.p98
)

-- Calculate median per-transaction retail cost with data quality flags
SELECT
  'US' as geography_level,  -- Identifier for national level
  month_date,  -- First day of month for joining
  APPROX_QUANTILES(trans_amount, 100)[OFFSET(50)] as retail_cost,
  COUNT(*) as transaction_count,
  COUNT(DISTINCT membccid) as unique_visitors,

  -- Data quality assessment
  CASE
    WHEN COUNT(DISTINCT membccid) < min_sample_exclude THEN 'EXCLUDE'
    WHEN COUNT(DISTINCT membccid) < min_sample_rolling THEN 'ROLLING_3MO'
    ELSE 'SINGLE_MONTH'
  END as data_quality_flag,

  -- Metadata for reference
  start_date as period_start,
  end_date as period_end,
  CURRENT_TIMESTAMP() as calculation_timestamp
FROM retail_no_outliers
GROUP BY month_date
ORDER BY month_date
