-- ============================================================================
-- CTI NATIONAL RESTAURANTS: National-Level Cost Calculation
-- ============================================================================
-- Purpose: Calculate P35 (breakfast/lunch) and P65 (dinner) percentiles for
--          full-service restaurant spending at the national level
--
-- Aggregation: None - individual transactions analyzed
-- Geographic Level: United States (all states combined)
-- Outlier Removal: P5/P98 nationally per month
-- Date Range: Configurable
--
-- Rationale: National aggregation provides a single U.S. baseline for
--            restaurant costs, calculated from all visitor transactions
--            across the country with month-specific outlier thresholds.
--
-- MCC Code: 5812 (Eating Places, Restaurants)
-- ============================================================================

-- CONFIGURATION: Update these values as needed
DECLARE start_date DATE DEFAULT '2024-01-01';
DECLARE end_date DATE DEFAULT '2025-10-31';
DECLARE distance_threshold INT64 DEFAULT 60;  -- Miles for visitor classification

-- DATA QUALITY THRESHOLDS (95% CI, 5% MOE)
DECLARE min_sample_exclude INT64 DEFAULT 600;  -- Exclude geography if below this
DECLARE min_sample_rolling INT64 DEFAULT 2000; -- Use 3-month rolling if below this

-- ============================================================================
-- MAIN QUERY
-- ============================================================================

WITH restaurant_transactions AS (
  -- Extract all restaurant transactions for the period (no state grouping)
  SELECT
    DATE_TRUNC(t.trans_date, MONTH) as month_date,
    t.trans_amount,
    t.membccid
  FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
  JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
    ON t.mtid = m.mtid and t.ref_date = m.ref_date
  WHERE t.trans_date BETWEEN start_date AND end_date
    AND t.trans_distance > distance_threshold
    AND m.merch_type = 0  -- Physical locations only
    AND m.merch_country = 'US'
    AND m.mcc = '5812'  -- Full-service restaurants
),

-- Calculate P5 and P98 thresholds nationally per month for outlier removal
national_thresholds AS (
  SELECT
    month_date,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(5)] as p5,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(98)] as p98
  FROM restaurant_transactions
  GROUP BY month_date
),

-- Filter out outliers (transactions below P5 or above P98)
restaurants_no_outliers AS (
  SELECT
    r.month_date,
    r.trans_amount,
    r.membccid
  FROM restaurant_transactions r
  JOIN national_thresholds t
    ON r.month_date = t.month_date
  WHERE r.trans_amount >= t.p5
    AND r.trans_amount <= t.p98
)

-- Calculate final percentiles with data quality flags
SELECT
  'US' as geography_level,  -- Identifier for national level
  month_date,  -- First day of month for joining
  APPROX_QUANTILES(trans_amount, 100)[OFFSET(35)] as breakfast_lunch_cost,
  APPROX_QUANTILES(trans_amount, 100)[OFFSET(65)] as dinner_cost,
  APPROX_QUANTILES(trans_amount, 100)[OFFSET(50)] as median_meal_cost,
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
FROM restaurants_no_outliers
GROUP BY month_date
ORDER BY month_date
