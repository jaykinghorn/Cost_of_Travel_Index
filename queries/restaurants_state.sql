-- ============================================================================
-- RESTAURANTS: State-Level Cost Calculation
-- ============================================================================
-- Purpose: Calculate P35 (breakfast/lunch) and P65 (dinner) percentiles for
--          full-service restaurant spending at the state level
--
-- Aggregation: None - individual transactions analyzed
-- Outlier Removal: P5/P98 per state
-- Date Range: Configurable (default 2-week test period)
-- ============================================================================

-- CONFIGURATION: Update these values as needed
DECLARE start_date DATE DEFAULT '2025-07-01';
DECLARE end_date DATE DEFAULT '2025-07-14';
DECLARE distance_threshold INT64 DEFAULT 60;  -- Miles for visitor classification

-- ============================================================================
-- MAIN QUERY
-- ============================================================================

WITH restaurant_transactions AS (
  -- Extract all restaurant transactions for the period
  SELECT
    m.merch_state,
    t.trans_amount,
    t.membccid
  FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
  JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
    ON t.mtid = m.mtid
  WHERE t.trans_date BETWEEN start_date AND end_date
    AND t.trans_distance > distance_threshold
    AND m.merch_type = 0  -- Physical locations only
    AND m.merch_country = 'US'
    AND m.mcc = '5812'  -- Full-service restaurants
),

-- Calculate P5 and P98 thresholds per state for outlier removal
state_thresholds AS (
  SELECT
    merch_state,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(5)] as p5,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(98)] as p98
  FROM restaurant_transactions
  GROUP BY merch_state
),

-- Filter out outliers (transactions below P5 or above P98)
restaurants_no_outliers AS (
  SELECT
    r.merch_state,
    r.trans_amount,
    r.membccid
  FROM restaurant_transactions r
  JOIN state_thresholds t
    ON r.merch_state = t.merch_state
  WHERE r.trans_amount >= t.p5
    AND r.trans_amount <= t.p98
)

-- Calculate final percentiles after outlier removal
SELECT
  merch_state,
  APPROX_QUANTILES(trans_amount, 100)[OFFSET(35)] as breakfast_lunch_cost,
  APPROX_QUANTILES(trans_amount, 100)[OFFSET(65)] as dinner_cost,
  APPROX_QUANTILES(trans_amount, 100)[OFFSET(50)] as median_meal_cost,
  COUNT(*) as transaction_count,
  COUNT(DISTINCT membccid) as unique_visitors
FROM restaurants_no_outliers
GROUP BY merch_state
ORDER BY transaction_count DESC
