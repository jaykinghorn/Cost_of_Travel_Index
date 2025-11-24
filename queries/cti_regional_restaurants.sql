-- ============================================================================
-- CTI REGIONAL RESTAURANTS: Census Division-Level Cost Calculation
-- ============================================================================
-- Purpose: Calculate P35/P65 restaurant costs at Census Division level
--
-- Aggregation: None - individual transactions analyzed
-- Geographic Level: 9 Census Divisions (New England, Middle Atlantic, etc.)
-- Outlier Removal: P5/P98 per division per month
-- Date Range: Configurable
--
-- Rationale: Using individual restaurant transactions provides a representative
--            sample of typical meal costs. Division-level aggregation captures
--            regional price differences while maintaining sufficient sample sizes.
--
-- MCC Code: 5812 (Eating Places, Restaurants)
-- ============================================================================

-- CONFIGURATION: Update these values as needed
DECLARE start_date DATE DEFAULT '2022-01-01';
DECLARE end_date DATE DEFAULT '2022-12-31';
DECLARE distance_threshold INT64 DEFAULT 60;  -- Miles for visitor classification

-- DATA QUALITY THRESHOLDS (95% CI, 5% MOE)
DECLARE min_sample_exclude INT64 DEFAULT 600;  -- Exclude geography if below this
DECLARE min_sample_rolling INT64 DEFAULT 2000; -- Use 3-month rolling if below this

-- ============================================================================
-- MAIN QUERY
-- ============================================================================

WITH restaurant_transactions AS (
  -- Extract all restaurant transactions with division mapping
  SELECT
    r.census_region_division,
    DATE_TRUNC(t.trans_date, MONTH) as month_date,
    t.trans_amount,
    t.membccid
  FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
  JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
    ON t.mtid = m.mtid and t.ref_date = m.ref_date
  JOIN `prj-sandbox-i7sk.jk_testing.us_census_region_divisions_bridge_table` r
    ON m.merch_state = r.abbreviation
  WHERE t.trans_date BETWEEN start_date AND end_date
    AND t.trans_distance > distance_threshold
    AND m.merch_type = 0  -- Physical locations only
    AND m.merch_country = 'US'
    AND m.mcc = '5812'  -- Restaurants
),

-- Calculate P5 and P98 thresholds per division per month for outlier removal
division_thresholds AS (
  SELECT
    census_region_division,
    month_date,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(5)] as p5,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(98)] as p98
  FROM restaurant_transactions
  GROUP BY census_region_division, month_date
),

-- Filter out outliers (transactions below P5 or above P98)
restaurants_no_outliers AS (
  SELECT
    r.census_region_division,
    r.month_date,
    r.trans_amount,
    r.membccid
  FROM restaurant_transactions r
  JOIN division_thresholds t
    ON r.census_region_division = t.census_region_division
    AND r.month_date = t.month_date
  WHERE r.trans_amount >= t.p5
    AND r.trans_amount <= t.p98
)

-- Calculate P35/P65 restaurant costs with data quality flags
SELECT
  census_region_division,
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
GROUP BY census_region_division, month_date
ORDER BY month_date, census_region_division
