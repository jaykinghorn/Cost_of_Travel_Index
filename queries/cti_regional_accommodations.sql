-- ============================================================================
-- CTI REGIONAL ACCOMMODATIONS: Census Division-Level Cost Calculation
-- ============================================================================
-- Purpose: Calculate median accommodation costs at Census Division level
--
-- Aggregation: None - individual transactions analyzed
-- Geographic Level: 9 Census Divisions (New England, Middle Atlantic, etc.)
-- Minimum Threshold: $50 to filter incidental charges (parking, minibar, etc.)
-- Outlier Removal: P5/P98 per division per month
-- Date Range: Configurable
--
-- Rationale: Using individual accommodation transactions with $50 minimum
--            filters out ancillary charges while capturing the full range of
--            hotel stay costs. Division-level aggregation provides regional
--            pricing trends.
--
-- MCC Codes: 3501-3838 (hotel chains), 7011-7012 (hotels/timeshares)
--            300+ codes covering various hotel chains, resorts, and accommodations
-- ============================================================================

-- CONFIGURATION: Update these values as needed
DECLARE start_date DATE DEFAULT '2022-01-01';
DECLARE end_date DATE DEFAULT '2022-12-31';
DECLARE distance_threshold INT64 DEFAULT 60;  -- Miles for visitor classification
DECLARE min_transaction_amount FLOAT64 DEFAULT 50.0;  -- Minimum to qualify as accommodation booking

-- DATA QUALITY THRESHOLDS (95% CI, 5% MOE)
DECLARE min_sample_exclude INT64 DEFAULT 600;  -- Exclude geography if below this
DECLARE min_sample_rolling INT64 DEFAULT 2000; -- Use 3-month rolling if below this

-- ============================================================================
-- MAIN QUERY
-- ============================================================================

WITH accommodation_transactions AS (
  -- Extract all accommodation transactions above minimum threshold with division mapping
  SELECT
    r.census_region_division,
    DATE_TRUNC(t.trans_date, MONTH) as month_date,
    t.trans_amount,
    t.membccid
  FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
  JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
    ON t.mtid = m.mtid
  JOIN `prj-sandbox-i7sk.jk_testing.us_census_region_divisions_bridge_table` r
    ON m.merch_state = r.abbreviation
  WHERE t.trans_date BETWEEN start_date AND end_date
    AND t.trans_distance > distance_threshold
    AND m.merch_type = 0  -- Physical locations only
    AND m.merch_country = 'US'
    AND (m.mcc BETWEEN '3501' AND '3838' OR m.mcc IN ('7011', '7012'))
    AND t.trans_amount >= min_transaction_amount  -- Filter out small incidental charges
),

-- Calculate P5 and P98 thresholds per division per month for outlier removal
division_thresholds AS (
  SELECT
    census_region_division,
    month_date,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(5)] as p5,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(98)] as p98
  FROM accommodation_transactions
  GROUP BY census_region_division, month_date
),

-- Filter out outliers (transactions below P5 or above P98)
accommodations_no_outliers AS (
  SELECT
    a.census_region_division,
    a.month_date,
    a.trans_amount,
    a.membccid
  FROM accommodation_transactions a
  JOIN division_thresholds t
    ON a.census_region_division = t.census_region_division
    AND a.month_date = t.month_date
  WHERE a.trans_amount >= t.p5
    AND a.trans_amount <= t.p98
)

-- Calculate median accommodation cost with data quality flags
SELECT
  census_region_division,
  month_date,  -- First day of month for joining
  APPROX_QUANTILES(trans_amount, 100)[OFFSET(50)] as accommodation_cost,
  COUNT(*) as transaction_count,
  COUNT(DISTINCT membccid) as unique_visitors,

  -- Additional diagnostics
  MIN(trans_amount) as min_cost,
  MAX(trans_amount) as max_cost,
  APPROX_QUANTILES(trans_amount, 4)[OFFSET(1)] as q25,
  APPROX_QUANTILES(trans_amount, 4)[OFFSET(3)] as q75,

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
FROM accommodations_no_outliers
GROUP BY census_region_division, month_date
ORDER BY month_date, census_region_division
