-- ============================================================================
-- ACCOMMODATIONS: State-Level Cost Calculation
-- ============================================================================
-- Purpose: Calculate median accommodation costs at the state level
--
-- Aggregation: None - individual transactions analyzed
-- Minimum Threshold: $50 per transaction (filters out incidental charges like
--                    parking fees, minibar, resort fees charged separately)
-- Outlier Removal: P5/P98 per state
-- Date Range: Configurable (default 2-week test period)
--
-- Rationale: Using individual transactions with $50 minimum threshold provides
--            a representative sample of accommodation bookings while filtering
--            out small incidental charges. This simpler approach avoids the
--            complexity of cardholder aggregation while still capturing typical
--            hotel stay costs.
--
-- MCC Codes: 3501-3838 (hotel chains), 7011-7012 (hotels/timeshares)
--            300+ codes covering various hotel chains, resorts, and accommodations
-- ============================================================================

-- CONFIGURATION: Update these values as needed
DECLARE start_date DATE DEFAULT '2025-07-01';
DECLARE end_date DATE DEFAULT '2025-07-31';
DECLARE distance_threshold INT64 DEFAULT 60;  -- Miles for visitor classification
DECLARE min_transaction_amount FLOAT64 DEFAULT 50.0;  -- Minimum to qualify as accommodation booking

-- DATA QUALITY THRESHOLDS (95% CI, 5% MOE)
DECLARE min_sample_exclude INT64 DEFAULT 600;  -- Exclude geography if below this
DECLARE min_sample_rolling INT64 DEFAULT 2000; -- Use 3-month rolling if below this

-- ============================================================================
-- MAIN QUERY
-- ============================================================================

WITH accommodation_transactions AS (
  -- Extract all accommodation transactions above minimum threshold
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
    AND (m.mcc BETWEEN '3501' AND '3838' OR m.mcc IN ('7011', '7012'))
    AND t.trans_amount >= min_transaction_amount  -- Filter out small incidental charges
),

-- Calculate P5 and P98 thresholds per state per month for outlier removal
state_thresholds AS (
  SELECT
    merch_state,
    month_date,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(5)] as p5,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(98)] as p98
  FROM accommodation_transactions
  GROUP BY merch_state, month_date
),

-- Filter out outliers (transactions below P5 or above P98)
accommodations_no_outliers AS (
  SELECT
    a.merch_state,
    a.month_date,
    a.trans_amount,
    a.membccid
  FROM accommodation_transactions a
  JOIN state_thresholds t
    ON a.merch_state = t.merch_state
    AND a.month_date = t.month_date
  WHERE a.trans_amount >= t.p5
    AND a.trans_amount <= t.p98
)

-- Calculate median accommodation cost with data quality flags
SELECT
  merch_state,
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
  MIN(start_date) as period_start,
  MAX(end_date) as period_end,
  CURRENT_TIMESTAMP() as calculation_timestamp
FROM accommodations_no_outliers
GROUP BY merch_state, month_date
ORDER BY month_date, transaction_count DESC
