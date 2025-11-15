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
DECLARE end_date DATE DEFAULT '2025-07-14';
DECLARE distance_threshold INT64 DEFAULT 60;  -- Miles for visitor classification
DECLARE min_transaction_amount FLOAT64 DEFAULT 50.0;  -- Minimum to qualify as accommodation booking

-- ============================================================================
-- MAIN QUERY
-- ============================================================================

WITH accommodation_transactions AS (
  -- Extract all accommodation transactions above minimum threshold
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
    AND (m.mcc BETWEEN '3501' AND '3838' OR m.mcc IN ('7011', '7012'))
    AND t.trans_amount >= min_transaction_amount  -- Filter out small incidental charges
),

-- Calculate P5 and P98 thresholds per state for outlier removal
state_thresholds AS (
  SELECT
    merch_state,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(5)] as p5,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(98)] as p98
  FROM accommodation_transactions
  GROUP BY merch_state
),

-- Filter out outliers (transactions below P5 or above P98)
accommodations_no_outliers AS (
  SELECT
    a.merch_state,
    a.trans_amount,
    a.membccid
  FROM accommodation_transactions a
  JOIN state_thresholds t
    ON a.merch_state = t.merch_state
  WHERE a.trans_amount >= t.p5
    AND a.trans_amount <= t.p98
)

-- Calculate median accommodation cost per state
SELECT
  merch_state,
  APPROX_QUANTILES(trans_amount, 100)[OFFSET(50)] as accommodation_cost,
  COUNT(*) as transaction_count,
  COUNT(DISTINCT membccid) as unique_visitors,
  -- Additional diagnostics
  MIN(trans_amount) as min_cost,
  MAX(trans_amount) as max_cost,
  APPROX_QUANTILES(trans_amount, 4)[OFFSET(1)] as q25,
  APPROX_QUANTILES(trans_amount, 4)[OFFSET(3)] as q75
FROM accommodations_no_outliers
GROUP BY merch_state
ORDER BY transaction_count DESC
