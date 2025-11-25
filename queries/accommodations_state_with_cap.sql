-- ============================================================================
-- ACCOMMODATIONS: State-Level Cost Calculation WITH DAILY CAP
-- ============================================================================
-- Purpose: Calculate median accommodation costs at the state level
--          with a daily transaction cap per cardholder to prevent inflated medians
--
-- NEW: Daily Transaction Cap - Maximum 10 transactions per cardholder per day
--      Keeps highest-value transactions when capping to preserve legitimate spending
--      Prevents high-frequency bookers from skewing median upward
--
-- Aggregation: None - individual transactions analyzed
-- Minimum Threshold: $50 per transaction (filters out incidental charges like
--                    parking fees, minibar, resort fees charged separately)
-- Outlier Removal: P5/P98 per state (applied AFTER capping)
-- Date Range: Configurable (default 2-week test period)
--
-- Rationale: Using individual transactions with daily cap and $50 minimum threshold
--            provides a representative sample of accommodation bookings while filtering
--            out small incidental charges and preventing high-frequency transactions
--            from inflating the median.
--
-- MCC Codes: 3501-3838 (hotel chains), 7011-7012 (hotels/timeshares)
--            300+ codes covering various hotel chains, resorts, and accommodations
-- ============================================================================

-- CONFIGURATION: Update these values as needed
DECLARE start_date DATE DEFAULT '2025-07-01';
DECLARE end_date DATE DEFAULT '2025-07-31';
DECLARE distance_threshold INT64 DEFAULT 60;  -- Miles for visitor classification
DECLARE min_transaction_amount FLOAT64 DEFAULT 50.0;  -- Minimum to qualify as accommodation booking
DECLARE max_accommodation_txn_per_cardholder_per_day INT64 DEFAULT 10;  -- Daily transaction cap

-- DATA QUALITY THRESHOLDS (95% CI, 5% MOE)
DECLARE min_sample_exclude INT64 DEFAULT 600;  -- Exclude geography if below this
DECLARE min_sample_rolling INT64 DEFAULT 2000; -- Use 3-month rolling if below this

-- ============================================================================
-- MAIN QUERY
-- ============================================================================

WITH accommodation_transactions_raw AS (
  -- Extract all accommodation transactions with daily ranking
  SELECT
    m.merch_state,
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
  WHERE t.trans_date BETWEEN start_date AND end_date
    AND t.trans_distance > distance_threshold
    AND m.merch_type = 0  -- Physical locations only
    AND m.merch_country = 'US'
    AND (m.mcc BETWEEN '3501' AND '3838' OR m.mcc IN ('7011', '7012'))
    AND t.trans_amount >= min_transaction_amount  -- Filter out small incidental charges
),

-- Apply daily transaction cap
accommodation_transactions_capped AS (
  SELECT
    merch_state,
    month_date,
    trans_amount,
    membccid,
    txid
  FROM accommodation_transactions_raw
  WHERE daily_rank <= max_accommodation_txn_per_cardholder_per_day
),

-- Analyze per-cardholder booking patterns
cardholder_booking_patterns AS (
  SELECT
    merch_state,
    month_date,
    membccid,
    COUNT(DISTINCT trans_date) as booking_days,
    COUNT(DISTINCT CASE WHEN daily_txn_count > max_accommodation_txn_per_cardholder_per_day THEN trans_date END) as days_over_cap,
    AVG(daily_txn_count) as avg_daily_txn_count,
    MAX(daily_txn_count) as max_daily_txn_count
  FROM (
    SELECT
      merch_state,
      month_date,
      membccid,
      trans_date,
      COUNT(*) as daily_txn_count
    FROM accommodation_transactions_raw
    GROUP BY merch_state, month_date, membccid, trans_date
  )
  GROUP BY merch_state, month_date, membccid
),

-- Track capping impact per state with booking behavior metrics
capping_impact AS (
  SELECT
    r.merch_state,
    r.month_date,
    COUNT(*) as txn_before_cap,
    SUM(CASE WHEN r.daily_rank <= max_accommodation_txn_per_cardholder_per_day THEN 1 ELSE 0 END) as txn_after_cap,
    COUNT(*) - SUM(CASE WHEN r.daily_rank <= max_accommodation_txn_per_cardholder_per_day THEN 1 ELSE 0 END) as txn_capped,
    COUNT(DISTINCT r.membccid) as unique_cardholders_total,

    -- Cardholders who exceeded cap on at least one day
    COUNT(DISTINCT CASE WHEN r.daily_rank > max_accommodation_txn_per_cardholder_per_day THEN r.membccid END) as cardholders_affected_by_cap,

    -- Average transactions per cardholder per day (across all cardholders and booking days)
    ROUND(AVG(p.avg_daily_txn_count), 2) as avg_txn_per_cardholder_per_day,

    -- Cardholders who FREQUENTLY exceed the cap (>50% of their booking days)
    COUNT(DISTINCT CASE
      WHEN SAFE_DIVIDE(p.days_over_cap, p.booking_days) > 0.5 THEN p.membccid
    END) as frequent_high_volume_bookers

  FROM accommodation_transactions_raw r
  JOIN cardholder_booking_patterns p
    ON r.merch_state = p.merch_state
    AND r.month_date = p.month_date
    AND r.membccid = p.membccid
  GROUP BY r.merch_state, r.month_date
),

-- Calculate P5 and P98 thresholds per state per month for outlier removal (AFTER capping)
state_thresholds AS (
  SELECT
    merch_state,
    month_date,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(5)] as p5,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(98)] as p98
  FROM accommodation_transactions_capped
  GROUP BY merch_state, month_date
),

-- Filter out outliers (transactions below P5 or above P98)
accommodations_no_outliers AS (
  SELECT
    a.merch_state,
    a.month_date,
    a.trans_amount,
    a.membccid
  FROM accommodation_transactions_capped a
  JOIN state_thresholds t
    ON a.merch_state = t.merch_state
    AND a.month_date = t.month_date
  WHERE a.trans_amount >= t.p5
    AND a.trans_amount <= t.p98
)

-- Calculate median accommodation cost with data quality flags and capping metrics
SELECT
  a.merch_state,
  a.month_date,  -- First day of month for joining
  APPROX_QUANTILES(a.trans_amount, 100)[OFFSET(50)] as accommodation_cost,
  COUNT(*) as transaction_count_final,
  COUNT(DISTINCT a.membccid) as unique_visitors,

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
  i.frequent_high_volume_bookers,
  ROUND(SAFE_DIVIDE(i.frequent_high_volume_bookers, i.unique_cardholders_total) * 100, 2) as pct_frequent_high_volume_bookers,

  -- Outlier removal metrics
  i.txn_after_cap as txn_before_outlier_removal,
  i.txn_after_cap - COUNT(*) as txn_removed_as_outliers,
  ROUND(SAFE_DIVIDE(i.txn_after_cap - COUNT(*), i.txn_after_cap) * 100, 2) as pct_txn_removed_as_outliers,

  -- Additional diagnostics
  MIN(a.trans_amount) as min_cost,
  MAX(a.trans_amount) as max_cost,
  APPROX_QUANTILES(a.trans_amount, 4)[OFFSET(1)] as q25,
  APPROX_QUANTILES(a.trans_amount, 4)[OFFSET(3)] as q75,

  -- Data quality assessment
  CASE
    WHEN COUNT(DISTINCT a.membccid) < min_sample_exclude THEN 'EXCLUDE'
    WHEN COUNT(DISTINCT a.membccid) < min_sample_rolling THEN 'ROLLING_3MO'
    ELSE 'SINGLE_MONTH'
  END as data_quality_flag,

  -- Metadata for reference
  start_date as period_start,
  end_date as period_end,
  max_accommodation_txn_per_cardholder_per_day as max_daily_cap,
  CURRENT_TIMESTAMP() as calculation_timestamp

FROM accommodations_no_outliers a
LEFT JOIN capping_impact i
  ON a.merch_state = i.merch_state
  AND a.month_date = i.month_date
GROUP BY a.merch_state, a.month_date, i.txn_before_cap, i.txn_after_cap,
         i.txn_capped, i.cardholders_affected_by_cap, i.unique_cardholders_total,
         i.avg_txn_per_cardholder_per_day, i.frequent_high_volume_bookers
ORDER BY a.month_date, transaction_count_final DESC;
