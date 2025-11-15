-- ============================================================================
-- RETAIL: State-Level Cost Calculation
-- ============================================================================
-- Purpose: Calculate median daily retail spending at the state level
--
-- Aggregation: Daily totals per cardholder per city
--              (sums all retail purchases made by same person in same city on same day)
-- Outlier Removal: P5/P98 per state
-- Date Range: Configurable (default 2-week test period)
--
-- Rationale: Retail spending represents daily shopping behavior - tourists
--            may visit multiple stores in a destination on a single day, and
--            we want to capture total daily shopping spend per person.
-- ============================================================================

-- CONFIGURATION: Update these values as needed
DECLARE start_date DATE DEFAULT '2025-07-01';
DECLARE end_date DATE DEFAULT '2025-07-14';
DECLARE distance_threshold INT64 DEFAULT 60;  -- Miles for visitor classification

-- MCC Codes for Retail (38 codes covering various retail categories)
-- Art supplies, cosmetics, books, sporting goods, apparel, jewelry,
-- gift shops, department stores, and specialty retail

-- ============================================================================
-- MAIN QUERY
-- ============================================================================

WITH retail_transactions AS (
  -- Aggregate transactions by cardholder + city + day
  SELECT
    m.merch_state,
    m.merch_city,
    t.membccid,
    DATE_TRUNC(t.trans_date, DAY) as shopping_day,
    SUM(t.trans_amount) as daily_total
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
  GROUP BY m.merch_state, m.merch_city, t.membccid, shopping_day
),

-- Calculate P5 and P98 thresholds per state for outlier removal
state_thresholds AS (
  SELECT
    merch_state,
    APPROX_QUANTILES(daily_total, 100)[OFFSET(5)] as p5,
    APPROX_QUANTILES(daily_total, 100)[OFFSET(98)] as p98
  FROM retail_transactions
  GROUP BY merch_state
),

-- Filter out outliers (daily totals below P5 or above P98)
retail_no_outliers AS (
  SELECT
    r.merch_state,
    r.daily_total,
    r.membccid
  FROM retail_transactions r
  JOIN state_thresholds t
    ON r.merch_state = t.merch_state
  WHERE r.daily_total >= t.p5
    AND r.daily_total <= t.p98
)

-- Calculate median daily retail spending per state
SELECT
  merch_state,
  APPROX_QUANTILES(daily_total, 100)[OFFSET(50)] as retail_cost,
  COUNT(*) as aggregated_day_count,
  COUNT(DISTINCT membccid) as unique_visitors
FROM retail_no_outliers
GROUP BY merch_state
ORDER BY aggregated_day_count DESC
