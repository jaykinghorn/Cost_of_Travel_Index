-- ============================================================================
-- CTI REGIONAL ATTRACTIONS: Census Division-Level Cost Calculation
-- ============================================================================
-- Purpose: Calculate median per-visit attraction costs at Census Division level
--
-- Aggregation: None - individual transactions analyzed
-- Geographic Level: 9 Census Divisions (New England, Middle Atlantic, etc.)
-- Outlier Removal: P5/P98 per division per month
-- Date Range: Configurable
--
-- MCC Codes: 7996, 7995, 7998, 7991, 7933, 7832, 7911, 7929, 7922, 7932, 7994, 7999
-- Categories: Amusement parks, aquariums, recreation services, bowling alleys,
--             billiards, dance halls, bands/orchestras, theatrical producers,
--             golf courses, public golf courses, recreation services
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

WITH attraction_transactions AS (
  -- Extract all attraction transactions for the period with division mapping
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
    AND m.mcc IN (
      '7996',  -- Amusement Parks, Carnivals, Circuses, Fortune Tellers
      '7995',  -- Betting/Casino Gambling
      '7998',  -- Aquariums, Seaquariums, Dolphinariums
      '7991',  -- Tourist Attractions and Exhibits
      '7933',  -- Bowling Alleys
      '7832',  -- Motion Picture Theaters
      '7911',  -- Dance Halls, Studios, and Schools
      '7929',  -- Bands, Orchestras, and Miscellaneous Entertainers
      '7922',  -- Theatrical Producers (Except Motion Pictures), Ticket Agencies
      '7932',  -- Billiard and Pool Establishments
      '7994',  -- Video Game Arcades/Establishments
      '7999'   -- Recreation Services (Not Elsewhere Classified)
    )
),

-- Calculate P5 and P98 thresholds per division per month for outlier removal
division_thresholds AS (
  SELECT
    census_region_division,
    month_date,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(5)] as p5,
    APPROX_QUANTILES(trans_amount, 100)[OFFSET(98)] as p98
  FROM attraction_transactions
  GROUP BY census_region_division, month_date
),

-- Filter out outliers (transactions below P5 or above P98)
attractions_no_outliers AS (
  SELECT
    a.census_region_division,
    a.month_date,
    a.trans_amount,
    a.membccid
  FROM attraction_transactions a
  JOIN division_thresholds t
    ON a.census_region_division = t.census_region_division
    AND a.month_date = t.month_date
  WHERE a.trans_amount >= t.p5
    AND a.trans_amount <= t.p98
)

-- Calculate median attraction cost with data quality flags
SELECT
  census_region_division,
  month_date,  -- First day of month for joining
  APPROX_QUANTILES(trans_amount, 100)[OFFSET(50)] as attraction_cost,
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
FROM attractions_no_outliers
GROUP BY census_region_division, month_date
ORDER BY month_date, census_region_division
