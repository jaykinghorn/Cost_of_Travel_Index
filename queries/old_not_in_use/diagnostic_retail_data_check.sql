-- Diagnostic Query: Retail Data Check
-- Purpose: Identify which filter is causing zero results in retail_daily_cap_test.sql

DECLARE month_start DATE DEFAULT '2024-07-01';
DECLARE month_end DATE DEFAULT '2024-07-31';
DECLARE distance_threshold FLOAT64 DEFAULT 60.0;

-- Step 1: Check if ANY transactions exist in July 2024
SELECT 'Total transactions in July 2024' AS check_name, COUNT(*) AS count
FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
WHERE t.trans_date BETWEEN month_start AND month_end

UNION ALL

-- Step 2: Check transactions with distance > 60 miles
SELECT 'Transactions with distance > 60 miles' AS check_name, COUNT(*) AS count
FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
WHERE t.trans_date BETWEEN month_start AND month_end
  AND t.trans_distance > distance_threshold

UNION ALL

-- Step 3: Check after joining to merchant_tourism
SELECT 'After joining merchant_tourism' AS check_name, COUNT(*) AS count
FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
  ON t.mtid = m.mtid
WHERE t.trans_date BETWEEN month_start AND month_end
  AND t.trans_distance > distance_threshold

UNION ALL

-- Step 4: Check with merch_type = 0 filter
SELECT 'After merch_type = 0 filter' AS check_name, COUNT(*) AS count
FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
  ON t.mtid = m.mtid
WHERE t.trans_date BETWEEN month_start AND month_end
  AND t.trans_distance > distance_threshold
  AND m.merch_type = 0

UNION ALL

-- Step 5: Check retail MCC codes
SELECT 'With retail MCC codes' AS check_name, COUNT(*) AS count
FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
  ON t.mtid = m.mtid
WHERE t.trans_date BETWEEN month_start AND month_end
  AND t.trans_distance > distance_threshold
  AND m.merch_type = 0
  AND m.mcc IN (
    '5971', '5977', '7230', '5942', '5994', '5611', '5621', '5631',
    '5641', '5651', '5681', '5691', '5699', '5311', '5992', '5993',
    '5309', '5310', '5931', '5943', '5947', '5950', '5995', '5999',
    '5944', '5948', '5661', '5655', '5832', '5932', '5937', '5940',
    '5941', '5945', '5946', '5949', '5970', '5972'
  )

UNION ALL

-- Step 6: Check after joining to geo reference
SELECT 'After joining admin_geo_reference' AS check_name, COUNT(*) AS count
FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
  ON t.mtid = m.mtid
JOIN `data-reference.Geo_Reference.admin_geo_reference` geo
  ON m.merch_city = CAST(geo.admin2_id AS STRING)
WHERE t.trans_date BETWEEN month_start AND month_end
  AND t.trans_distance > distance_threshold
  AND m.merch_type = 0
  AND m.mcc IN (
    '5971', '5977', '7230', '5942', '5994', '5611', '5621', '5631',
    '5641', '5651', '5681', '5691', '5699', '5311', '5992', '5993',
    '5309', '5310', '5931', '5943', '5947', '5950', '5995', '5999',
    '5944', '5948', '5661', '5655', '5832', '5932', '5937', '5940',
    '5941', '5945', '5946', '5949', '5970', '5972'
  )

UNION ALL

-- Step 7: Final check with country filter
SELECT 'Final with country = United States' AS check_name, COUNT(*) AS count
FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
  ON t.mtid = m.mtid
JOIN `data-reference.Geo_Reference.admin_geo_reference` geo
  ON m.merch_city = CAST(geo.admin2_id AS STRING)
WHERE t.trans_date BETWEEN month_start AND month_end
  AND t.trans_distance > distance_threshold
  AND m.merch_type = 0
  AND geo.country = 'United States'
  AND m.mcc IN (
    '5971', '5977', '7230', '5942', '5994', '5611', '5621', '5631',
    '5641', '5651', '5681', '5691', '5699', '5311', '5992', '5993',
    '5309', '5310', '5931', '5943', '5947', '5950', '5995', '5999',
    '5944', '5948', '5661', '5655', '5832', '5932', '5937', '5940',
    '5941', '5945', '5946', '5949', '5970', '5972'
  )

ORDER BY check_name;

-- Additional diagnostic: Check what merch_city values look like
-- SELECT 'Sample merch_city values' AS info, merch_city, COUNT(*) AS count
-- FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism`
-- WHERE merch_city IS NOT NULL
-- GROUP BY merch_city
-- LIMIT 10;
