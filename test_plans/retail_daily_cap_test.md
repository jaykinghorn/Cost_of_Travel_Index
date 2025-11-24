# Retail Daily Cap Test Plan

**Branch**: `feature/retail-daily-cap-test`
**Version**: 0.1.0
**Date**: 2024-11-24
**Status**: Testing

## Problem Statement

Current methodology calculates retail spending medians directly from all transactions, resulting in unrealistically high median values (thousands of dollars) in some locations. This likely occurs when cardholders make numerous small purchases throughout the day, and the sum of all transactions inflates the perceived "typical" retail spending.

## Hypothesis

High median retail spending is driven by cardholders who make many small purchases throughout a single day. By capping the number of retail transactions per cardholder per day before calculating statistics, we can better represent typical daily retail spending patterns.

## Proposed Modification

### Current Approach (Main Branch)
1. Extract all retail transactions for the month
2. Apply P5/P98 outlier removal at transaction level
3. Calculate median from all filtered transactions
4. Multiply by basket quantity (retail_days)

### Test Approach (This Branch)
1. Extract all retail transactions for the month
2. **Group by cardholder (membccid) and transaction date (trans_date)**
3. **Cap at maximum 6 transactions per cardholder per day**
   - Sort transactions by amount (descending) to keep highest-value purchases
   - Keep only top 6 transactions per cardholder per day
4. Apply P5/P98 outlier removal at transaction level
5. Calculate median from filtered transactions
6. Multiply by basket quantity (retail_days)

## Implementation Details

### SQL Query Modification

Add a new CTE to cap daily retail transactions per cardholder:

```sql
-- Step 1: Rank retail transactions by cardholder and date
WITH retail_transactions_ranked AS (
  SELECT
    geo.admin2_id as county_fips,
    geo.admin1_abbr as state_abbr,
    t.trans_amount,
    t.trans_date,
    t.txid,
    t.membccid,
    ROW_NUMBER() OVER (
      PARTITION BY t.membccid, t.trans_date
      ORDER BY t.trans_amount DESC
    ) as daily_rank
  FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
  JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
    ON t.mtid = m.mtid
  JOIN `data-reference.Geo_Reference.admin_geo_reference` geo
    ON m.merch_city = geo.admin2_id
  WHERE t.trans_date BETWEEN @month_start AND @month_end
    AND t.trans_distance > 60
    AND m.merch_type = 0
    AND geo.country = 'United States'
    AND m.mcc IN ('5971', '5977', '7230', '5942', '5994', '5611', '5621', '5631',
                  '5641', '5651', '5681', '5691', '5699', '5311', '5992', '5993',
                  '5309', '5310', '5931', '5943', '5947', '5950', '5995', '5999',
                  '5944', '5948', '5661', '5655', '5832', '5932', '5937', '5940',
                  '5941', '5945', '5946', '5949', '5970', '5972')
),

-- Step 2: Filter to max 6 transactions per cardholder per day
retail_capped AS (
  SELECT
    county_fips,
    state_abbr,
    trans_amount,
    trans_date,
    txid,
    membccid
  FROM retail_transactions_ranked
  WHERE daily_rank <= 6
)

-- Then continue with outlier removal and statistics on retail_capped
```

### Python DataFrame Modification

Alternative implementation in pandas after SQL extraction:

```python
# After extracting retail transactions
retail_df = transactions_df[transactions_df['category'] == 'retail'].copy()

# Cap at 6 transactions per cardholder per day
retail_capped = (
    retail_df
    .sort_values('trans_amount', ascending=False)
    .groupby(['membccid', 'trans_date'])
    .head(6)
    .reset_index(drop=True)
)

# Then proceed with outlier removal and median calculation
# ... rest of processing pipeline
```

## Testing Approach

### 1. Baseline Comparison
Run both methodologies on the same test month (e.g., July 2024) and compare:
- Median retail spending per state/county
- Distribution of retail spending (P25, P50, P75)
- Number of transactions retained after capping
- Impact on total basket cost

### 2. Expected Outcomes
- **Reduced median retail spending** in locations with previously inflated values
- **More consistent retail spending** across similar destination types
- **Retained transaction count** should show how many transactions were excluded by the 6-per-day cap

### 3. Validation Metrics
Compare current vs. test approach:

| Metric | Current Method | Test Method (6-cap) | Expected Change |
|--------|----------------|---------------------|-----------------|
| Median retail (high-cost state) | $XXXX | $XXX | Decrease 50-80% |
| Median retail (typical state) | $XXX | $XXX | Minimal change |
| Transactions retained | 100% | ~60-80% | Decrease |
| Total basket cost variance | High | Lower | Stabilization |

## Configuration Parameters

Add new parameter to config dictionary:

```python
# Retail transaction cap (new for this test)
'max_retail_txn_per_cardholder_per_day' (int): Default 6
```

Make this configurable so we can test different thresholds (4, 6, 8, 10, unlimited).

## Rationale for 6 Transactions Cap

**Why 6?**
- Represents a reasonable upper bound for distinct retail purchases in a day
- Examples of 6 purchases:
  - Morning coffee shop
  - Souvenir shop #1
  - Lunch spot gift shop
  - Clothing store
  - Souvenir shop #2
  - Evening gift shop
- Prevents "spree shoppers" from skewing median upward
- Keeps highest-value purchases when capping (sorted descending)

**Alternatives to test**:
- 4 transactions: More conservative, may exclude legitimate shopping days
- 8 transactions: More permissive, may still include some spree behavior
- 10 transactions: Least restrictive tested option

## Success Criteria

The test will be considered successful if:

1. **Median retail spending** in high-outlier states decreases to realistic levels (under $500 for 2 days)
2. **Median retail spending** in typical states remains stable (minimal change)
3. **Transaction retention** is reasonable (>50% of transactions retained)
4. **Total basket cost** becomes more consistent across similar destination types
5. **Statistical validity** is maintained (sufficient sample sizes after capping)

## Rollout Plan

### Phase 1: Single Month Test (This Branch)
- Test on July 2024 data
- Compare results with current methodology
- Document findings in test results file

### Phase 2: Multi-Month Validation
- If Phase 1 successful, test across 3-6 months
- Check for seasonal consistency
- Validate threshold choice (6 vs. other values)

### Phase 3: Merge Decision
- If validation successful, update development_plan.md
- Merge to main branch
- Update project_overview.md with new methodology

## Files Modified in This Branch

- `test_plans/retail_daily_cap_test.md` (this file)
- `development_plan.md` (Section 3.4 - Retail Calculations)
- Notebook implementation (TBD when created)

## Analysis Questions to Answer

1. What percentage of cardholders make >6 retail transactions per day?
2. What is the distribution of retail transactions per cardholder per day?
3. How does the cap affect urban vs. rural destinations differently?
4. Does the cap impact states with large outlet malls or shopping districts disproportionately?
5. What is the optimal cap value (4, 6, 8, or 10)?

## Comparison with Other Categories

**Why only retail needs capping:**
- **Restaurants**: Already using percentiles (P35/P65), naturally resistant to outliers
- **Attractions**: Typically 1-2 major attractions per day, rarely excessive transactions
- **Accommodations**: Using median of single transactions, inherently capped per night
- **Retail**: Uniquely susceptible to many small purchases aggregating to large daily totals

## Next Steps

1. Update development_plan.md with test methodology
2. Implement SQL query with daily cap logic
3. Run test on July 2024 data
4. Document comparison results
5. Iterate on cap threshold if needed
6. Make merge decision based on validation results

---

**Test Owner**: Jay Kinghorn
**Review Date**: TBD after initial test results
**Decision Date**: TBD pending validation
