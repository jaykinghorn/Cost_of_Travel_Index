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
   * Sort transactions by amount (descending) to keep highest-value purchases
   * Keep only top 6 transactions per cardholder per day
4. Apply P5/P98 outlier removal at transaction level
5. Calculate median from filtered transactions
6. Multiply by basket quantity (retail_days)


## Testing Approach

### 1. Baseline Comparison

Run both methodologies on the same test month (e.g., July 2024) and compare:

* Median retail spending per state
* Distribution of retail spending: High, median, low
* Number of transactions retained after capping
* Impact on total basket cost

### 2. Expected Outcomes

* **Reduced median retail spending** in locations with previously inflated values
* **More consistent retail spending** across similar destination types
* **Retained transaction count** should show how many transactions were excluded by the 6-per-day cap

### 3. Validation Metrics

Compare current vs. test approach:

| Metric | Current Method | Test Method (6-cap) | Test Method (8-cap) | Test Method (10-cap) |
|----|----|----|----|----|
| Retail High (high-cost state) -IA | $74.58 | $69.41 | $68.59 | $68.58 |
| Median retail (typical state) | $41.77 | $43.21 |    | Minimal change |
| Transactions retained | 100% | \~60-80% |    | Decrease |
| Total basket cost variance | High | Lower |    | Stabilization |

## 

## Rationale for 10 Transactions Cap

**Why 10?**

* Represents a reasonable upper bound for distinct retail purchases in a day
* Examples of 10 purchases:
  * Souvenir shop #1
  * Shoe store
  * Clothing store
  * Clothing store 
  * Souvenir shop #2
  * Evening gift shop
* Prevents "spree shoppers" from skewing median upward
* Keeps highest-value purchases when capping (sorted descending)

**Alternatives to test**:

* 6 transactions: More conservative, may exclude legitimate shopping days
* 8 transactions: More permissive, may still include some spree behavior
* 10 transactions: Least restrictive tested option


### Phase 3: Merge Decision

* If validation successful, update development_plan.md
* Merge to main branch
* Update project_overview.md with new methodology

## Files Modified in This Branch

* `test_plans/retail_daily_cap_test.md` (this file)
* `development_plan.md` (Section 3.4 - Retail Calculations)
* Notebook implementation (TBD when created)

## Analysis Questions to Answer


1. What percentage of cardholders make >10 retail transactions per day? (
2. What is the distribution of retail transactions per cardholder per day?
3. How does the cap affect urban vs. rural destinations differently?
4. Does the cap impact states with large outlet malls or shopping districts disproportionately?
5. What is the optimal cap value (4, 6, 8, or 10)?

## Comparison with Other Categories

**Why only retail needs capping:**

* **Restaurants**: Already using percentiles (P35/P65), naturally resistant to outliers
* **Attractions**: Typically 1-2 major attractions per day, rarely excessive transactions
* **Accommodations**: Using median of single transactions, inherently capped per night
* **Retail**: Uniquely susceptible to many small purchases aggregating to large daily totals

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