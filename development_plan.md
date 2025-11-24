# Cost of Travel Index - Development Plan

Version 0.1.2

## Overview

This development plan outlines the implementation strategy for building the Cost of Travel Index, a county, city and state-level economic analysis tool that calculates the cost of a hypothetical three-day weekend across U.S. locations using Zartico's spend datasets.

### Key Constraints & Considerations

* **Monthly Processing**: Process one month at a time due to data volume constraints
* **Platform**: Jupyter/Colab notebook compatible with both environments
* **Data Sources**: BigQuery tables (transaction_tourism, merchant_tourism, admin_geo_reference)
* **Statistical Method**: P5/P98 outlier removal before calculating percentiles and medians
* **Configurability**: All parameters (percentiles, thresholds, basket quantities) must be configurable

  \


---

## Phase 1: Setup & Configuration

### 1.1 Environment Detection & Setup

**Task**: Create environment-aware initialization

* Detect whether running in Jupyter or Colab
* Set up BigQuery authentication accordingly:
  * Colab: Use `google.colab.auth.authenticate_user()`
  * Jupyter: Use service account or application default credentials
* Import required libraries: `pandas`, `numpy`, `google-cloud-bigquery`, `datetime`
* Test BigQuery connectivity

**Deliverable**: Notebook cell with environment detection and authentication logic

### 1.2 Configuration Parameters

**Task**: Create centralized configuration dictionary

* **Date Parameters**:
  * `target_month` (string): "YYYY-MM-01" format for processing month
  * `month_start_date` (date): Derived from target_month
  * `month_end_date` (date): Last day of target_month
* **Statistical Parameters**:
  * `outlier_lower_percentile` (float): Default 5 (P5)
  * `outlier_upper_percentile` (float): Default 98 (P98)
  * `restaurant_low_percentile` (float): Default 35 (breakfast/lunch)
  * `restaurant_high_percentile` (float): Default 65 (dinner)
* **Basket Quantities**:
  * `avg_lodging` (Float): Default 1 (single transaction for multiple days)
  * `low_price_meals` (int): Default 4 (breakfast & lunch)
  * `high_price_meals` (int): Default 2 (dinner)
  * `attraction_days` (int): Default 2
  * `retail_days` (int): Default 2
* **Retail Transaction Cap** (TEST BRANCH):
  * `max_retail_txn_per_cardholder_per_day` (int): Default 6
  * Prevents inflated medians from high-frequency shoppers
  * Keeps highest-value transactions when capping
* **Quality Thresholds** (initial estimates, to be refined in Phase 4):
  * `min_lodging_transactions` (int): Default 30
  * `min_restaurant_transactions` (int): Default 50
  * `min_attraction_transactions` (int): Default 20
  * `min_retail_transactions` (int): Default 20
* **MCC Code Categories** (configurable to allow category refinement):
  * `mcc_restaurants` (list): \[5812\] - Full-service restaurants
  * `mcc_attractions` (list): \[7996, 7995, 7998, 7991, 7933, 7832, 7911, 7929, 7922, 7932, 7994, 7999\] - Amusement parks, aquariums, recreation services, bowling alleys, billiards, dance halls, bands/orchestras, theatrical producers, golf courses, public golf courses, recreation services
  * `mcc_retail` (list): \["5971", "5977", "7230", "5942", "5994", "5611", "5621", "5631", "5641", "5651", "5681", "5691", "5699", "5311", "5992", "5993", "5309", "5310", "5931", "5943", "5947", "5950", "5995", "5999", "5944", "5948", "5661", "5655", "5832", "5932", "5937", "5940", "5941", "5945", "5946", "5949", "5970", "5972"\] - Various retail categories including art supplies, cosmetics, beauty supplies, books, sporting goods, apparel, department stores, gift shops, jewelry, and specialty retail
* **Data Source Configuration**:
  * `spend_dataset`: "prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment"
  * `geo_dataset`: "data-reference.Geo_Reference"
  * `lodging_dataset`: TBD (placeholder for KeyData Lodging)
  * `output_dataset`: TBD (destination for results)
  * `output_table`: TBD (cost_of_travel_index table name)
* **DC FIPS Code Mapping**:
  * `dc_fips_code` (string): "11001" - Washington DC FIPS code placeholder
  * Note: DC may have different or missing FIPS representation between spend and lodging datasets; manual mapping may be required

**Deliverable**: Python dictionary with all configurable parameters, with comments explaining each

### 1.3 Helper Functions

**Task**: Create utility functions for reusability

* `format_month_date(year, month)`: Format date strings for queries
* `get_month_boundaries(target_month)`: Return start and end dates
* `get_weekend_days(target_month)`: Return list of Friday/Saturday dates in month
* `log_progress(message, details)`: Simple logging for tracking progress
* `validate_config(config)`: Validate configuration parameters

**Deliverable**: Function definitions with docstrings


---

## Phase 2: Data Extraction (SQL Queries)

### 2.1 County Reference Data Query

**Task**: Extract county FIPS codes and names

* Query `admin_geo_reference` table
* Select US counties only (country = 'United States')
* Map admin2_id to county FIPS codes
* Include admin1 (state) for reference
* Cache results (static reference data)

**SQL Requirements**:

```sql
SELECT DISTINCT
  admin2_id as county_fips,
  admin2 as county_name,
  admin1 as state_name,
  admin1_abbr as state_abbr
FROM `data-reference.Geo_Reference.admin_geo_reference`
WHERE country = 'United States'
  AND admin2_id IS NOT NULL
ORDER BY admin2_id
```

**Deliverable**: DataFrame with county reference data

### 2.2 Transaction Data Query (Spend Categories)

**Task**: Extract monthly transaction data for restaurants, attractions, and retail

* Query `transaction_tourism` joined with `merchant_tourism`
* Filter by month date range
* Filter by visitor transactions (trans_distance > 60 miles)
* Filter by physical merchant locations (merch_type = 0)
* Join to admin_geo_reference on merch_city to get county FIPS
* Classify transactions by MCC codes:
  * **Restaurants**: MCC 5812 (full-service restaurants)
  * **Attractions**: MCCs 7996, 7995, 7998, 7991, 7933, 7832, 7911, 7929, 7922, 7932, 7994, 7999 (amusement parks, aquariums, recreation services, bowling, billiards, dance halls, entertainment, golf courses)
  * **Retail**: 38 retail MCCs covering art supplies, cosmetics, books, sporting goods, apparel, jewelry, gift shops, department stores, and specialty retail
* Select: county_fips, category, trans_amount, trans_date, txid

**SQL Structure**:

```sql
SELECT
  geo.admin2_id as county_fips,
  CASE
    WHEN m.mcc = '5812' THEN 'restaurant'
    WHEN m.mcc IN ('7996', '7995', '7998', '7991', '7933', '7832', '7911',
                   '7929', '7922', '7932', '7994', '7999') THEN 'attraction'
    WHEN m.mcc IN ('5971', '5977', '7230', '5942', '5994', '5611', '5621', '5631',
                   '5641', '5651', '5681', '5691', '5699', '5311', '5992', '5993',
                   '5309', '5310', '5931', '5943', '5947', '5950', '5995', '5999',
                   '5944', '5948', '5661', '5655', '5832', '5932', '5937', '5940',
                   '5941', '5945', '5946', '5949', '5970', '5972') THEN 'retail'
    ELSE 'other'
  END as category,
  t.trans_amount,
  t.trans_date,
  t.txid
FROM `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.transaction_tourism` t
JOIN `prj-prod-codecs-spend-b3c4.Spend_CODEC_Enrichment.merchant_tourism` m
  ON t.mtid = m.mtid
JOIN `data-reference.Geo_Reference.admin_geo_reference` geo
  ON m.merch_city = geo.admin2_id
WHERE t.trans_date BETWEEN @month_start AND @month_end
  AND t.trans_distance > 60
  AND m.merch_type = 0
  AND geo.country = 'United States'
  AND (m.mcc = '5812'
    OR m.mcc IN ('7996', '7995', '7998', '7991', '7933', '7832', '7911',
                 '7929', '7922', '7932', '7994', '7999')
    OR m.mcc IN ('5971', '5977', '7230', '5942', '5994', '5611', '5621', '5631',
                 '5641', '5651', '5681', '5691', '5699', '5311', '5992', '5993',
                 '5309', '5310', '5931', '5943', '5947', '5950', '5995', '5999',
                 '5944', '5948', '5661', '5655', '5832', '5932', '5937', '5940',
                 '5941', '5945', '5946', '5949', '5970', '5972'))
```

**Query Optimization**:

* Use parameterized queries for dates
* Partition by ref_date (merchant_tourism partitioning column)
* Consider using APPROX_QUANTILES for initial exploration if exact percentiles are too expensive

**Deliverable**: DataFrame with all spend transactions for the month

### 2.3 Lodging Data Query

**Task**: Extract Friday/Saturday lodging data for the month

* Query lodging dataset (schema TBD, awaiting details)
* Filter for Friday and Saturday nights only within target month
* Extract: county_fips, night_date, ADR (average daily rate), property_id
* **Handle DC FIPS**: Apply manual FIPS code mapping for Washington DC
* Group by property and date for deduplication if needed

**SQL Structure** (placeholder - adjust when lodging schema available):

```sql
SELECT
  county_fips,  -- Adjust field name based on actual schema
  stay_date as night_date,
  adr,
  property_id
FROM `[lodging_dataset_uri]`
WHERE stay_date IN (@friday_saturday_dates)
  AND stay_date BETWEEN @month_start AND @month_end
  AND country = 'United States'
```

**DC FIPS Handling**:

```python
# After loading lodging data, ensure DC uses consistent FIPS code 11001
# If DC has different representation in lodging data, map to standard FIPS
if 'county_fips' in lodging_df.columns:
    # Normalize DC FIPS to 11001 if it appears differently
    lodging_df['county_fips'] = lodging_df['county_fips'].replace(
        {None: '11001'}  # Adjust mapping based on actual lodging data representation
    )
```

**Deliverable**: DataFrame with Friday/Saturday lodging data

### 2.4 Property Count Query

**Task**: Count distinct hotel properties per county for quality thresholds

* Count distinct property_id per county_fips
* Based on same Friday/Saturday filter as lodging data
* Will be used to validate minimum reporting threshold

**Deliverable**: DataFrame with county_fips and property_count


---

## Phase 3: Data Processing & Calculations (Python)

### 3.1 Outlier Removal Function

**Task**: Create function to remove P5/P98 outliers per county

* Input: DataFrame with county_fips, category, trans_amount
* Group by county_fips and category
* Calculate P5 and P98 thresholds per group
* Filter out transactions outside \[P5, P98\] range
* Track removed transaction counts per county and category
* Return: filtered DataFrame + removal statistics DataFrame

**Function Signature**:

```python
def remove_outliers(df, config):
    """
    Remove outlier transactions based on percentile thresholds.

    Parameters:
    - df: DataFrame with columns [county_fips, category, trans_amount, txid]
    - config: Configuration dictionary with outlier percentile settings

    Returns:
    - filtered_df: DataFrame with outliers removed
    - removal_stats: DataFrame with removal counts per county/category
    """
```

**Key Logic**:

* Use `groupby().transform()` to calculate percentiles within groups
* Use `pd.qcut()` or `df.quantile()` for percentile calculations
* Create boolean mask for filtering
* Count removed rows by comparing original and filtered DataFrames

**Deliverable**: Reusable function with comprehensive outlier removal logic

### 3.2 Restaurant Calculations

**Task**: Calculate breakfast/lunch and dinner costs per county

* Input: Filtered restaurant transactions
* Group by county_fips
* Calculate P35 (breakfast/lunch price point)
* Calculate P65 (dinner price point)
* Multiply by basket quantities:
  * breakfast_lunch_cost = P35 \* low_price_meals (default 4)
  * dinner_cost = P65 \* high_price_meals (default 2)
* Track transaction counts per county

**Pandas Operations**:

```python
restaurant_df = filtered_df[filtered_df['category'] == 'restaurant']
restaurant_stats = restaurant_df.groupby('county_fips')['trans_amount'].agg([
    ('p35', lambda x: x.quantile(0.35)),
    ('p65', lambda x: x.quantile(0.65)),
    ('txn_count', 'count')
])
restaurant_stats['breakfast_lunch_cost'] = restaurant_stats['p35'] * config['low_price_meals']
restaurant_stats['dinner_cost'] = restaurant_stats['p65'] * config['high_price_meals']
```

**Deliverable**: DataFrame with county-level restaurant costs

### 3.3 Attraction Calculations

**Task**: Calculate attraction costs per county

* Input: Filtered attraction transactions
* Group by county_fips
* Calculate median daily attraction spending
* Multiply by basket quantity (default 2 days)
* Track transaction counts per county

**Pandas Operations**:

```python
attraction_df = filtered_df[filtered_df['category'] == 'attraction']
attraction_stats = attraction_df.groupby('county_fips')['trans_amount'].agg([
    ('median', 'median'),
    ('txn_count', 'count')
])
attraction_stats['attraction_cost'] = attraction_stats['median'] * config['attraction_days']
```

**Deliverable**: DataFrame with county-level attraction costs

### 3.4 Retail Calculations

**Task**: Calculate retail costs per county with daily transaction cap

**TEST BRANCH NOTE**: This section implements a modified approach to prevent inflated retail medians caused by high-frequency shoppers making many small purchases per day.

* Input: Filtered retail transactions
* **NEW**: Cap at maximum 6 transactions per cardholder (membccid) per day
  - Keeps highest-value transactions when capping
  - Prevents "spree shoppers" from skewing median upward
* Group by county_fips
* Calculate median daily retail spending
* Multiply by basket quantity (default 2 days)
* Track transaction counts per county (before and after capping)

**Pandas Operations**:

```python
retail_df = filtered_df[filtered_df['category'] == 'retail'].copy()

# Cap at max 6 retail transactions per cardholder per day
# Sort by amount descending to keep highest-value purchases
retail_capped = (
    retail_df
    .sort_values('trans_amount', ascending=False)
    .groupby(['membccid', 'trans_date'])
    .head(config['max_retail_txn_per_cardholder_per_day'])  # default 6
    .reset_index(drop=True)
)

# Track capping impact
txn_before_cap = len(retail_df)
txn_after_cap = len(retail_capped)
txn_capped = txn_before_cap - txn_after_cap

# Calculate median from capped transactions
retail_stats = retail_capped.groupby('county_fips')['trans_amount'].agg([
    ('median', 'median'),
    ('txn_count', 'count')
])
retail_stats['txn_capped'] = txn_capped  # Track for validation
retail_stats['retail_cost'] = retail_stats['median'] * config['retail_days']
```

**SQL Alternative** (apply cap in query):

```sql
WITH retail_transactions_ranked AS (
  SELECT
    geo.admin2_id as county_fips,
    t.trans_amount,
    t.trans_date,
    t.txid,
    t.membccid,
    ROW_NUMBER() OVER (
      PARTITION BY t.membccid, t.trans_date
      ORDER BY t.trans_amount DESC
    ) as daily_rank
  FROM transaction_tourism t
  JOIN merchant_tourism m ON t.mtid = m.mtid
  JOIN admin_geo_reference geo ON m.merch_city = geo.admin2_id
  WHERE m.mcc IN (...retail MCCs...)
    AND t.trans_date BETWEEN @month_start AND @month_end
    AND t.trans_distance > 60
)
SELECT * FROM retail_transactions_ranked
WHERE daily_rank <= 6  -- Cap at 6 transactions per cardholder per day
```

**Configuration Parameter**:
- `max_retail_txn_per_cardholder_per_day` (int): Default 6
  - Rationale: Reasonable upper bound for distinct retail purchases in a day
  - Alternative values to test: 4, 8, 10

**Deliverable**: DataFrame with county-level retail costs and capping statistics

### 3.5 Lodging Calculations

**Task**: Calculate Friday/Saturday lodging costs per county

* Input: Friday/Saturday lodging data (already filtered in SQL)
* Group by county_fips
* Calculate mean ADR across all Friday/Saturday nights in month
* Multiply by basket quantity (default 2 nights)
* Track property counts per county (for quality threshold)
* Note: Using mean instead of median because ADR is already an aggregated metric

**Pandas Operations**:

```python
lodging_stats = lodging_df.groupby('county_fips').agg({
    'adr': 'mean',
    'property_id': 'nunique'
})
lodging_stats.rename(columns={
    'adr': 'avg_adr',
    'property_id': 'hotel_property_count'
}, inplace=True)
lodging_stats['lodging_cost'] = lodging_stats['avg_adr'] * config['lodging_nights']
```

**Special Handling**:

* If lodging data includes both nightly rates and occupancy, calculate weighted average
* Handle properties with missing Friday or Saturday data (use available nights)

**Deliverable**: DataFrame with county-level lodging costs and property counts

### 3.6 Merge Components into Final Dataset

**Task**: Combine all cost components into single county-level DataFrame

* Merge restaurant, attraction, retail, and lodging DataFrames on county_fips
* Use outer join to preserve all counties with any data
* Calculate total_basket_cost as sum of all components
* Add month_date column with target month
* Handle missing values (NaN) for counties lacking specific categories

**Pandas Operations**:

```python
final_df = county_ref_df[['county_fips']].merge(
    restaurant_stats, on='county_fips', how='left'
).merge(
    attraction_stats, on='county_fips', how='left'
).merge(
    retail_stats, on='county_fips', how='left'
).merge(
    lodging_stats, on='county_fips', how='left'
)

# Calculate total basket cost
final_df['total_basket_cost'] = final_df[[
    'breakfast_lunch_cost', 'dinner_cost', 'attraction_cost',
    'retail_cost', 'lodging_cost'
]].sum(axis=1, skipna=False)  # NaN if any component is missing
```

**Deliverable**: Comprehensive DataFrame with all basket components per county


---

## Phase 4: Quality Thresholds & Validation

### 4.1 Apply Data Quality Thresholds

**Task**: Flag counties that meet minimum data requirements

* Create boolean column `meets_threshold`
* Check against all configured thresholds:
  * Minimum hotel properties reporting
  * Minimum transaction counts per category
  * All basket components must have valid (non-NaN) values
* Counties not meeting thresholds still included in output but flagged

**Logic**:

```python
final_df['meets_threshold'] = (
    (final_df['hotel_property_count'] >= config['min_hotel_properties']) &
    (final_df['restaurant_txn_count'] >= config['min_restaurant_transactions']) &
    (final_df['attraction_txn_count'] >= config['min_attraction_transactions']) &
    (final_df['retail_txn_count'] >= config['min_retail_transactions']) &
    (final_df['lodging_txn_count'] >= config['min_lodging_transactions']) &
    (final_df['total_basket_cost'].notna())
)
```

**Deliverable**: Final DataFrame with quality threshold flags

### 4.2 Validation & Data Quality Report

**Task**: Generate summary statistics for review before writing to BigQuery

* Count of counties processed
* Count of counties meeting thresholds
* Distribution of basket costs (min, max, median, quartiles)
* Count of missing/NaN values per column
* List of counties with unusual values (outliers at county level)
* Summary of outlier removal (total transactions removed)

**Outputs**:

* Print summary statistics to notebook
* Generate DataFrame with validation metrics
* Flag potential data quality issues for review

**Key Metrics**:

```python
print(f"Total counties processed: {len(final_df)}")
print(f"Counties meeting thresholds: {final_df['meets_threshold'].sum()}")
print(f"Counties missing lodging data: {final_df['lodging_cost'].isna().sum()}")
print(f"\nBasket cost distribution:")
print(final_df['total_basket_cost'].describe())
```

**Deliverable**: Validation report displayed in notebook

### 4.3 Manual Review Checkpoint

**Task**: Display sample records for manual verification

* Show top 10 and bottom 10 counties by total_basket_cost
* Display random sample of 20 counties
* Show counties just below threshold (for threshold tuning)
* Allow user to inspect data before final write

**Deliverable**: Interactive display of sample records


---

## Phase 5: Output & Storage

### 5.1 Prepare Output Schema

**Task**: Format final DataFrame to match output table schema

* Add `created_at` timestamp (current time)
* Ensure column names match schema exactly:
  * county_fips (string)
  * month_date (date)
  * lodging_cost (float)
  * breakfast_lunch_cost (float)
  * dinner_cost (float)
  * attraction_cost (float)
  * retail_cost (float)
  * total_basket_cost (float)
  * lodging_txn_count (integer)
  * restaurant_txn_count (integer)
  * attraction_txn_count (integer)
  * retail_txn_count (integer)
  * lodging_txn_removed (integer)
  * restaurant_txn_removed (integer)
  * attraction_txn_removed (integer)
  * retail_txn_removed (integer)
  * hotel_property_count (integer)
  * meets_threshold (boolean)
  * created_at (timestamp)

**Data Type Conversions**:

```python
output_df = final_df.copy()
output_df['month_date'] = pd.to_datetime(config['target_month'])
output_df['created_at'] = pd.Timestamp.now()

# Ensure proper data types
integer_cols = [
    'lodging_txn_count', 'restaurant_txn_count', 'attraction_txn_count',
    'retail_txn_count', 'lodging_txn_removed', 'restaurant_txn_removed',
    'attraction_txn_removed', 'retail_txn_removed', 'hotel_property_count'
]
for col in integer_cols:
    output_df[col] = output_df[col].fillna(0).astype(int)
```

**Deliverable**: DataFrame ready for BigQuery upload

### 5.2 Write to BigQuery

**Task**: Upload results to BigQuery output table

* Create table if it doesn't exist (with proper schema)
* Use WRITE_APPEND disposition to add monthly data
* Handle duplicate month data (either WRITE_TRUNCATE for re-processing or manual deletion)
* Configure write options:
  * Schema auto-detection: False (use explicit schema)
  * Create disposition: CREATE_IF_NEEDED
  * Write disposition: WRITE_APPEND (or WRITE_TRUNCATE for testing)

**BigQuery Write Logic**:

```python
from google.cloud import bigquery

client = bigquery.Client(project=config['project_id'])
table_id = f"{config['output_dataset']}.{config['output_table']}"

# Define explicit schema
schema = [
    bigquery.SchemaField("county_fips", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("month_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("lodging_cost", "FLOAT"),
    bigquery.SchemaField("breakfast_lunch_cost", "FLOAT"),
    # ... (all fields)
]

job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
)

job = client.load_table_from_dataframe(output_df, table_id, job_config=job_config)
job.result()  # Wait for job to complete
```

**Error Handling**:

* Catch and log BigQuery API errors
* Validate successful write with row count verification
* Provide rollback guidance if write fails partially

**Deliverable**: Data successfully written to BigQuery

### 5.3 Post-Write Verification

**Task**: Verify data integrity after write

* Query BigQuery to confirm record count for target month
* Spot check random county values match DataFrame
* Verify no duplicate county_fips + month_date combinations
* Print success message with summary

**Verification Query**:

```sql
SELECT
  COUNT(*) as total_records,
  COUNT(DISTINCT county_fips) as unique_counties,
  SUM(CASE WHEN meets_threshold THEN 1 ELSE 0 END) as counties_meeting_threshold
FROM `[output_table]`
WHERE month_date = @target_month
```

**Deliverable**: Confirmation message with verification results


---

## Phase 6: Testing & Iteration

### 6.1 Single Month Test Run

**Task**: Test complete workflow with one month of data

* Select test month (e.g., January 2024)
* Run entire notebook end-to-end
* Review all validation reports
* Verify BigQuery output
* Document any issues or unexpected results

**Test Checklist**:

- [ ] Environment detection works in both Jupyter and Colab
- [ ] BigQuery authentication successful
- [ ] All SQL queries execute without errors
- [ ] Data extracts have expected row counts
- [ ] Outlier removal produces reasonable results
- [ ] All counties have required columns populated
- [ ] Quality thresholds flag appropriate counties
- [ ] Output schema matches specification
- [ ] BigQuery write completes successfully
- [ ] Post-write verification confirms accuracy

**Deliverable**: Documented test results with any issues noted

### 6.2 Threshold Tuning

**Task**: Analyze results to refine quality thresholds

* Review distribution of transaction counts across counties
* Identify counties just below/above threshold
* Assess impact of different threshold values on county inclusion
* Balance data quality vs. geographic coverage
* Update configuration with refined thresholds

**Analysis Questions**:

* What percentage of counties meet current thresholds?
* Are rural counties disproportionately excluded?
* Do threshold counties have reliable cost estimates?
* Are there seasonal patterns in data availability?

**Deliverable**: Updated threshold parameters with justification

### 6.3 MCC Code Refinement

**Task**: Validate and refine MCC code classifications

* Review sample transactions from each category
* Identify misclassified merchant types
* Add/remove MCC codes as needed for accuracy
* Document final MCC code lists for each category

**Categories to Validate**:

* **Restaurants**: Ensure all food service MCCs included
* **Attractions**: Separate entertainment/recreation from services
* **Retail**: Distinguish shopping from essential services

**Deliverable**: Finalized MCC code mapping for each spend category

### 6.4 Multi-Month Testing

**Task**: Process 3-6 consecutive months to test reusability

* Update only `target_month` parameter between runs
* Verify month-over-month consistency
* Check for seasonal patterns or anomalies
* Confirm no data accumulation or memory issues
* Test BigQuery append functionality (no duplicates)

**Test Months**:

* January 2024 (winter baseline)
* April 2024 (spring)
* July 2024 (peak summer travel)

**Deliverable**: Successfully processed multiple months with documented observations

### 6.5 Edge Case Testing

**Task**: Test handling of unusual scenarios

* **Month with holidays**: Does holiday spending skew results?
* **Counties with minimal data**: Are NaN values handled properly?
* **High-cost outlier counties**: Do major cities (NYC, SF) calculate correctly?
* **Rural counties**: Do low-volume counties meet thresholds appropriately?
* **DC FIPS mapping**: Verify Washington DC data joins correctly

**Deliverable**: Documented edge case results with any fixes applied

### 6.6 Documentation & Parameterization Guide

**Task**: Create user guide for running the notebook

* Document how to set configuration parameters
* Provide examples for different use cases:
  * Processing single month
  * Batch processing multiple months (loop structure)
  * Re-processing existing month with new thresholds
  * Adjusting basket composition (e.g., 3-night weekend)
* Include troubleshooting section for common issues
* Document MCC code lists in markdown cell

**Deliverable**: Comprehensive documentation within notebook


---

## Implementation Notes

### Monthly Processing Strategy

Since data volume requires processing one month at a time, structure the notebook for easy month iteration:

**Option 1: Manual Month Update**

* User updates `target_month` parameter
* Runs entire notebook
* Good for initial development and ad-hoc analysis

**Option 2: Loop for Batch Processing**

* Create optional loop structure in final notebook cell
* Iterate through list of months
* Useful for historical backfill (2022-01 through present)
* Example:

```python
months_to_process = pd.date_range('2022-01-01', '2024-12-01', freq='MS')
for month in months_to_process:
    config['target_month'] = month.strftime('%Y-%m-01')
    # Run processing functions...
    print(f"Completed {month.strftime('%Y-%m')}")
```

### Colab vs. Jupyter Considerations

**Colab-Specific**:

* OAuth authentication via google.colab.auth
* May have longer query timeouts
* Better for one-off analysis
* Easy sharing with stakeholders

**Jupyter-Specific**:

* Service account or application default credentials
* Better for scheduled/repeated execution
* Local environment control
* Faster iterations during development

**Shared Requirements**:

* Both need BigQuery API enabled
* Both need appropriate IAM permissions on datasets
* Both should use same configuration structure

### Performance Optimization

Given large data volumes, consider these optimizations:


1. **Query Optimization**:
   * Use partitioning (ref_date) in WHERE clauses
   * Limit columns in SELECT to only those needed
   * Consider APPROX_QUANTILES if exact percentiles too expensive
2. **Memory Management**:
   * Process categories separately if memory constrained
   * Use chunking for very large transaction tables
   * Clear intermediate DataFrames when no longer needed
3. **Caching**:
   * Cache county reference data (static)
   * Cache MCC code mappings (static)
   * Don't re-query configuration tables

### Data Quality Monitoring

Track these metrics across months to identify data issues:

* Counties with declining property counts (market changes vs. data issues)
* Sudden cost spikes/drops (market trends vs. outliers)
* Missing category data (coverage gaps)
* Transaction volume trends (seasonality vs. data delivery issues)

Consider creating a separate monitoring dashboard or report after multiple months are processed.


---

## Success Criteria

The Cost of Travel Index implementation will be considered successful when:


1. **Functionality**: Notebook processes any month (2022-01 forward) successfully
2. **Accuracy**: Statistical calculations match methodology specification
3. **Quality**: Data quality thresholds appropriately filter unreliable estimates
4. **Usability**: Parameters easily configurable without code changes
5. **Reliability**: Handles edge cases (missing data, DC FIPS, etc.) gracefully
6. **Compatibility**: Runs in both Jupyter and Colab environments
7. **Reproducibility**: Same input month produces identical output
8. **Documentation**: Users can run notebook with minimal guidance


---

## Next Steps After V0.1.1

Future enhancements beyond initial scope:


1. **Indexing**: Calculate indexed values relative to baseline period
2. **Aggregation**: State/regional/national rollups
3. **Cohort Analysis**: Group counties by characteristics for comparison
4. **Visualization**: Dashboard for trend analysis
5. **Automation**: Scheduled pipeline for monthly updates
6. **API**: Expose index data via API for applications
7. **Forecasting**: Predict future cost trends


---

## Appendix: Required Information Still Needed

To complete implementation, the following information is required:


1. **Lodging Dataset Details**:
   * Full table URI
   * Schema (field names and types)
   * Partitioning/clustering details
   * ADR calculation methodology
   * Property ID field name
   * Date field name(s)
2. **DC FIPS Codes**:
   * Standard FIPS code: 11001 (Washington DC)
   * Verify representation in lodging dataset (may be missing or different)
   * Confirm if manual mapping needed between datasets
3. **Output Table Configuration**:
   * Target BigQuery project ID
   * Target dataset name
   * Target table name
   * Partitioning requirements (if any)
4. **Quality Threshold Guidance**:
   * Business requirements for minimum data quality
   * Acceptable county exclusion rate
   * Seasonal considerations for thresholds

## Appendix: MCC Code Reference

The following MCC codes are configured for the Cost of Travel Index:

### Restaurant Category

* **5812**: Full-Service Restaurants

### Attraction Category (12 codes)

* **7996**: Amusement Parks, Carnivals, Circuses, Fortune Tellers
* **7995**: Betting/Casino Gambling
* **7998**: Aquariums, Seaquariums, Dolphinariums
* **7991**: Tourist Attractions and Exhibits
* **7933**: Bowling Alleys
* **7832**: Motion Picture Theaters
* **7911**: Dance Halls, Studios, and Schools
* **7929**: Bands, Orchestras, and Miscellaneous Entertainers
* **7922**: Theatrical Producers (Except Motion Pictures) and Ticket Agencies
* **7932**: Billiard and Pool Establishments
* **7994**: Video Game Arcades/Establishments
* **7999**: Recreation Services (Not Elsewhere Classified)

### Retail Category (38 codes)

* **5971**: Art Dealers and Galleries
* **5977**: Cosmetic Stores
* **7230**: Beauty and Barber Shops
* **5942**: Book Stores
* **5994**: News Dealers and Newsstands
* **5611**: Men's and Boys' Clothing and Accessory Stores
* **5621**: Women's Ready-to-Wear Stores
* **5631**: Women's Accessory and Specialty Shops
* **5641**: Children's and Infants' Wear Stores
* **5651**: Family Clothing Stores
* **5681**: Furriers and Fur Shops
* **5691**: Men's and Women's Clothing Stores
* **5699**: Miscellaneous Apparel and Accessory Shops
* **5311**: Department Stores
* **5992**: Florists
* **5993**: Cigar Stores and Stands
* **5309**: Duty Free Stores
* **5310**: Discount Stores
* **5931**: Used Merchandise and Secondhand Stores
* **5943**: Stationery Stores, Office and School Supply Stores
* **5947**: Gift, Card, Novelty, and Souvenir Shops
* **5950**: Glassware/Crystal Stores
* **5995**: Pet Shops, Pet Food, and Supplies Stores
* **5999**: Miscellaneous and Specialty Retail Stores
* **5944**: Jewelry Stores, Watches, Clocks, and Silverware Stores
* **5948**: Luggage and Leather Goods Stores
* **5661**: Shoe Stores
* **5655**: Sports and Riding Apparel Stores
* **5832**: Antique Shops
* **5932**: Antique Shops (duplicate category)
* **5937**: Antique Reproductions
* **5940**: Bicycle Shops
* **5941**: Sporting Goods Stores
* **5945**: Hobby, Toy, and Game Shops
* **5946**: Camera and Photographic Supply Stores
* **5949**: Sewing, Needlework, Fabric, and Piece Goods Stores
* **5970**: Artist's Supply and Craft Shops
* **5972**: Stamp and Coin Stores

These lists are configurable in the notebook parameters to allow for refinement based on analysis results.


---

**Document Version**: 1.0
**Date**: 2025-11-13
**Status**: Ready for Implementation