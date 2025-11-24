Version 0.1.1

# Cost of Travel Index

## **Project Goal**

The purpose of the Cost of Travel Index is to show micro and macro economic changes to the travel industry, which influence traveler decision-making, as well as destination marketing and positioning.

The Cost of Travel Index has its inspiration in the cost of living index that combines a common basket of goods and tracks that cost over time nationally and regionally. Our Cost of Travel Index follows a similar approach by mimicking expenditures for a three-day weekend in each destination across the United States and being able to aggregate this into state, region, and national trends over time.

Our goal is to help destinations understand how they are performing relative to their competitive or cohort set, understand their relative expense or value compared to similar averages, position themselves accordingly through marketing in the traveler's mind, and track these changes over time.


## **Methodology**

### Data Sources

* **Source**: Zartico's spend data (structured BigQuery tables)
* **Update Frequency**: Daily
* **Historical Coverage**: Monthly calculations from 2022-01-01 forward
* **Geographic Scope**:  city, county and state-level analysis across the United States

### Basket of Goods Calculation

The index represents a hypothetical three-day weekend experienced by a couple traveling and experiencing a destination. The basket of goods includes:





1. **Lodging**: median monthly transaction value at Hotels, Casino hotels, Bed and Breakfasts and other accommodations.
2. **Restaurant Meals**: 6 total meals
   * 4 meals at 35th percentile price (breakfast and lunch - typically cheaper)
   * 2 meals at 65th percentile price (dinner - typically most expensive)
3. **Attractions**: 2 days of average daily attraction spending
4. **Retail**: 2 days of average daily retail spending

### Statistical Approach

**State-Level Specificity**: All percentiles and medians are calculated within each state for each month to reflect regional market conditions.

**Transaction Analysis Approach**:

* **No Aggregation**: Individual transactions analyzed directly for restaurants, attractions, and retail
* **Accommodations**: Individual transactions with $50 minimum threshold to filter incidental charges (parking fees, minibar, etc.)
* **Visitor Classification**: Transactions where distance from home > 60 miles
* **Physical Locations Only**: Online/e-commerce transactions excluded (merch_type = 0)

**Outlier Removal**: P5/P98 percentile approach applied before calculating medians and percentiles:

* Remove transactions below 5th percentile (P5) per state
* Remove transactions above 98th percentile (P98) per state
* Rationale: Eliminates extreme outliers ($0.01 errors and $20,000 anomalies) while preserving legitimate high/low spending
* Applied consistently across all spending categories

**Percentile Calculations**:

* Restaurants: P35 (breakfast/lunch) and P65 (dinner) calculated from filtered transaction data
* Calculated after outlier removal to represent typical meal costs

**Median Calculations**:

* Accommodations, attractions, and retail use median (P50) after outlier removal
* More robust to remaining variance than mean

**Configuration**: All thresholds configurable via SQL DECLARE statements:

* Date ranges (default: full month)
* Distance threshold (default: 60 miles)
* Minimum transaction amounts (default: $50 for accommodations, $0 for others)
* Data quality thresholds (600/2000 samples)

### Data Quality Thresholds

States are evaluated using statistical sample size requirements to ensure reliable estimates:

**Minimum Sample Sizes** (95% confidence interval, 5% margin of error):

* **600 transactions**: Minimum threshold - states below this are flagged as EXCLUDE
* **2,000 transactions**: Rolling average threshold - states between 600-2,000 flagged as ROLLING_3MO
* **Above 2,000**: Sufficient for single-month estimates - flagged as SINGLE_MONTH

**Data Quality Flags**:

* `EXCLUDE`: Insufficient sample size (< 600 transactions) - exclude from index
* `ROLLING_3MO`: Limited sample (600-1,999 transactions) - use 3-month rolling average
* `SINGLE_MONTH`: Sufficient sample (≥ 2,000 transactions) - use single month data

**Applied Per Category**: Each spending category (accommodations, restaurants, attractions, retail) has independent quality flags, allowing partial index calculation when some categories meet thresholds while others don't.


## **Technical Implementation**

### Approach

* **Platform**: Jupyter/Colab Notebook with Python and SQL
* **Data Extraction**: SQL queries against BigQuery tables for monthly data gathering
* **Data Transformation**: Python pandas DataFrames for calculations and transformations
* **Data Storage**: Write completed index values back to BigQuery dataset for trending and retrieval

### Processing Workflow





1. SQL query to extract monthly national, region and state transaction data by month
2. Python-based outlier removal and statistical calculations
3. Calculate basket components per month for national, regional and state-level indices.
4. Apply data quality thresholds
5. Write results to output table in BigQuery

### Output Table Schema

**Granularity**: Multiple datbles One row per county per month

**Primary Key**: State Abbreviation, Region Name + Month date

**Columns**:

* `county_fips` (string): County FIPS code identifier (joins to county names table)
* `month_date` (date): YYYY-MM-DD format (e.g., 2024-01-01)
* `lodging_cost` (float): Median accommodations cost
* `breakfast_lunch_cost` (float): Cost for 4 meals at P35
* `dinner_cost` (float): Cost for 2 meals at P65
* `attraction_cost` (float): Cost for 2 days
* `retail_cost` (float): Cost for 1 day
* `total_basket_cost` (float): Sum of all component costs
* `lodging_txn_count` (integer): Number of lodging transactions (after outlier removal)
* `restaurant_txn_count` (integer): Number of restaurant transactions (after outlier removal)
* `attraction_txn_count` (integer): Number of attraction transactions (after outlier removal)
* `retail_txn_count` (integer): Number of retail transactions (after outlier removal)
* `lodging_txn_removed` (integer): Number of lodging transactions removed as outliers
* `restaurant_txn_removed` (integer): Number of restaurant transactions removed as outliers
* `attraction_txn_removed` (integer): Number of attraction transactions removed as outliers
* `retail_txn_removed` (integer): Number of retail transactions removed as outliers
* `hotel_property_count` (integer): Number of hotel properties reporting
* `meets_threshold` (boolean): Whether county meets minimum data quality thresholds
* `created_at` (timestamp): Record creation timestamp

**Index Format**: Raw dollar amounts (baseline indexing and year-over-year comparisons out of scope for initial version)


## **Output & Usage**

Produce a monthly Cost of Travel Index for each county in the United States that has sufficient data to meet quality thresholds. (City and State as fast-follow options)

This data can then be aggregated at the state, region, and national level by month to track changes in the travel industry over time.


**Out of Scope for V0.1.0**:

* Visualization dashboards or applications
* Automated scheduling/pipeline
* Cohort identification or competitive set analysis
* Indexed values relative to baseline periods




Notes

* Delaware meals are lower than expected. Katie Stadius flagged that they are reviewing the Delaware spend data.
* There is variance between the accommodations calculated by this script and the values from KeyData. Use Spending accommodations for now. Additional discovery later will be helpful to determine which to use long-term.
* Maryland retail is significantly lower than the other states \~$10 compared to $30




