Version 0.1.1

# Cost of Travel Index

## **Project Goal**

The purpose of the Cost of Travel Index is to show micro and macro economic changes to the travel industry, which influence traveler decision-making, as well as destination marketing and positioning.

The Cost of Travel Index has its inspiration in the cost of living index that combines a common basket of goods and tracks that cost over time nationally and regionally. Our Cost of Travel Index follows a similar approach by mimicking expenditures for a three-day weekend in each destination across the United States and being able to aggregate this into state, region, and national trends over time.

Our goal is to help destinations understand how they are performing relative to their competitive or cohort set, understand their relative expense or value compared to similar averages, position themselves accordingly through marketing in the traveler's mind, and track these changes over time.


## **Methodology**

### Data Sources

* **Source**: Zartico's spend and lodging datasets (structured BigQuery tables)
* **Update Frequency**: Daily
* **Historical Coverage**: Monthly calculations from 2022-01-01 forward
* **Geographic Scope**: County-level analysis across the United States

### Basket of Goods Calculation

The index represents a hypothetical three-day weekend experienced by a couple traveling and experiencing a destination. The basket of goods includes:


1. **Lodging**: 2 hotel nights at Friday/Saturday average ADR
   * Calculated from all Friday and Saturday nights within the month
   * Averaged across both nights to capture weekend rates
2. **Restaurant Meals**: 6 total meals
   * 4 meals at 35th percentile price (breakfast and lunch - typically cheaper)
   * 2 meals at 65th percentile price (dinner - typically most expensive)
3. **Attractions**: 2 days of average daily attraction spending
4. **Retail**: 1 day of average daily retail spending

### Statistical Approach

**County-Level Specificity**: All percentiles and medians are calculated within each county for each month to reflect local market conditions (e.g., Miami vs. rural Kansas).

**Outlier Removal**: Hybrid percentile approach applied before calculating medians and percentiles:

* Remove transactions below 5th percentile (P5)
* Remove transactions above 98th percentile (P98)
* Rationale: Eliminates extreme outliers ($0.01 errors and $20,000 anomalies) while preserving legitimate high/low spending

**Percentile Calculations**:

* 35th and 65th percentiles for restaurant meals calculated from filtered transaction data per county per month

**Median Calculations**:

* Attraction and retail spending use median values after outlier removal
* More robust to remaining variance than mean

**Configuration**: Percentile thresholds and basket quantities are configurable parameters in the notebook to allow methodology fine-tuning.

### Data Quality Thresholds

Counties must meet statistical thresholds to have a Cost of Travel Index calculated:

* Minimum number of hotel properties reporting
* Minimum transaction counts per category
* Specific thresholds TBD during exploratory data analysis

Counties not meeting thresholds will be excluded from monthly calculations.


## **Technical Implementation**

### Approach

* **Platform**: Jupyter/Colab Notebook with Python and SQL
* **Data Extraction**: SQL queries against BigQuery tables for monthly data gathering
* **Data Transformation**: Python pandas DataFrames for calculations and transformations
* **Data Storage**: Write completed index values back to BigQuery dataset for trending and retrieval

### Processing Workflow


1. SQL query to extract monthly transaction data by county
2. Python-based outlier removal and statistical calculations
3. Calculate basket components per county
4. Apply data quality thresholds
5. Write results to output table in BigQuery

### Output Table Schema

**Granularity**: One row per county per month

**Primary Key**: County FIPS code + Month date

**Columns**:

* `county_fips` (string): County FIPS code identifier (joins to county names table)
* `month_date` (date): YYYY-MM-DD format (e.g., 2024-01-01)
* `lodging_cost` (float): Cost for 2 nights
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

Produce a monthly Cost of Travel Index for each county in the United States that has sufficient data to meet quality thresholds.

This data can then be aggregated at the state, region, and national level by month to track changes in the travel industry over time.


Technical Note: Washington DC has a county FIPS code in our spend data, but not in our KeyData Lodging data. We will need to manually add the DC. FIPS code to join the two data sets together. 


**Out of Scope for V0.1.0**:

* Visualization dashboards or applications
* Automated scheduling/pipeline
* Cohort identification or competitive set analysis
* Indexed values relative to baseline periods








