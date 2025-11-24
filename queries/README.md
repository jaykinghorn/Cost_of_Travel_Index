# SQL Queries Directory

This directory contains BigQuery SQL queries for the Cost of Travel Index project.

## Query Files

### retail_daily_cap_test.sql
**Purpose**: Test implementation of retail spending calculation with daily transaction cap

**Branch**: `feature/retail-daily-cap-test`

**Description**:
Implements a modified retail spending calculation that caps transactions at 6 per cardholder per day before applying P5/P98 outlier removal. This approach prevents inflated median retail spending values caused by high-frequency shoppers.

**Key Features**:
- Ranks retail transactions by cardholder and date
- Caps at 6 highest-value transactions per cardholder per day
- Applies P5/P98 outlier removal after capping
- Calculates state and county-level retail spending medians
- Tracks capping impact metrics (transactions removed, cardholders affected)
- Outputs detailed comparison statistics

**Configuration Parameters** (declared at top of query):
```sql
month_start: '2024-07-01'
month_end: '2024-07-31'
distance_threshold: 60.0 miles
max_retail_txn_per_cardholder_per_day: 6
outlier_lower_percentile: 0.05 (P5)
outlier_upper_percentile: 0.98 (P98)
```

**Output Columns**:
- `state_abbr`, `county_fips`, `county_name`: Geographic identifiers
- `state_median_retail`, `county_median_retail`: Median transaction values
- `retail_basket_cost_2day`: Calculated 2-day basket cost
- `txn_before_cap`, `txn_after_cap`, `txn_capped`: Capping impact counts
- `pct_txn_capped`, `pct_cardholders_affected`: Capping impact percentages
- Transaction and cardholder counts at state/county level

**Bonus Queries** (commented out, uncomment to run):
- Summary statistics across all states
- Top 10 states by median retail spending
- States most affected by capping

**Usage**:
1. Adjust configuration parameters at top of query
2. Run main query for detailed results
3. Uncomment bonus queries for specific analyses
4. Export results for comparison with non-capped methodology

---

## Query Development Guidelines

### File Naming Convention
- Descriptive name indicating purpose: `[category]_[feature]_[type].sql`
- Examples: `retail_daily_cap_test.sql`, `restaurant_percentile_analysis.sql`

### Query Structure
1. **Header Comment Block**
   - Purpose and description
   - Branch (if test query)
   - Key features or modifications

2. **Configuration Section**
   - DECLARE statements for all parameters
   - Default values with comments

3. **Main Query Logic**
   - CTEs for step-by-step processing
   - Clear comments for each step
   - Descriptive CTE names

4. **Output Selection**
   - Final SELECT with all relevant metrics
   - Ordered results for readability

5. **Bonus Queries** (optional)
   - Additional analyses commented out
   - Can be run separately as needed

### Best Practices
- **Parameterization**: Use DECLARE for all configurable values
- **Readability**: Break complex logic into CTEs
- **Comments**: Explain non-obvious logic or business rules
- **Performance**: Note any optimization considerations
- **Testing**: Include sample parameter values that work

### Query Types

**1. Test Queries**
- Experimental approaches for methodology validation
- Compare with baseline/production queries
- Include impact analysis metrics
- Documented in corresponding test plan

**2. Production Queries**
- Validated and approved methodologies
- Used for regular index calculation
- Optimized for performance
- Thoroughly documented

**3. Analysis Queries**
- Ad-hoc exploration and validation
- Data quality checks
- Comparison and benchmarking
- May not be maintained long-term

**4. Utility Queries**
- Reference data extraction
- Schema documentation
- Helper queries for development

## Running Queries

### In BigQuery Console
1. Copy query to BigQuery editor
2. Adjust DECLARE parameters as needed
3. Verify project/dataset access
4. Run and export results

### In Jupyter/Colab Notebook
```python
from google.cloud import bigquery

client = bigquery.Client(project='your-project-id')

# Read query from file
with open('queries/retail_daily_cap_test.sql', 'r') as f:
    query = f.read()

# Run query
results = client.query(query).to_dataframe()
```

### Using bq Command Line
```bash
bq query --use_legacy_sql=false < queries/retail_daily_cap_test.sql
```

## Query Performance Notes

- **Partitioning**: Use `ref_date` partition filter in merchant_tourism joins
- **Approximation**: APPROX_QUANTILES used for percentiles (faster than exact)
- **Row Numbers**: Window functions can be expensive on large datasets
- **Aggregation Level**: State-level aggregation faster than county-level

## Version Control

- Keep queries in version control with code
- Test queries live in feature branches
- Production queries merged to main after validation
- Archive deprecated queries in `queries/archive/` folder

## Related Documentation

- [development_plan.md](../development_plan.md) - Technical implementation details
- [test_plans/retail_daily_cap_test.md](../test_plans/retail_daily_cap_test.md) - Test methodology
- [data_tables.md](../data_tables.md) - Source table schemas
