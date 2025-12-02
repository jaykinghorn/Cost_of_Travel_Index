"""
============================================================================
ACCOMMODATIONS: State-Level Cost Calculation (PySpark Version)
============================================================================
Purpose: Calculate median accommodation costs at the state level using Spark DataFrames

Aggregation: None - individual transactions analyzed
Minimum Threshold: $50 per transaction (filters out incidental charges like
                   parking fees, minibar, resort fees charged separately)
Outlier Removal: P5/P98 per state
Date Range: Configurable (default 2-week test period)

Rationale: Using individual transactions with $50 minimum threshold provides
           a representative sample of accommodation bookings while filtering
           out small incidental charges. This simpler approach avoids the
           complexity of cardholder aggregation while still capturing typical
           hotel stay costs.

MCC Codes: 3501-3838 (hotel chains), 7011-7012 (hotels/timeshares)
           300+ codes covering various hotel chains, resorts, and accommodations
============================================================================
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from datetime import date

# CONFIGURATION: Update these values as needed
START_DATE = date(2025, 7, 1)
END_DATE = date(2025, 7, 31)
DISTANCE_THRESHOLD = 60  # Miles for visitor classification
MIN_TRANSACTION_AMOUNT = 50.0  # Minimum to qualify as accommodation booking

# DATA QUALITY THRESHOLDS (95% CI, 5% MOE)
MIN_SAMPLE_EXCLUDE = 600  # Exclude geography if below this
MIN_SAMPLE_ROLLING = 2000  # Use 3-month rolling if below this

# GCS bucket URIs
MERCHANT_TOURISM_PATH = "gs://spend-codecs-prod/enrichment/merchant_tourism"
TRANSACTION_TOURISM_PATH = "gs://spend-codecs-prod/enrichment/transaction_tourism"

# Initialize Spark session (if not already initialized)
# spark = SparkSession.builder.appName("AccommodationsStateCost").getOrCreate()

# Read data from GCS buckets
merchant_df = spark.read.parquet(MERCHANT_TOURISM_PATH)
transaction_df = spark.read.parquet(TRANSACTION_TOURISM_PATH)

# Filter and join to get accommodation transactions
accommodation_transactions = (
    transaction_df
    .filter(
        (F.col("trans_date") >= F.lit(START_DATE)) &
        (F.col("trans_date") <= F.lit(END_DATE)) &
        (F.col("trans_distance") > DISTANCE_THRESHOLD) &
        (F.col("trans_amount") >= MIN_TRANSACTION_AMOUNT)
    )
    .join(
        merchant_df.filter(
            (F.col("merch_type") == 0) &  # Physical locations only
            (F.col("merch_country") == "US") &
            (
                (F.col("mcc").between("3501", "3838")) |
                (F.col("mcc").isin("7011", "7012"))
            )
        ),
        on=["mtid", "ref_date"],
        how="inner"
    )
    .select(
        F.col("merch_state"),
        F.trunc(F.col("trans_date"), "month").alias("month_date"),
        F.col("trans_amount"),
        F.col("membccid")
    )
)

# Calculate P5 and P98 thresholds per state per month for outlier removal
state_thresholds = (
    accommodation_transactions
    .groupBy("merch_state", "month_date")
    .agg(
        F.expr("percentile_approx(trans_amount, 0.05)").alias("p5"),
        F.expr("percentile_approx(trans_amount, 0.98)").alias("p98")
    )
)

# Filter out outliers (transactions below P5 or above P98)
accommodations_no_outliers = (
    accommodation_transactions
    .join(
        state_thresholds,
        on=["merch_state", "month_date"],
        how="inner"
    )
    .filter(
        (F.col("trans_amount") >= F.col("p5")) &
        (F.col("trans_amount") <= F.col("p98"))
    )
)

# Calculate median accommodation cost with data quality flags
accommodation_state_results = (
    accommodations_no_outliers
    .groupBy("merch_state", "month_date")
    .agg(
        F.expr("percentile_approx(trans_amount, 0.50)").alias("accommodation_cost"),
        F.count("*").alias("transaction_count"),
        F.countDistinct("membccid").alias("unique_visitors"),
        F.min("trans_amount").alias("min_cost"),
        F.max("trans_amount").alias("max_cost"),
        F.expr("percentile_approx(trans_amount, 0.25)").alias("q25"),
        F.expr("percentile_approx(trans_amount, 0.75)").alias("q75")
    )
    .withColumn(
        "data_quality_flag",
        F.when(F.col("unique_visitors") < MIN_SAMPLE_EXCLUDE, "EXCLUDE")
        .when(F.col("unique_visitors") < MIN_SAMPLE_ROLLING, "ROLLING_3MO")
        .otherwise("SINGLE_MONTH")
    )
    .withColumn("period_start", F.lit(START_DATE))
    .withColumn("period_end", F.lit(END_DATE))
    .withColumn("calculation_timestamp", F.current_timestamp())
    .orderBy(F.col("month_date"), F.col("transaction_count").desc())
)

# Display results
accommodation_state_results.show(50, truncate=False)

# Export to CSV in GCS bucket with date range in filename
output_filename = f"gs://cost_of_travel_index_staging/results/accommodation_state_results_{START_DATE.strftime('%Y%m%d')}_{END_DATE.strftime('%Y%m%d')}.csv"
accommodation_state_results.toPandas().to_csv(output_filename, index=False)
