"""
============================================================================
RESTAURANTS: State-Level Cost Calculation (PySpark Version)
============================================================================
Purpose: Calculate median per-visit restaurant costs at the state level using Spark DataFrames

Aggregation: None - individual transactions analyzed
Outlier Removal: P5/P98 per state
Date Range: Configurable (default 2-week test period)

Rationale: Using individual restaurant transactions provides a representative
           sample of typical dining costs without over-aggregating daily
           spending patterns.

MCC Codes: 5812, 5814 (Eating Places and Fast Food Restaurants)
============================================================================
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from datetime import date

# CONFIGURATION: Update these values as needed
START_DATE = date(2025, 7, 1)
END_DATE = date(2025, 7, 31)
DISTANCE_THRESHOLD = 60  # Miles for visitor classification

# DATA QUALITY THRESHOLDS (95% CI, 5% MOE)
MIN_SAMPLE_EXCLUDE = 600  # Exclude geography if below this
MIN_SAMPLE_ROLLING = 2000  # Use 3-month rolling if below this

# GCS bucket URIs
MERCHANT_TOURISM_PATH = "gs://spend-codecs-prod/enrichment/merchant_tourism"
TRANSACTION_TOURISM_PATH = "gs://spend-codecs-prod/enrichment/transaction_tourism"

# Restaurant MCC codes
RESTAURANT_MCC_CODES = ['5812', '5814']

# Initialize Spark session (if not already initialized)
# spark = SparkSession.builder.appName("RestaurantsStateCost").getOrCreate()

# Read data from GCS buckets
merchant_df = spark.read.parquet(MERCHANT_TOURISM_PATH)
transaction_df = spark.read.parquet(TRANSACTION_TOURISM_PATH)

# Filter and join to get restaurant transactions
restaurant_transactions = (
    transaction_df
    .filter(
        (F.col("trans_date") >= F.lit(START_DATE)) &
        (F.col("trans_date") <= F.lit(END_DATE)) &
        (F.col("trans_distance") > DISTANCE_THRESHOLD)
    )
    .join(
        merchant_df.filter(
            (F.col("merch_type") == 0) &  # Physical locations only
            (F.col("merch_country") == "US") &
            (F.col("mcc").isin(RESTAURANT_MCC_CODES))
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
    restaurant_transactions
    .groupBy("merch_state", "month_date")
    .agg(
        F.expr("percentile_approx(trans_amount, 0.05)").alias("p5"),
        F.expr("percentile_approx(trans_amount, 0.98)").alias("p98")
    )
)

# Filter out outliers (transactions below P5 or above P98)
restaurants_no_outliers = (
    restaurant_transactions
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

# Calculate median restaurant cost with data quality flags
restaurant_state_results = (
    restaurants_no_outliers
    .groupBy("merch_state", "month_date")
    .agg(
        F.expr("percentile_approx(trans_amount, 0.50)").alias("restaurant_cost"),
        F.count("*").alias("transaction_count"),
        F.countDistinct("membccid").alias("unique_visitors")
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
restaurant_state_results.show(50, truncate=False)

# Export to CSV in GCS bucket with date range in filename
output_filename = f"gs://cost_of_travel_index_staging/results/restaurant_state_results_{START_DATE.strftime('%Y%m%d')}_{END_DATE.strftime('%Y%m%d')}.csv"
restaurant_state_results.toPandas().to_csv(output_filename, index=False)
