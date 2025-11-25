"""
============================================================================
ATTRACTIONS: State-Level Cost Calculation (PySpark Version)
============================================================================
Purpose: Calculate median per-visit attraction costs at the state level using Spark DataFrames

Aggregation: None - individual transactions analyzed
Outlier Removal: P5/P98 per state
Date Range: Configurable (default 2-week test period)

MCC Codes: 7996, 7995, 7998, 7991, 7933, 7832, 7911, 7929, 7922, 7932, 7994, 7999
Categories: Amusement parks, aquariums, recreation services, bowling alleys,
            billiards, dance halls, bands/orchestras, theatrical producers,
            golf courses, public golf courses, recreation services
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

# Attraction MCC codes
ATTRACTION_MCC_CODES = [
    '7996',  # Amusement Parks, Carnivals, Circuses, Fortune Tellers
    '7995',  # Betting/Casino Gambling
    '7998',  # Aquariums, Seaquariums, Dolphinariums
    '7991',  # Tourist Attractions and Exhibits
    '7933',  # Bowling Alleys
    '7832',  # Motion Picture Theaters
    '7911',  # Dance Halls, Studios, and Schools
    '7929',  # Bands, Orchestras, and Miscellaneous Entertainers
    '7922',  # Theatrical Producers (Except Motion Pictures), Ticket Agencies
    '7932',  # Billiard and Pool Establishments
    '7994',  # Video Game Arcades/Establishments
    '7999'   # Recreation Services (Not Elsewhere Classified)
]

# Initialize Spark session (if not already initialized)
# spark = SparkSession.builder.appName("AttractionsStateCost").getOrCreate()

# Read data from GCS buckets
merchant_df = spark.read.parquet(MERCHANT_TOURISM_PATH)
transaction_df = spark.read.parquet(TRANSACTION_TOURISM_PATH)

# Filter and join to get attraction transactions
attraction_transactions = (
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
            (F.col("mcc").isin(ATTRACTION_MCC_CODES))
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
    attraction_transactions
    .groupBy("merch_state", "month_date")
    .agg(
        F.expr("percentile_approx(trans_amount, 0.05)").alias("p5"),
        F.expr("percentile_approx(trans_amount, 0.98)").alias("p98")
    )
)

# Filter out outliers (transactions below P5 or above P98)
attractions_no_outliers = (
    attraction_transactions
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

# Calculate median attraction cost with data quality flags
attraction_state_results = (
    attractions_no_outliers
    .groupBy("merch_state", "month_date")
    .agg(
        F.expr("percentile_approx(trans_amount, 0.50)").alias("attraction_cost"),
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
attraction_state_results.show(50, truncate=False)

# Export to CSV in GCS bucket with date range in filename
output_filename = f"gs://cost_of_travel_index_staging/results/attraction_state_results_{START_DATE.strftime('%Y%m%d')}_{END_DATE.strftime('%Y%m%d')}.csv"
attraction_state_results.toPandas().to_csv(output_filename, index=False)
