from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, max, min, 
    datediff, current_date, date_format,
    when, coalesce, lit, dense_rank,
    row_number, lead, lag, months_between,
    collect_list, collect_set, size,
    first, last, stddev, variance
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
import datetime

# Initialize Spark Session with optimized configurations for large datasets
spark = SparkSession.builder \
    .appName("VVD_Campaign_Performance_Pipeline") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .config("spark.sql.shuffle.partitions", "400") \
    .getOrCreate()

# Set up date parameters
analysis_end_date = current_date()
analysis_start_date = date_sub(analysis_end_date, 548)  # 18 months

# Load main tables with proper schema definition
# Define schema for better performance
campaign_schema = StructType([
    StructField("TACTIC_ID", StringType(), True),
    StructField("CLIENT_ID", StringType(), True),
    StructField("CAMPAIGN_ID", StringType(), True),
    StructField("CAMPAIGN_NAME", StringType(), True),
    StructField("DEPLOYMENT_DATE", DateType(), True),
    StructField("CHANNEL", StringType(), True),
    StructField("CREATIVE_ID", StringType(), True),
    StructField("SEGMENT", StringType(), True),
    StructField("OFFER_TYPE", StringType(), True),
    StructField("RESPONSE_FLAG", IntegerType(), True),
    StructField("RESPONSE_DATE", DateType(), True),
    StructField("CONVERSION_FLAG", IntegerType(), True),
    StructField("CONVERSION_DATE", DateType(), True),
    StructField("REVENUE", DoubleType(), True)
])

# Read campaign deployments data
df_deployments = spark.read \
    .option("header", "true") \
    .schema(campaign_schema) \
    .csv("path/to/vvd_campaign_deployments.csv") \
    .filter(col("DEPLOYMENT_DATE").between(analysis_start_date, analysis_end_date))

# Cache the main dataframe for reuse
df_deployments.cache()

# 1. Contact Frequency Analysis
# Calculate contact frequency per client
window_client = Window.partitionBy("CLIENT_ID").orderBy("DEPLOYMENT_DATE")
window_client_30d = Window.partitionBy("CLIENT_ID").orderBy("DEPLOYMENT_DATE").rangeBetween(-30*86400, 0)
window_client_60d = Window.partitionBy("CLIENT_ID").orderBy("DEPLOYMENT_DATE").rangeBetween(-60*86400, 0)
window_client_90d = Window.partitionBy("CLIENT_ID").orderBy("DEPLOYMENT_DATE").rangeBetween(-90*86400, 0)

df_contact_frequency = df_deployments.withColumn(
    "days_since_last_contact", 
    datediff(col("DEPLOYMENT_DATE"), lag("DEPLOYMENT_DATE", 1).over(window_client))
).withColumn(
    "contact_number", 
    row_number().over(window_client)
).withColumn(
    "contacts_last_30d",
    count("TACTIC_ID").over(window_client_30d) - 1  # Exclude current contact
).withColumn(
    "contacts_last_60d",
    count("TACTIC_ID").over(window_client_60d) - 1
).withColumn(
    "contacts_last_90d",
    count("TACTIC_ID").over(window_client_90d) - 1
)

# 2. Campaign Performance Metrics
# Aggregate at campaign level
df_campaign_performance = df_deployments.groupBy(
    "CAMPAIGN_ID", 
    "CAMPAIGN_NAME",
    "CHANNEL",
    "OFFER_TYPE"
).agg(
    count("TACTIC_ID").alias("total_deployments"),
    countDistinct("CLIENT_ID").alias("unique_clients"),
    sum("RESPONSE_FLAG").alias("total_responses"),
    sum("CONVERSION_FLAG").alias("total_conversions"),
    sum("REVENUE").alias("total_revenue"),
    avg("REVENUE").alias("avg_revenue_per_deployment"),
    
    # Response and conversion rates
    (sum("RESPONSE_FLAG") / count("TACTIC_ID") * 100).alias("response_rate"),
    (sum("CONVERSION_FLAG") / count("TACTIC_ID") * 100).alias("conversion_rate"),
    (sum("CONVERSION_FLAG") / sum("RESPONSE_FLAG") * 100).alias("response_to_conversion_rate"),
    
    # Time-based metrics
    avg(when(col("RESPONSE_FLAG") == 1, 
        datediff(col("RESPONSE_DATE"), col("DEPLOYMENT_DATE"))
    )).alias("avg_days_to_response"),
    
    avg(when(col("CONVERSION_FLAG") == 1, 
        datediff(col("CONVERSION_DATE"), col("DEPLOYMENT_DATE"))
    )).alias("avg_days_to_conversion")
)

# 3. Contact Frequency Impact Analysis
# Join frequency data with response/conversion
df_frequency_impact = df_contact_frequency.groupBy(
    "contacts_last_30d"
).agg(
    count("TACTIC_ID").alias("deployments_count"),
    avg("RESPONSE_FLAG").alias("avg_response_rate"),
    avg("CONVERSION_FLAG").alias("avg_conversion_rate"),
    avg("REVENUE").alias("avg_revenue"),
    stddev("REVENUE").alias("revenue_stddev")
).orderBy("contacts_last_30d")

# 4. Client Segmentation by Engagement
# Create engagement score
df_client_engagement = df_deployments.groupBy("CLIENT_ID").agg(
    count("TACTIC_ID").alias("total_contacts"),
    sum("RESPONSE_FLAG").alias("total_responses"),
    sum("CONVERSION_FLAG").alias("total_conversions"),
    sum("REVENUE").alias("total_revenue"),
    max("DEPLOYMENT_DATE").alias("last_contact_date"),
    min("DEPLOYMENT_DATE").alias("first_contact_date"),
    
    # Calculate engagement metrics
    (sum("RESPONSE_FLAG") / count("TACTIC_ID")).alias("response_rate"),
    (sum("CONVERSION_FLAG") / count("TACTIC_ID")).alias("conversion_rate")
).withColumn(
    "days_since_last_contact",
    datediff(current_date(), col("last_contact_date"))
).withColumn(
    "customer_lifetime_days",
    datediff(col("last_contact_date"), col("first_contact_date"))
).withColumn(
    "engagement_score",
    col("response_rate") * 0.3 + 
    col("conversion_rate") * 0.5 + 
    (col("total_revenue") / col("total_contacts")) * 0.2
)

# 5. Optimal Contact Frequency Analysis
# Find optimal frequency by channel
df_optimal_frequency = df_contact_frequency.join(
    df_deployments.select("TACTIC_ID", "CHANNEL", "RESPONSE_FLAG", "CONVERSION_FLAG", "REVENUE"),
    on="TACTIC_ID"
).groupBy(
    "CHANNEL",
    "contacts_last_30d"
).agg(
    count("TACTIC_ID").alias("sample_size"),
    avg("RESPONSE_FLAG").alias("response_rate"),
    avg("CONVERSION_FLAG").alias("conversion_rate"),
    avg("REVENUE").alias("avg_revenue"),
    stddev("REVENUE").alias("revenue_std")
).filter(col("sample_size") >= 100)  # Ensure statistical significance

# 6. Time Series Analysis
# Monthly performance trends
df_monthly_trends = df_deployments.withColumn(
    "year_month",
    date_format("DEPLOYMENT_DATE", "yyyy-MM")
).groupBy("year_month", "CHANNEL").agg(
    count("TACTIC_ID").alias("deployments"),
    countDistinct("CLIENT_ID").alias("unique_clients"),
    avg("RESPONSE_FLAG").alias("response_rate"),
    avg("CONVERSION_FLAG").alias("conversion_rate"),
    sum("REVENUE").alias("total_revenue")
).orderBy("year_month", "CHANNEL")

# 7. Create summary statistics
print("VVD Campaign Performance Analysis Summary")
print("=" * 50)
print(f"Analysis Period: 18 months")
print(f"Total Deployments: {df_deployments.count():,}")
print(f"Unique Clients: {df_deployments.select('CLIENT_ID').distinct().count():,}")
print(f"Total Campaigns: {df_deployments.select('CAMPAIGN_ID').distinct().count():,}")

# Save results
output_path = "path/to/output/"

# Save all analytical results
df_campaign_performance.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/campaign_performance")
df_frequency_impact.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/frequency_impact")
df_client_engagement.write.mode("overwrite").parquet(f"{output_path}/client_engagement")
df_optimal_frequency.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/optimal_frequency")
df_monthly_trends.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/monthly_trends")

# Create executive summary
executive_summary = f"""
VVD Campaign Performance Executive Summary
Generated: {datetime.datetime.now()}

Key Metrics:
- Total Deployments: {df_deployments.count():,}
- Unique Clients: {df_deployments.select('CLIENT_ID').distinct().count():,}
- Overall Response Rate: {df_deployments.agg(avg('RESPONSE_FLAG') * 100).collect()[0][0]:.2f}%
- Overall Conversion Rate: {df_deployments.agg(avg('CONVERSION_FLAG') * 100).collect()[0][0]:.2f}%
- Total Revenue: ${df_deployments.agg(sum('REVENUE')).collect()[0][0]:,.2f}

Top Performing Campaigns:
{df_campaign_performance.orderBy(col("conversion_rate").desc()).limit(5).toPandas().to_string()}

Optimal Contact Frequency:
{df_optimal_frequency.orderBy(col("avg_revenue").desc()).limit(10).toPandas().to_string()}
"""

# Save executive summary
with open(f"{output_path}/executive_summary.txt", "w") as f:
    f.write(executive_summary)

# Clean up
df_deployments.unpersist()
spark.stop()