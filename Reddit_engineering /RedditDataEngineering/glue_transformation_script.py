"""
AWS Glue ETL Script - Reddit Data Transformation
This script reads raw Reddit CSV from S3, transforms it, and saves to processed S3
Copy this code to AWS Glue Script Editor
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Data quality rules
DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount > 0,
    RowCount > 0,
    IsComplete "id",
    IsComplete "title",
    IsComplete "author",
    IsComplete "created_utc"
]
"""

# Read raw CSV from S3
print("Reading raw data from S3...")
raw_data = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": "\"", 
        "withHeader": True, 
        "separator": ",",
        "optimizePerformance": False
    }, 
    connection_type="s3", 
    format="csv", 
    connection_options={
        "paths": ["s3://ansh-reddit-raw/reddit_20231022.csv"]
    }, 
    transformation_ctx="raw_data"
)

# Convert to Spark DataFrame for transformations
df = raw_data.toDF()

print("Applying transformations...")

# Transformation 1: Add popularity category based on score
df = df.withColumn(
    "popularity_category",
    when(col("score") >= 1000, "viral")
    .when(col("score") >= 100, "popular")
    .when(col("score") >= 10, "normal")
    .otherwise("low_engagement")
)

# Transformation 2: Add engagement rate (comments per upvote)
df = df.withColumn(
    "engagement_rate",
    when(col("score") > 0, round(col("num_comments") / col("score"), 4))
    .otherwise(0)
)

# Transformation 3: Extract date components for analytics
df = df.withColumn("post_date", to_date(col("created_utc")))
df = df.withColumn("post_year", year(col("created_utc")))
df = df.withColumn("post_month", month(col("created_utc")))
df = df.withColumn("post_day", dayofmonth(col("created_utc")))
df = df.withColumn("post_hour", hour(col("created_utc")))
df = df.withColumn("post_day_of_week", dayofweek(col("created_utc")))

# Transformation 4: Categorize by content type
df = df.withColumn(
    "content_type",
    when(col("url").contains("i.redd.it") | col("url").contains("imgur"), "image")
    .when(col("url").contains("v.redd.it") | col("url").contains("youtube"), "video")
    .when(col("url").contains("reddit.com/r/"), "text_post")
    .otherwise("external_link")
)

# Transformation 5: Add flags for special posts
df = df.withColumn("is_highly_commented", col("num_comments") > 50)
df = df.withColumn("is_controversial", (col("score") < 100) & (col("num_comments") > 50))
df = df.withColumn("is_viral", col("score") > 1000)

# Transformation 6: Clean author field (remove deleted/suspended)
df = df.withColumn(
    "author_status",
    when(col("author").isin(["[deleted]", "[removed]", "AutoModerator"]), "system")
    .otherwise("active")
)

# Transformation 7: Title length analysis
df = df.withColumn("title_length", length(col("title")))
df = df.withColumn(
    "title_length_category",
    when(col("title_length") < 50, "short")
    .when(col("title_length") < 100, "medium")
    .otherwise("long")
)

# Transformation 8: Add processing metadata
df = df.withColumn("processed_timestamp", current_timestamp())
df = df.withColumn("data_source", lit("reddit_api"))

# Select and reorder columns for final output
final_df = df.select(
    # Original fields
    "id",
    "title",
    "title_length",
    "title_length_category",
    "score",
    "num_comments",
    "author",
    "author_status",
    "created_utc",
    "post_date",
    "post_year",
    "post_month",
    "post_day",
    "post_hour",
    "post_day_of_week",
    "url",
    "content_type",
    "over_18",
    "edited",
    "spoiler",
    "stickied",
    # Calculated fields
    "popularity_category",
    "engagement_rate",
    "is_highly_commented",
    "is_controversial",
    "is_viral",
    # Metadata
    "processed_timestamp",
    "data_source"
)

# Convert back to DynamicFrame
transformed_data = DynamicFrame.fromDF(final_df, glueContext, "transformed_data")

# Evaluate data quality
print("Evaluating data quality...")
EvaluateDataQuality().process_rows(
    frame=transformed_data, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_transformed", 
        "enableDataQualityResultsPublishing": True
    }, 
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT", 
        "observations.scope": "ALL"
    }
)

# Write transformed data to processed S3 bucket
# Partitioned by year and month for efficient querying
print("Writing transformed data to S3...")
glueContext.write_dynamic_frame.from_options(
    frame=transformed_data, 
    connection_type="s3", 
    format="parquet",  # Changed to parquet for better performance
    connection_options={
        "path": "s3://ansh-reddit-processed", 
        "compression": "snappy",
        "partitionKeys": ["post_year", "post_month"]  # Partition by year and month
    }, 
    transformation_ctx="processed_data"
)

print("Transformation complete!")
job.commit()
