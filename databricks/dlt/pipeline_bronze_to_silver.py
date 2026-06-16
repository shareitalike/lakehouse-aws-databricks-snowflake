# ==============================================================================
# Delta Live Tables: Bronze → Silver Pipeline
# ==============================================================================
# Purpose:
#   Ingests raw JSON events from S3 using Auto Loader and writes them to the
#   Bronze DLT table. Applies data quality expectations and CDC deduplication
#   to produce a clean Silver table, partitioned by year/month/day.
#
# Bronze: Auto Loader streaming read from s3://<bucket>/bronze/events/
# Silver: CDC-merged (apply_changes on event_id), partitioned Delta table
# ==============================================================================

import dlt
from pyspark.sql import functions as F

# --- Configuration (Pulled dynamically in DLT) ---
# In a real DLT pipeline, paths are often parameterized or managed by Unity Catalog Volumes
# For this project, we map to our S3 bucket.
S3_BUCKET = spark.conf.get("pipeline.s3_bucket", "retailedge-analytics-prod")
BRONZE_PATH = f"s3a://{S3_BUCKET}/bronze/events/"

# ==============================================================================
# 1. BRONZE LAYER (Ingestion)
# ==============================================================================
@dlt.table(
    name="events_bronze",
    comment="Raw streaming events from S3 using Auto Loader",
    table_properties={"quality": "bronze"}
)
def events_bronze():
    """
    Auto Loader uses AWS SQS/SNS event notifications to discover new files
    in S3 without scanning the full directory — efficient at any scale.
    cloudFiles.inferColumnTypes detects column types automatically.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true") # Auto schema inference
        .load(BRONZE_PATH)
    )

# ==============================================================================
# 2. SILVER LAYER (Transformations & Data Quality)
# ==============================================================================
@dlt.view(
    name="events_silver_cleaned",
    comment="Cleaned events with parsed dates and enforced data quality"
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
def events_silver_cleaned():
    """
    DLT Views are temporary — they exist only within the pipeline execution context
    and are not materialized as tables in Unity Catalog.
    Expectations drop rows failing the condition and log failure counts
    to the DLT Event Log for observability.
    """
    return (
        dlt.read_stream("events_bronze")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
        .withColumn("year", F.date_format("event_time", "yyyy"))
        .withColumn("month", F.date_format("event_time", "MM"))
        .withColumn("day", F.date_format("event_time", "dd"))
    )

# ==============================================================================
# 3. SILVER LAYER (CDC & Deduplication)
# ==============================================================================
# Define the target table structure. Unity Catalog will place this in retailedge.silver.events_silver
dlt.create_streaming_table(
    name="events_silver",
    comment="Final Silver table deduplicated by event_id",
    table_properties={"quality": "silver"},
    partition_cols=["year", "month", "day"]
)

# Apply Changes (CDC)
# apply_changes handles deduplication via MERGE logic internally.
# If a duplicate event_id arrives, the row with the latest ingestion_timestamp is kept.
dlt.apply_changes(
    target="events_silver",           
    source="events_silver_cleaned",   
    keys=["event_id"],                
    sequence_by="ingestion_timestamp",
    apply_as_deletes=None,
    except_column_list=[]
)
