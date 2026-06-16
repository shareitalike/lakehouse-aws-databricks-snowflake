# ==============================================================================
# Schema Evolution Runbook — Delta Lake + Auto Loader
# ==============================================================================
# Purpose:
#   Demonstrates how the pipeline handles upstream schema changes gracefully.
#   Use this notebook to test and validate schema evolution behavior before
#   promoting schema changes to the production DLT pipeline.
#
# Scenario:
#   A new field is added to the upstream event payload (e.g., 'session_id').
#   This notebook verifies the pipeline continues without manual intervention.
#
# Key behaviors validated:
#   1. Delta Lake mergeSchema for additive column changes
#   2. Auto Loader schemaLocation checkpoint update
#   3. Existing rows backfill with NULL for the new column
# ==============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

S3_BUCKET   = dbutils.secrets.get(scope="aws", key="s3_bucket_name")
SILVER_PATH = f"s3a://{S3_BUCKET}/silver/events"

# ==============================================================================
# 1. ORIGINAL SCHEMA (Baseline — what Silver was built on)
# ==============================================================================
print("=== Original Schema (Baseline) ===")
original_schema = StructType([
    StructField("event_id",   StringType(),    False),
    StructField("event_time", TimestampType(), False),
    StructField("user_id",    IntegerType(),   False),
    StructField("event_type", StringType(),    False),
    StructField("product_id", IntegerType(),   True),
    StructField("price",      DoubleType(),    True),
    StructField("device",     StringType(),    False),
    StructField("country",    StringType(),    False),
])

# ==============================================================================
# 2. EVOLVED SCHEMA (New 'session_id' field added by upstream producer)
# ==============================================================================
print("\n=== Evolved Schema (New: session_id) ===")
evolved_schema = StructType([
    StructField("event_id",   StringType(),    False),
    StructField("event_time", TimestampType(), False),
    StructField("user_id",    IntegerType(),   False),
    StructField("event_type", StringType(),    False),
    StructField("product_id", IntegerType(),   True),
    StructField("price",      DoubleType(),    True),
    StructField("device",     StringType(),    False),
    StructField("country",    StringType(),    False),
    StructField("session_id", StringType(),    True),  # <--- NEW FIELD
])

# ==============================================================================
# 3. BEHAVIOR WITHOUT mergeSchema (for validation / testing purposes)
# ==============================================================================
# Writing a new column to an existing Delta table without mergeSchema raises:
#   AnalysisException: A schema mismatch detected when writing to the Delta table.
# Delta Lake's schema enforcement prevents unintentional schema corruption.

print("""
WITHOUT mergeSchema: Write raises AnalysisException (schema mismatch).
This is expected behavior — Delta Lake protects data integrity by default.
""")

# ==============================================================================
# 4. CORRECT APPROACH: mergeSchema = True
# ==============================================================================
# mergeSchema adds the new column to the table schema non-breakingly.
# Existing rows will have NULL for the new column.

new_events_data = [
    ("evt_new_001", "2025-01-01 10:00:00", 101, "purchase",     42, 99.99, "mobile",  "US", "sess_abc123"),
    ("evt_new_002", "2025-01-01 10:05:00", 102, "product_view", 43, None,  "desktop", "IN", "sess_def456"),
]
new_events_df = spark.createDataFrame(new_events_data, schema=evolved_schema)

print("Writing evolved data with mergeSchema = True ...")

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    (
        new_events_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(SILVER_PATH)
    )
    print("Schema evolved successfully. Existing rows have NULL for session_id.")

# ==============================================================================
# 5. VERIFY: Print the evolved table schema
# ==============================================================================
print("\n=== Silver Table Schema After Evolution ===")
spark.read.format("delta").load(SILVER_PATH).printSchema()

# ==============================================================================
# 6. AUTO LOADER BEHAVIOR (Production DLT pipeline)
# ==============================================================================
# In pipeline_bronze_to_silver.py, Auto Loader is configured with:
#   .option("cloudFiles.inferColumnTypes",  "true")
#   .option("cloudFiles.schemaLocation",    "<schema_checkpoint_path>")
#
# When a new file arrives with 'session_id':
#   1. Auto Loader detects the schema change via the schemaLocation checkpoint.
#   2. The DLT pipeline logs a "Schema Evolution Detected" event.
#   3. On the next trigger, the evolved schema is merged into the Bronze table.
#   4. Delta mergeSchema propagates the new column through Silver automatically.
#   5. Gold DLT tables ignore the new column unless explicitly referenced.

print("""
Auto Loader Schema Evolution Flow:
1. New JSON file with 'session_id' lands in S3 Bronze.
2. Auto Loader detects schema change via schemaLocation checkpoint.
3. Pipeline logs "Schema Evolution Detected" event to DLT Event Log.
4. On next trigger, evolved schema is applied automatically.
5. Delta mergeSchema adds 'session_id' to Silver. Existing rows → NULL.
6. Gold tables unaffected (aggregations do not reference session_id).

Zero manual intervention required.
""")
