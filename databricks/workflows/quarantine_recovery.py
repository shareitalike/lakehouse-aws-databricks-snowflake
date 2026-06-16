# ==============================================================================
# Quarantine Recovery Layer — Dead Letter Queue (DLQ) Processor
# ==============================================================================
# Purpose:
#   Processes events rejected by Lambda into the S3 Quarantine bucket.
#   Classifies each record as repairable or unrecoverable, repairs fixable
#   records, and routes them back to S3 Bronze for DLT Auto Loader reprocessing.
#   Truly corrupt records are written to a Dead Letter Delta table.
#
# Architecture Flow:
#   S3 Quarantine Path → [This Notebook] → S3 Bronze (recovered)
#                                        → Dead Letter Delta Table (unrecoverable)
#
# Inputs:  s3://<bucket>/quarantine/events  (NDJSON, written by Lambda)
# Outputs: s3://<bucket>/bronze/events      (recovered events)
#          s3://<bucket>/dead_letter/events (unrecoverable events, Delta format)
# ==============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
S3_BUCKET        = dbutils.secrets.get(scope="aws", key="s3_bucket_name")
QUARANTINE_PATH  = f"s3a://{S3_BUCKET}/quarantine/events"
BRONZE_PATH      = f"s3a://{S3_BUCKET}/bronze/events"
DEAD_LETTER_PATH = f"s3a://{S3_BUCKET}/dead_letter/events"

# Explicit schema — Lambda writes the original record + rejection_reason field.
QUARANTINE_SCHEMA = StructType([
    StructField("event_id",           StringType(),    True),
    StructField("event_time",         StringType(),    True),
    StructField("user_id",            IntegerType(),   True),
    StructField("event_type",         StringType(),    True),
    StructField("product_id",         IntegerType(),   True),
    StructField("price",              DoubleType(),    True),
    StructField("device",             StringType(),    True),
    StructField("country",            StringType(),    True),
    StructField("ingestion_timestamp",StringType(),    True),
    StructField("processing_date",    StringType(),    True),
    StructField("rejection_reason",   StringType(),    True),
])

# ==============================================================================
# 2. READ QUARANTINE RECORDS
# ==============================================================================
print("Reading quarantine events from S3...")

df_quarantine = (
    spark.read
    .schema(QUARANTINE_SCHEMA)
    .json(QUARANTINE_PATH)
)

quarantine_count = df_quarantine.count()
print(f"Total quarantined records: {quarantine_count}")

if quarantine_count == 0:
    print("No quarantine records to process. Exiting.")
    dbutils.notebook.exit("NO_QUARANTINE_RECORDS")

print("\nRejection Reason Breakdown:")
(
    df_quarantine
    .groupBy("rejection_reason")
    .count()
    .orderBy("count", ascending=False)
    .show(20, truncate=False)
)

# ==============================================================================
# 3. CLASSIFY: Repairable vs. Unrecoverable
# ==============================================================================
# Repairable: caused by known upstream application bugs with a deterministic fix.
# Unrecoverable: structurally corrupt or missing primary key — cannot be merged.

REPAIRABLE_REASONS = [
    "anomaly_login_has_price",    # Login events should never carry price
    "anomaly_logout_has_price",   # Logout events should never carry price
    "anomaly_future_timestamp",   # Clock skew — fall back to ingestion_timestamp
]

df_repairable    = df_quarantine.filter(F.col("rejection_reason").isin(REPAIRABLE_REASONS))
df_unrecoverable = df_quarantine.filter(~F.col("rejection_reason").isin(REPAIRABLE_REASONS))

print(f"\nRepairable records:    {df_repairable.count()}")
print(f"Unrecoverable records: {df_unrecoverable.count()}")

# ==============================================================================
# 4. REPAIR LOGIC
# ==============================================================================

# Fix 1: Remove price from login/logout events (upstream application bug)
df_price_fixed = (
    df_repairable
    .filter(F.col("rejection_reason").isin(["anomaly_login_has_price", "anomaly_logout_has_price"]))
    .withColumn("price",      F.lit(None).cast(DoubleType()))
    .withColumn("product_id", F.lit(None).cast(IntegerType()))
    .drop("rejection_reason")
)

# Fix 2: Replace future timestamp with ingestion_timestamp (clock skew tolerance)
df_timestamp_fixed = (
    df_repairable
    .filter(F.col("rejection_reason") == "anomaly_future_timestamp")
    .withColumn("event_time", F.col("ingestion_timestamp"))
    .drop("rejection_reason")
)

df_recovered  = df_price_fixed.union(df_timestamp_fixed)
recovered_count = df_recovered.count()
print(f"\nRecords successfully repaired: {recovered_count}")

# ==============================================================================
# 5. ROUTE RECOVERED RECORDS → S3 BRONZE
# ==============================================================================
# DLT Auto Loader will detect these new files on its next trigger run and
# process them through the full Bronze → Silver → Gold pipeline.

if recovered_count > 0:
    (
        df_recovered
        .write
        .format("json")
        .mode("append")
        .save(BRONZE_PATH)
    )
    print(f"{recovered_count} recovered records written back to Bronze.")

# ==============================================================================
# 6. ROUTE UNRECOVERABLE RECORDS → DEAD LETTER (Delta format for auditability)
# ==============================================================================
unrecoverable_count = df_unrecoverable.count()
if unrecoverable_count > 0:
    (
        df_unrecoverable
        .withColumn("dead_lettered_at", F.current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .save(DEAD_LETTER_PATH)
    )
    print(f"{unrecoverable_count} unrecoverable records written to dead letter store.")
    print(f"Location: {DEAD_LETTER_PATH}")

# ==============================================================================
# 7. SUMMARY
# ==============================================================================
print(f"""
{'='*60}
QUARANTINE RECOVERY SUMMARY
{'='*60}
  Total quarantined:    {quarantine_count:>6d}
  Repaired & recovered: {recovered_count:>6d}  → Routed back to Bronze
  Unrecoverable:        {unrecoverable_count:>6d}  → Dead Letter store

  Recovery Rate: {round(recovered_count/quarantine_count*100, 1)}%
{'='*60}
""")
