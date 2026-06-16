# ==============================================================================
# DLT Pipeline Observability — Event Log Analysis
# ==============================================================================
# Purpose:
#   Queries the DLT Event Log (_dlt_event_log) to report on pipeline health,
#   data quality expectation failure rates, and SLA compliance.
#   Run as a scheduled Workflow task after the main DLT pipeline completes.
#
# Output: Console report surfacing:
#   - Last 7 pipeline run states (COMPLETED / FAILED)
#   - Per-expectation pass/fail counts and failure percentage
#   - SLA compliance (target: pipeline backlog = 0 within 15 minutes)
# ==============================================================================

from pyspark.sql import functions as F

# ==============================================================================
# 1. LOCATE THE DLT EVENT LOG
# ==============================================================================
# The event log path is: <pipeline_storage_location>/system/events
# When using Unity Catalog, it is also accessible as a system table.

PIPELINE_STORAGE_PATH = "s3://retailedge-analytics-prod/_dlt_event_log/retailedge_dlt_pipeline"

event_log = (
    spark.read.format("delta")
    .load(f"{PIPELINE_STORAGE_PATH}/system/events")
)

print("DLT Event Log Schema:")
event_log.printSchema()

# ==============================================================================
# 2. PIPELINE RUN HISTORY
# ==============================================================================
print("\nPipeline Run History (Last 7 Runs)")
(
    event_log
    .filter(F.col("event_type") == "update_progress")
    .select(
        "timestamp",
        F.get_json_object("details", "$.update_progress.state").alias("state"),
        F.get_json_object("details", "$.update_progress.creation_time").alias("created_at")
    )
    .orderBy(F.col("timestamp").desc())
    .limit(7)
    .show(truncate=False)
)

# ==============================================================================
# 3. DATA QUALITY EXPECTATION REPORT
# ==============================================================================
# Each @dlt.expect_or_drop rule writes a flow_progress event with pass/fail counts.
# This report surfaces which expectations are failing and at what rate.

print("\nData Quality Expectations — Failure Report")
(
    event_log
    .filter(F.col("event_type") == "flow_progress")
    .select(
        "timestamp",
        F.get_json_object("details", "$.flow_progress.name").alias("table_name"),
        F.get_json_object("details", "$.flow_progress.data_quality.expectations")
         .alias("expectations_json")
    )
    .filter(F.col("expectations_json").isNotNull())
    .select(
        "timestamp",
        "table_name",
        F.explode(F.from_json(
            "expectations_json",
            "array<struct<name:string,dataset:string,passed_records:long,failed_records:long>>"
        )).alias("expectation")
    )
    .select(
        "timestamp",
        "table_name",
        F.col("expectation.name").alias("rule_name"),
        F.col("expectation.passed_records").alias("passed"),
        F.col("expectation.failed_records").alias("failed"),
        F.round(
            F.col("expectation.failed_records") * 100.0 /
            (F.col("expectation.passed_records") + F.col("expectation.failed_records")),
            2
        ).alias("failure_pct")
    )
    .orderBy(F.col("timestamp").desc())
    .show(20, truncate=False)
)

# ==============================================================================
# 4. SLA COMPLIANCE CHECK
# ==============================================================================
# SLA Target: pipeline backlog_bytes = 0 after each triggered run.
# A non-zero backlog means the pipeline did not fully catch up in this run.

print("\nPipeline SLA Compliance Check (Target: backlog_bytes = 0 on completion)")
(
    event_log
    .filter(F.col("event_type") == "update_progress")
    .select(
        F.col("timestamp"),
        F.get_json_object("details", "$.update_progress.state").alias("state"),
        F.get_json_object("details", "$.update_progress.metrics.backlog_bytes")
         .cast("long").alias("backlog_bytes")
    )
    .filter(F.col("state") == "COMPLETED")
    .withColumn("within_sla", F.col("backlog_bytes") == 0)
    .show(10, truncate=False)
)

print("\nObservability check complete.")
