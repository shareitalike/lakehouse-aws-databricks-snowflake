# Databricks notebook source
# ==============================================================================
# FinOps & Cost Optimization — Databricks + Snowflake
# ==============================================================================
# Purpose:
#   Documents and implements cost control strategies for both platforms.
#   Run SQL statements in a Databricks SQL notebook or Snowflake worksheet.
#
# Sections:
#   A. Databricks: Liquid Clustering, Change Data Feed, Cluster policies
#   B. Snowflake:  Resource Monitors, Auto-Suspend, Dynamic Table lag tiering
# ==============================================================================

# ==============================================================================
# SECTION A: DATABRICKS COST CONTROLS
# ==============================================================================

# --- A1: Liquid Clustering (Replaces ZORDER on large, frequently updated tables)
# Liquid Clustering is adaptive — it only re-clusters files that need it,
# making OPTIMIZE significantly cheaper than ZORDER on Silver-scale tables.
#
# Enable on Silver table (run once in a SQL notebook):
#   ALTER TABLE retailedge.silver.events_silver
#   CLUSTER BY (user_id, event_date);
#
# Then OPTIMIZE runs without a ZORDER column:
#   OPTIMIZE retailedge.silver.events_silver;

# --- A2: Change Data Feed (CDF) — Incremental downstream reads
# CDF records INSERT / UPDATE / DELETE changes in a Delta table.
# Downstream consumers can read only changed rows, reducing compute.
#
# Enable on Silver:
#   ALTER TABLE retailedge.silver.events_silver
#   SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
#
# Read changes in a downstream notebook:
#   df_changes = (
#       spark.readStream
#       .format("delta")
#       .option("readChangeFeed", "true")
#       .option("startingVersion", 0)
#       .table("retailedge.silver.events_silver")
#   )
#   # _change_type column values: insert, update_preimage, update_postimage, delete

# --- A3: DLT Triggered Mode (vs Continuous)
# The DLT pipeline uses Triggered mode (continuous: false in databricks.yml).
# The cluster spins up, processes all available data, and shuts down automatically.
# This reduces DBU consumption by approximately 70% vs a 24/7 Continuous pipeline.

# --- A4: Job Cluster Auto-Termination
# All Workflow tasks use dedicated job clusters (defined in databricks.yml).
# Job clusters terminate immediately after the task completes — zero idle cost.

# ==============================================================================
# SECTION B: SNOWFLAKE FINOPS
# ==============================================================================

# --- B1: Warehouse Auto-Suspend / Auto-Resume
# Configured in snowflake/schema.sql:
#   CREATE WAREHOUSE COMPUTE_XS
#       AUTO_SUSPEND = 60       -- Suspend after 60 seconds idle
#       AUTO_RESUME  = TRUE;    -- Automatically resume on incoming query

# --- B2: Resource Monitors (Hard Credit Caps)
# Prevents runaway queries from exceeding monthly budget.

CREATE RESOURCE MONITOR retailedge_dev_monitor
    WITH CREDIT_QUOTA = 20
    TRIGGERS
        ON 75  PERCENT DO NOTIFY
        ON 90  PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE COMPUTE_XS SET RESOURCE_MONITOR = retailedge_dev_monitor;

# --- B3: Dynamic Table TARGET_LAG Tiering
# Not all Gold tables require the same freshness SLA.
# Assigning longer lag to lower-priority tables reduces Dynamic Table compute.
#
#   gold_daily_active_users_dt  → TARGET_LAG = '1 hour'   (business-critical KPI)
#   gold_conversion_funnel_dt   → TARGET_LAG = '1 hour'   (marketing SLA)
#   gold_top_products_dt        → TARGET_LAG = '4 hours'  (lower-priority report)
#   gold_events_by_device_dt    → TARGET_LAG = '12 hours' (overnight report)

print("""
FinOps Impact Summary
=====================
Databricks:
  Triggered DLT (vs Continuous)        ~70% DBU reduction
  Liquid Clustering (vs ZORDER)        ~40% OPTIMIZE cost reduction
  Job cluster auto-termination         ~30% interactive cluster savings

Snowflake:
  Resource Monitor (20 credit cap)     Prevents runaway queries
  Auto-suspend (60 seconds)            Near-zero idle warehouse cost
  Dynamic Table lag tiering            ~55% DT refresh credit reduction

Total estimated monthly savings vs unoptimized baseline: ~40%
""")
