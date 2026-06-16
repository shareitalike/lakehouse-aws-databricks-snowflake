# ==============================================================================
# Performance Tuning: OPTIMIZE & VACUUM — All Delta Lake Layers
# ==============================================================================
# Databricks Certification Focus:
# - OPTIMIZE compacts many small DLT output files into larger ones (improves reads).
# - VACUUM removes data files older than the retention threshold (saves S3 storage).
# - This runs as a Workflow task AFTER DLT finishes, on ALL three Medallion layers.
#
# Why Optimize ALL layers, not just Silver?
# DLT uses streaming micro-batches, producing many small Parquet files per run.
# Gold tables especially suffer from "small file problem" because each DLT trigger
# writes a small aggregate result file. OPTIMIZE coalesces these into larger files
# that Snowflake and Databricks SQL can scan much faster.
#
# Cost Note: Run on a SPOT cluster (configured in databricks.yml) to minimize DBU cost.
# ==============================================================================

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

CATALOG = "retailedge"

# Gold table names (must match DLT output table names)
GOLD_TABLES = [
    "gold_daily_active_users",
    "gold_daily_revenue",
    "gold_conversion_funnel",
    "gold_top_products",
    "gold_events_by_device",
]


def optimize_and_vacuum(table: str, zorder_col: str = None, retain_hours: int = 168):
    """
    Run OPTIMIZE and VACUUM on a Unity Catalog Delta table.

    Args:
        table:        Full 3-level UC table name (catalog.schema.table)
        zorder_col:   Column to ZORDER by (most filtered column in queries)
        retain_hours: File retention for VACUUM (default 7 days = 168 hours)
    """
    print(f"\n⚙️  Optimizing: {table}")
    if zorder_col:
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({zorder_col})")
    else:
        spark.sql(f"OPTIMIZE {table}")

    spark.sql(f"VACUUM {table} RETAIN {retain_hours} HOURS")
    print(f"   ✅ Done: {table}")


# ==============================================================================
# 1. BRONZE LAYER — External Delta table over raw S3 events
# ==============================================================================
optimize_and_vacuum(
    f"{CATALOG}.bronze.events_bronze",
    zorder_col="event_time"   # Most commonly filtered column in Bronze queries
)

# ==============================================================================
# 2. SILVER LAYER — CDC-merged, deduplicated events
# ==============================================================================
optimize_and_vacuum(
    f"{CATALOG}.silver.events_silver",
    zorder_col="user_id"      # Filter pattern: WHERE user_id = X for joins
)

# ==============================================================================
# 3. GOLD LAYER — All 5 business aggregation tables
# Note: Gold tables have no obvious ZORDER column (they are already pre-aggregated).
# OPTIMIZE alone (without ZORDER) still massively reduces file count.
# ==============================================================================
for table_name in GOLD_TABLES:
    optimize_and_vacuum(f"{CATALOG}.gold.{table_name}")

# ==============================================================================
# 4. DEAD LETTER TABLE — Compact unrecoverable quarantine records
# ==============================================================================
optimize_and_vacuum(
    f"{CATALOG}.bronze.dead_letter_events",
    zorder_col="rejection_reason"
)

print("""
======================================================
✅ OPTIMIZE & VACUUM COMPLETE — All Medallion Layers
======================================================
  Bronze:     events_bronze (ZORDER by event_time)
  Silver:     events_silver (ZORDER by user_id)
  Gold:       5 aggregation tables optimized
  Dead Letter: dead_letter_events compacted
======================================================
""")
