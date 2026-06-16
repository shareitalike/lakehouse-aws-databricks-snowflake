# ==============================================================================
# Snowflake Integration — Gold Layer Export
# ==============================================================================
# Purpose:
#   Reads Gold aggregation tables from Unity Catalog (Delta Lake) and writes
#   them to Snowflake using the Spark-Snowflake connector.
#   Runs as a Databricks Workflow task after the DLT pipeline completes.
#
# Inputs:  retailedge.gold.* (Unity Catalog Delta tables)
# Outputs: Snowflake EVENT_ANALYTICS.ANALYTICS.* (appended each run)
#
# Snowflake Dynamic Tables (defined in snowflake/dynamic_tables.sql) pick up
# the new rows automatically within their configured TARGET_LAG window.
# ==============================================================================

from pyspark.sql import functions as F

# ==============================================================================
# 1. SNOWFLAKE CONNECTION (credentials from Databricks Secrets)
# ==============================================================================
snowflake_options = {
    "sfURL":       dbutils.secrets.get(scope="snowflake", key="sfURL"),
    "sfUser":      dbutils.secrets.get(scope="snowflake", key="sfUser"),
    "sfPassword":  dbutils.secrets.get(scope="snowflake", key="sfPassword"),
    "sfDatabase":  "EVENT_ANALYTICS",
    "sfSchema":    "ANALYTICS",
    "sfWarehouse": "COMPUTE_XS",
    "sfRole":      "SYSADMIN"
}

# ==============================================================================
# 2. READ GOLD TABLES FROM UNITY CATALOG
# ==============================================================================
print("Reading Gold tables from Unity Catalog (retailedge.gold)...")

df_dau      = spark.table("retailedge.gold.gold_daily_active_users")
df_revenue  = spark.table("retailedge.gold.gold_daily_revenue")
df_funnel   = spark.table("retailedge.gold.gold_conversion_funnel")
df_products = spark.table("retailedge.gold.gold_top_products")

# ==============================================================================
# 3. WRITE TO SNOWFLAKE
# ==============================================================================
def write_to_snowflake(df, table_name: str, mode: str = "append"):
    """
    Writes a Spark DataFrame to a Snowflake table using the official connector.

    Args:
        df:         Spark DataFrame to write.
        table_name: Target Snowflake table name (within sfSchema).
        mode:       'append' for incremental loads, 'overwrite' for full refresh.
    """
    print(f"  Writing {df.count()} rows → Snowflake.{table_name} ...")
    (
        df.write
        .format("net.snowflake.spark.snowflake")
        .options(**snowflake_options)
        .option("dbtable", table_name)
        .mode(mode)
        .save()
    )
    print(f"  {table_name} updated.")

write_to_snowflake(df_dau,      "gold_daily_active_users")
write_to_snowflake(df_revenue,  "gold_daily_revenue")
write_to_snowflake(df_funnel,   "gold_conversion_funnel")
write_to_snowflake(df_products, "gold_top_products")

# ==============================================================================
# 4. COMPLETION
# ==============================================================================
print("""
Export complete.

Snowflake Dynamic Tables will refresh within their TARGET_LAG window:
  gold_daily_active_users_dt  → 1 hour
  gold_daily_revenue_dt       → 1 hour
  gold_conversion_funnel_dt   → 1 hour
  gold_top_products_dt        → 4 hours
""")
