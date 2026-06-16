# ==============================================================================
# Delta Live Tables: Silver → Gold Pipeline
# ==============================================================================
# Purpose:
#   Reads from the Silver events table and produces 3 Gold aggregation tables
#   as business-level metrics for downstream BI consumption via Snowflake.
#
# Output tables (retailedge.gold.*):
#   gold_daily_active_users  — DAU, total events, purchasing users
#   gold_daily_revenue       — purchase count, total/avg/min/max revenue
#   gold_conversion_funnel   — view → cart → purchase conversion rates
# ==============================================================================

import dlt
from pyspark.sql import functions as F

# ==============================================================================
# GOLD LAYER (Business Aggregations)
# ==============================================================================

@dlt.table(
    name="gold_daily_active_users",
    comment="Daily Active Users (DAU) and purchasing user metrics",
    table_properties={"quality": "gold"}
)
def daily_active_users():
    """
    Reads from the Silver DLT table. This Gold table is automatically
    refreshed whenever Silver is updated by the upstream pipeline run.
    """
    return (
        dlt.read("events_silver")
        .withColumn("event_date", F.to_date("event_time"))
        .groupBy("event_date")
        .agg(
            F.countDistinct("user_id").alias("daily_active_users"),
            F.count("*").alias("total_events"),
            F.countDistinct(
                F.when(F.col("event_type") == "purchase", F.col("user_id"))
            ).alias("purchasing_users"),
        )
    )

@dlt.table(
    name="gold_daily_revenue",
    comment="Daily aggregated revenue and average order value",
    table_properties={"quality": "gold"}
)
def daily_revenue():
    return (
        dlt.read("events_silver")
        .filter((F.col("event_type") == "purchase") & F.col("price").isNotNull())
        .withColumn("event_date", F.to_date("event_time"))
        .groupBy("event_date")
        .agg(
            F.count("*").alias("purchase_count"),
            F.round(F.sum("price"), 2).alias("total_revenue"),
            F.round(F.avg("price"), 2).alias("avg_order_value"),
            F.round(F.min("price"), 2).alias("min_order"),
            F.round(F.max("price"), 2).alias("max_order"),
        )
    )

@dlt.table(
    name="gold_conversion_funnel",
    comment="Daily view -> cart -> purchase conversion percentages",
    table_properties={"quality": "gold"}
)
def conversion_funnel():
    return (
        dlt.read("events_silver")
        .withColumn("event_date", F.to_date("event_time"))
        .groupBy("event_date")
        .agg(
            F.countDistinct(F.when(F.col("event_type") == "product_view", F.col("user_id"))).alias("viewers"),
            F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("user_id"))).alias("cart_adders"),
            F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("user_id"))).alias("purchasers"),
        )
        .withColumn("view_to_cart_pct", F.round(F.col("cart_adders") / F.col("viewers") * 100, 2))
        .withColumn("cart_to_purchase_pct", F.round(F.col("purchasers") / F.col("cart_adders") * 100, 2))
    )
