-- =============================================================================
-- Declarative Pipelines via Dynamic Tables — RetailEdge Commerce Project
-- Purpose: Modernize batch ETL into continuous, declarative pipelines guaranteeing 
-- a 1-hour freshness SLA without manual orchestration or MERGE statements.
-- =============================================================================
USE DATABASE EVENT_ANALYTICS;
USE SCHEMA ANALYTICS;

-- =============================================================================
-- Pre-requisite: The External Stage (Simulating Bronze/Silver data arriving)
-- =============================================================================
-- Note: Assuming the base fact_events table is continuously updated via Snowpipe 
-- or a Delta Live Tables push. The Dynamic Tables will react to changes in fact_events.

-- =============================================================================
-- 1. Daily Active Users Dynamic Table
-- =============================================================================
-- Target Lag: 1 hour (Snowflake will automatically compute changes every hour)
CREATE OR REPLACE DYNAMIC TABLE gold_daily_active_users_dt
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_XS
AS
SELECT
    event_date,
    COUNT(DISTINCT user_id) AS daily_active_users,
    COUNT(event_id) AS total_events,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS purchasing_users,
    CURRENT_TIMESTAMP() AS loaded_at
FROM fact_events
GROUP BY event_date;

-- =============================================================================
-- 2. Daily Revenue Dynamic Table
-- =============================================================================
CREATE OR REPLACE DYNAMIC TABLE gold_daily_revenue_dt
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_XS
AS
SELECT
    event_date,
    COUNT(event_id) AS purchase_count,
    SUM(price) AS total_revenue,
    AVG(price) AS avg_order_value,
    MIN(price) AS min_order,
    MAX(price) AS max_order,
    CURRENT_TIMESTAMP() AS loaded_at
FROM fact_events
WHERE event_type = 'purchase'
GROUP BY event_date;

-- =============================================================================
-- 3. Top Products Dynamic Table
-- =============================================================================
CREATE OR REPLACE DYNAMIC TABLE gold_top_products_dt
    TARGET_LAG = '4 hours' -- Less critical, so longer lag saves compute costs
    WAREHOUSE = COMPUTE_XS
AS
SELECT
    event_date,
    product_id,
    COUNT(event_id) AS total_interactions,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(CASE WHEN event_type = 'product_view' THEN 1 END) AS views,
    COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS cart_adds,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchases,
    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS revenue,
    CURRENT_TIMESTAMP() AS loaded_at
FROM fact_events
WHERE product_id IS NOT NULL
GROUP BY event_date, product_id;

-- =============================================================================
-- 4. Conversion Funnel Dynamic Table
-- =============================================================================
CREATE OR REPLACE DYNAMIC TABLE gold_conversion_funnel_dt
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_XS
AS
SELECT 
    event_date,
    COUNT(DISTINCT CASE WHEN event_type = 'product_view' THEN user_id END) AS viewers,
    COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) AS cart_adders,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS purchasers,
    ROUND(COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'product_view' THEN user_id END), 0), 2) AS view_to_cart_pct,
    ROUND(COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) * 100.0 / 
          NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END), 0), 2) AS cart_to_purchase_pct,
    CURRENT_TIMESTAMP() AS loaded_at
FROM fact_events
GROUP BY event_date;

-- =============================================================================
-- 5. Monitor Dynamic Tables Refresh History
-- =============================================================================
-- As a Senior Engineer, tracking pipeline health is critical.
-- Check Dynamic Table Configuration
SHOW DYNAMIC TABLES IN DATABASE EVENT_ANALYTICS;

-- Check Dynamic Table Refresh Logs
SELECT
    name,
    state,
    data_timestamp,
    refresh_action,
    target_lag_sec
FROM TABLE(EVENT_ANALYTICS.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
ORDER BY data_timestamp DESC;
