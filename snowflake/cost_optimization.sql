-- =============================================================================
-- Snowflake FinOps & Cost Control — RetailEdge Commerce Project
-- Purpose: Implement Resource Monitors and auto-suspend policies to prevent 
-- budget overruns from rogue queries or pipeline errors.
-- =============================================================================
USE ROLE ACCOUNTADMIN; -- Resource monitors require Account Admin privileges

-- =============================================================================
-- 1. Create a Global Resource Monitor (Credit Quota)
-- =============================================================================
-- Business Rule: The BI and Marketing warehouses share a monthly budget of 100 credits.
-- We want to be notified at 75% and 90%, and forcefully suspend at 100% to stop billing.

CREATE OR REPLACE RESOURCE MONITOR rm_analytics_budget
    WITH CREDIT_QUOTA = 100
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS 
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;

-- =============================================================================
-- 2. Assign the Resource Monitor to Warehouses
-- =============================================================================
-- Attach the monitor to the specific warehouses used by the analytics teams
ALTER WAREHOUSE COMPUTE_XS SET RESOURCE_MONITOR = rm_analytics_budget;

-- Assuming COMPUTE_MARKETING was created in rbac_governance.sql
ALTER WAREHOUSE COMPUTE_MARKETING SET RESOURCE_MONITOR = rm_analytics_budget;

-- =============================================================================
-- 3. Warehouse Optimization Health Checks
-- =============================================================================
-- Ensure all warehouses are set to auto-suspend quickly (60 seconds) to minimize idle time
ALTER WAREHOUSE COMPUTE_XS SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
ALTER WAREHOUSE COMPUTE_MARKETING SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- =============================================================================
-- 4. FinOps Reporting: Query Cost Analysis
-- =============================================================================
-- View the most expensive queries running in the account over the last 30 days
USE ROLE SYSADMIN;
USE DATABASE EVENT_ANALYTICS;

SELECT 
    query_id,
    query_text,
    warehouse_name,
    execution_time / 1000 / 60 AS execution_minutes,
    (execution_time / 1000 / 60 / 60) * 1.0 AS estimated_credits_used -- Approx for X-Small (1 credit/hr)
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    END_TIME_RANGE_START => DATEADD(days, -30, CURRENT_TIMESTAMP())
))
WHERE warehouse_size IS NOT NULL
ORDER BY execution_time DESC
LIMIT 20;
