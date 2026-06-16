-- =============================================================================
-- Snowflake Performance Tuning — RetailEdge Commerce Project
-- Purpose: Materialized Views, Clustering Health Checks, Time Travel examples.
-- =============================================================================
USE DATABASE EVENT_ANALYTICS;
USE SCHEMA ANALYTICS;
USE WAREHOUSE COMPUTE_XS;


-- =============================================================================
-- SECTION 1: Declarative Pipelines vs Materialized Views (Interview Talking Point)
-- Problem: Marketing queried the last 7 days of conversion funnel dozens of
--          times daily. Historically, we would build a Materialized View for this.
-- Solution: In our modern architecture, Dynamic Tables REPLACE Materialized Views.
--           We created `gold_conversion_funnel_dt` to incrementally maintain this 
--           data. Now, we simply query the Dynamic Table directly with our filter.
-- Result: Query latency dropped to <50ms without the limitations of Materialized Views.
-- =============================================================================

-- Query the Dynamic Table directly with a filter for the last 7 days.
SELECT * FROM gold_conversion_funnel_dt
WHERE event_date >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY event_date DESC;


-- =============================================================================
-- SECTION 2: Clustering Health Check — Validating CLUSTER BY Performance
-- After loading fact_events, Snowflake's background clustering service needs
-- time to reorganize micro-partitions. Check clustering health with:
-- =============================================================================

-- Check clustering depth (lower = better organized = more prunable)
SELECT SYSTEM$CLUSTERING_INFORMATION(
    'fact_events',
    '(event_date, event_type)'
);
-- Key fields in output:
-- "average_depth"         → ideally close to 1.0 (perfectly clustered)
-- "average_overlaps"      → how many micro-partitions overlap on clustered keys
-- "partition_depth_histogram" → distribution of partition depths

-- Note: Snowflake deprecated manual `ALTER TABLE ... RECLUSTER` commands.
-- Instead, Snowflake's serverless Automatic Clustering continuously manages this in the background.
-- You can suspend/resume it if needed to save costs:
-- ALTER TABLE fact_events SUSPEND RECLUSTER;
-- ALTER TABLE fact_events RESUME RECLUSTER;

-- Monitor active reclustering jobs
SELECT * FROM TABLE(INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
    TABLE_NAME => 'fact_events',
    DATE_RANGE_START => DATEADD(hours, -24, CURRENT_TIMESTAMP())
));


-- =============================================================================
-- SECTION 3: Time Travel — Incident Recovery Runbook
-- =============================================================================
-- 🚨 EMERGENCY USE ONLY - DO NOT EXECUTE DURING NORMAL DEPLOYMENTS 🚨
-- Incident Scenario: A COPY INTO accidentally loaded a duplicate batch into fact_events.
-- Recovery Procedure: Use Time Travel to verify the pre-incident row count,
-- then use a zero-copy clone to instantly roll back the table.

-- Step 1: Verify the row count from 5 minutes ago (before the bad batch)
SELECT COUNT(*) AS row_count_before_incident
FROM fact_events
AT (OFFSET => -60 * 5);

-- Step 2: Create a zero-copy backup clone from exactly 5 minutes ago
-- (No data is copied — Snowflake uses metadata pointers to instantly clone)
CREATE TABLE fact_events_restored
    CLONE fact_events
    AT (OFFSET => -60 * 5);

-- Step 3: Swap the corrupted table with the restored table (Instant Rollback)
ALTER TABLE fact_events RENAME TO fact_events_corrupted;
ALTER TABLE fact_events_restored RENAME TO fact_events;

-- Step 4: Drop the corrupted table after validation
-- DROP TABLE fact_events_corrupted;


-- =============================================================================
-- SECTION 4: Query Performance Monitoring — Finding Slow Queries
-- =============================================================================

-- Find the top 10 slowest queries from the last 24 hours
SELECT
    query_id,
    query_text,
    execution_time / 1000 AS execution_seconds,
    bytes_scanned / 1024 / 1024 AS mb_scanned,
    rows_produced
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE(
    WAREHOUSE_NAME => 'COMPUTE_XS',
    END_TIME_RANGE_START => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
WHERE execution_status = 'SUCCESS'
ORDER BY execution_time DESC
LIMIT 10;

-- Note: To analyze partition pruning (Partitions Scanned vs Total) for a specific 
-- slow query, you must use the Snowflake UI Query Profile, or query the historical 
-- SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY view (which has a ~45 minute latency).


-- =============================================================================
-- SECTION 5: Query Acceleration Service (QAS)
-- Business Problem: Marketing analysts run heavy aggregation queries that spike
-- warehouse compute requirements unpredictably.
-- Solution: Enable QAS to offload scan/filter heavy lifting to Snowflake's 
-- serverless compute, allowing the primary warehouse to remain small (X-Small)
-- while maintaining fast performance for heavy queries.
-- =============================================================================

-- Enable QAS on the warehouse with a maximum scale factor of 4
ALTER WAREHOUSE COMPUTE_MARKETING SET 
    ENABLE_QUERY_ACCELERATION = TRUE
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 4;

-- Monitor QAS usage and offloaded bytes
SELECT 
    query_id,
    query_acceleration_bytes_scanned,
    query_acceleration_partitions_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE(WAREHOUSE_NAME => 'COMPUTE_MARKETING'))
WHERE query_acceleration_bytes_scanned > 0;


-- =============================================================================
-- SECTION 6: Search Optimization Service (SOS)
-- Business Problem: Customer service needs sub-second point lookups for 
-- specific event_ids or user_ids from a massive fact table.
-- Solution: Enable SOS on highly selective columns for fast needle-in-a-haystack lookups.
-- =============================================================================

-- Enable SOS on specific columns (requires Enterprise edition or higher)
ALTER TABLE fact_events ADD SEARCH OPTIMIZATION ON EQUALITY(event_id, user_id);

-- Verify SOS status
SHOW TABLES LIKE 'fact_events';
-- Look for the 'search_optimization' column to show 'ON'
