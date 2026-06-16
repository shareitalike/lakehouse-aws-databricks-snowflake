-- ==============================================================================
-- Unity Catalog System Tables: Data Lineage Queries
-- ==============================================================================
-- Databricks Certification & Senior Engineer Focus:
-- Unity Catalog automatically captures column-level lineage for ALL operations
-- (reads, writes, MERGE, DLT pipelines) without any manual configuration.
-- 
-- This is stored in Databricks "System Tables" — a set of read-only, managed
-- tables under the `system` catalog. This is rarely seen in portfolio projects
-- and will absolutely impress senior interviewers.
--
-- Interview Talking Point:
-- "One of the key reasons we chose Unity Catalog was the automatic column-level
-- lineage tracking. When compliance teams asked 'where does the daily_revenue
-- metric come from?', we could answer in seconds by querying the system tables
-- instead of maintaining manual data dictionaries."
--
-- Certification Topic: Unity Catalog System Tables (system.access, system.lineage)
-- ==============================================================================

-- ==============================================================================
-- 1. TABLE-LEVEL LINEAGE: Where does Gold data come from?
-- ==============================================================================
-- This query traces the full upstream lineage of a Gold table back to the source.
-- The answer: gold_daily_revenue ← events_silver ← events_bronze ← S3 External Location

SELECT
    source_table_full_name,
    target_table_full_name,
    created_by,
    event_time
FROM system.lineage.table_lineage
WHERE target_table_full_name = 'retailedge.gold.gold_daily_revenue'
ORDER BY event_time DESC;

-- ==============================================================================
-- 2. COLUMN-LEVEL LINEAGE: Where does a specific column come from?
-- ==============================================================================
-- Compliance use-case: "Which column feeds the total_revenue field?"
-- Answer: events_silver.price -> gold_daily_revenue.total_revenue (via SUM aggregation)

SELECT
    source_table_full_name,
    source_column_name,
    target_table_full_name,
    target_column_name,
    transformation_type     -- e.g., DIRECT, AGGREGATE, CUSTOM
FROM system.lineage.column_lineage
WHERE 
    target_table_full_name = 'retailedge.gold.gold_daily_revenue'
    AND target_column_name = 'total_revenue'
ORDER BY event_time DESC;

-- ==============================================================================
-- 3. FULL PIPELINE LINEAGE MAP (End-to-End Trace)
-- ==============================================================================
-- Traces the complete data flow for the entire RetailEdge pipeline.
-- Useful for root cause analysis during incidents.

SELECT DISTINCT
    source_table_full_name,
    target_table_full_name,
    created_by          AS pipeline_or_user,
    event_time
FROM system.lineage.table_lineage
WHERE 
    source_table_full_name LIKE 'retailedge.%'
    OR target_table_full_name LIKE 'retailedge.%'
ORDER BY event_time DESC;

-- ==============================================================================
-- 4. ACCESS AUDIT LOG: Who queried our Gold data?
-- ==============================================================================
-- Compliance & Security use-case: "Who accessed the revenue data last week?"
-- This is critical for GDPR and SOC2 compliance reports.

SELECT
    event_time,
    user_identity.email     AS queried_by,
    action_name,            -- e.g., 'getTable', 'runCommand', 'queryData'
    request_params.full_name AS table_accessed,
    source_ip_address
FROM system.access.audit
WHERE
    action_name IN ('getTable', 'queryData', 'runCommand')
    AND request_params.full_name LIKE 'retailedge.gold.%'
    AND event_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
ORDER BY event_time DESC
LIMIT 100;

-- ==============================================================================
-- 5. DATA FRESHNESS MONITORING (System Tables + DLT)
-- ==============================================================================
-- Operational use-case: "When was each Gold table last updated?"

SELECT
    table_name,
    last_modified,
    size_in_bytes,
    num_files,
    DATEDIFF(MINUTE, last_modified, CURRENT_TIMESTAMP()) AS minutes_since_refresh
FROM system.information_schema.tables
WHERE table_schema = 'gold'
    AND table_catalog = 'retailedge'
ORDER BY last_modified DESC;
