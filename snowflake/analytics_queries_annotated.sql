-- ==============================================================================
-- RetailEdge Commerce: Snowflake Analytics Queries (Annotated for Interviews)
-- 
-- Audience: Use this file to demonstrate complex SQL knowledge during interviews.
-- It contains window functions, CTEs, clustering keys awareness, and business logic.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- QUERY 1: The "Abandoned Cart" Funnel Analysis
-- 
-- Business Context: Marketing wants to know what percentage of users who add an
-- item to their cart actually complete the checkout, grouped by device type,
-- looking at data from the last 30 days.
-- 
-- Technical Highlights:
-- * Multi-step CTEs for clarity
-- * LEFT JOINs to track drop-offs
-- * Aggregation with ratios
-- * Uses the `user_sessions_gold` and `event_facts_gold` tables
-- ------------------------------------------------------------------------------

WITH AddToCartEvents AS (
    SELECT 
        session_id,
        user_id,
        device_type,
        COUNT(*) as items_added
    FROM 
        RETAILEDGE.GOLD.EVENT_FACTS_GOLD
    WHERE 
        event_type = 'add_to_cart'
        AND event_timestamp >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY 
        session_id, user_id, device_type
),
CheckoutEvents AS (
    SELECT DISTINCT
        session_id
    FROM 
        RETAILEDGE.GOLD.EVENT_FACTS_GOLD
    WHERE 
        event_type = 'checkout_complete'
        AND event_timestamp >= DATEADD(day, -30, CURRENT_DATE())
)
SELECT 
    a.device_type,
    COUNT(a.session_id) as total_add_to_cart_sessions,
    COUNT(c.session_id) as total_completed_checkouts,
    ROUND(COUNT(c.session_id) * 100.0 / NULLIF(COUNT(a.session_id), 0), 2) as conversion_rate_pct
FROM 
    AddToCartEvents a
LEFT JOIN 
    CheckoutEvents c ON a.session_id = c.session_id
GROUP BY 
    a.device_type
ORDER BY 
    conversion_rate_pct DESC;


-- ------------------------------------------------------------------------------
-- QUERY 2: Sessionization & Time-Between-Events (Window Functions)
-- 
-- Business Context: Product team wants to know the average time it takes a user
-- to go from 'page_view' to 'add_to_cart' within a single session.
-- 
-- Technical Highlights:
-- * LEAD() window function to peek at the next row's timestamp
-- * DATEDIFF for time interval calculation
-- * Partitioning by session_id to isolate user journeys
-- ------------------------------------------------------------------------------

WITH EventJourney AS (
    SELECT 
        session_id,
        event_type,
        event_timestamp,
        -- Get the timestamp of the NEXT event in the same session
        LEAD(event_timestamp) OVER (
            PARTITION BY session_id 
            ORDER BY event_timestamp ASC
        ) as next_event_timestamp,
        -- Get the event type of the NEXT event in the same session
        LEAD(event_type) OVER (
            PARTITION BY session_id 
            ORDER BY event_timestamp ASC
        ) as next_event_type
    FROM 
        RETAILEDGE.GOLD.EVENT_FACTS_GOLD
    WHERE 
        -- Optimization: Only pull the last 7 days of data for this ad-hoc analysis
        event_timestamp >= DATEADD(day, -7, CURRENT_DATE())
)
SELECT 
    AVG(DATEDIFF(second, event_timestamp, next_event_timestamp)) as avg_seconds_to_add_to_cart
FROM 
    EventJourney
WHERE 
    event_type = 'page_view' 
    AND next_event_type = 'add_to_cart'
    -- Filter out edge cases where a session spanned an unusually long time
    AND DATEDIFF(second, event_timestamp, next_event_timestamp) < 3600; 


-- ------------------------------------------------------------------------------
-- QUERY 3: Cumulative Revenue YTD by Region (Advanced Analytics)
-- 
-- Business Context: Finance needs a running total (cumulative sum) of revenue 
-- year-to-date, broken down by user region.
-- 
-- Technical Highlights:
-- * Joining Fact and Dimension tables (Star Schema)
-- * SUM() OVER (PARTITION BY ... ORDER BY ...) for running totals
-- * Date truncation (DATE_TRUNC) for daily rollups
-- ------------------------------------------------------------------------------

WITH DailyRevenue AS (
    SELECT 
        u.region,
        DATE_TRUNC('day', e.event_timestamp) as revenue_date,
        SUM(e.price * e.quantity) as daily_revenue
    FROM 
        RETAILEDGE.GOLD.EVENT_FACTS_GOLD e
    JOIN 
        RETAILEDGE.GOLD.DIM_USERS u ON e.user_id = u.user_id
    WHERE 
        e.event_type = 'checkout_complete'
        AND YEAR(e.event_timestamp) = YEAR(CURRENT_DATE())
    GROUP BY 
        u.region, DATE_TRUNC('day', e.event_timestamp)
)
SELECT 
    region,
    revenue_date,
    daily_revenue,
    -- Calculate the running total partitioned by region and ordered by date
    SUM(daily_revenue) OVER (
        PARTITION BY region 
        ORDER BY revenue_date ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_revenue_ytd
FROM 
    DailyRevenue
ORDER BY 
    region, revenue_date;


-- ------------------------------------------------------------------------------
-- QUERY 4: Identifying Data Anomalies (Data Quality Checks)
-- 
-- Business Context: Data Engineering (us) running ad-hoc checks to ensure
-- there are no orphaned records or weird timestamp drifts in the Gold layer.
-- 
-- Technical Highlights:
-- * Anti-join pattern to find missing dimension keys
-- * Having clause for finding duplicate primary keys
-- ------------------------------------------------------------------------------

-- Check 1: Find events that belong to a user NOT in our user dimension (Orphaned records)
SELECT 
    COUNT(e.event_id) as orphaned_event_count
FROM 
    RETAILEDGE.GOLD.EVENT_FACTS_GOLD e
LEFT JOIN 
    RETAILEDGE.GOLD.DIM_USERS u ON e.user_id = u.user_id
WHERE 
    u.user_id IS NULL;

-- Check 2: Find sessions that have events, but no 'session_start' event (Incomplete sessions)
SELECT 
    session_id, 
    COUNT(*) as total_events
FROM 
    RETAILEDGE.GOLD.EVENT_FACTS_GOLD
GROUP BY 
    session_id
HAVING 
    SUM(CASE WHEN event_type = 'session_start' THEN 1 ELSE 0 END) = 0
ORDER BY 
    total_events DESC
LIMIT 10;
