-- =============================================================================
-- Secure Data Sharing — RetailEdge Commerce Project
-- Purpose: Securely share gold_top_products with an external retail partner
-- without copying, moving, or extracting the data.
-- =============================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE EVENT_ANALYTICS;
USE SCHEMA ANALYTICS;

-- =============================================================================
-- 1. Create a Secure View for the Partner
-- =============================================================================
-- We don't want to share the entire base table. We create a SECURE VIEW that 
-- restricts data. (Secure views hide the DDL definition from the consumer).
CREATE OR REPLACE SECURE VIEW partner_top_products_vw AS
SELECT 
    event_date,
    product_id,
    views,
    purchases
FROM gold_top_products_dt
WHERE event_date >= DATEADD(month, -1, CURRENT_DATE()); -- Only share last 30 days

-- =============================================================================
-- 2. Create the Share Object
-- =============================================================================
CREATE SHARE IF NOT EXISTS retail_partner_share;

-- =============================================================================
-- 3. Grant Privileges to the Share
-- =============================================================================
-- You must grant usage on the database and schema before sharing the view
GRANT USAGE ON DATABASE EVENT_ANALYTICS TO SHARE retail_partner_share;
GRANT USAGE ON SCHEMA EVENT_ANALYTICS.ANALYTICS TO SHARE retail_partner_share;
GRANT SELECT ON VIEW partner_top_products_vw TO SHARE retail_partner_share;

-- =============================================================================
-- 4. Add the Consumer Account to the Share
-- =============================================================================
-- Replace 'XY12345' with the partner's actual Snowflake account locator
-- ALTER SHARE retail_partner_share ADD ACCOUNTS = XY12345;

-- Verify the share
SHOW SHARES;
