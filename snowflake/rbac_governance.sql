-- =============================================================================
-- Snowflake RBAC Governance — RetailEdge Commerce Project
-- Purpose: Implements least-privilege access control for BI and Marketing teams.
-- =============================================================================
USE ROLE ACCOUNTADMIN; -- Required to create roles and users
USE DATABASE EVENT_ANALYTICS;
USE SCHEMA ANALYTICS;

-- =============================================================================
-- Step 1: Create Custom Roles
-- =============================================================================
CREATE ROLE IF NOT EXISTS BI_ANALYST_ROLE;     -- Full read on all analytics tables
CREATE ROLE IF NOT EXISTS MARKETING_ROLE;      -- Read access on aggregate Gold tables only

-- =============================================================================
-- Step 2: Create a Dedicated Marketing Warehouse (Separate from BI)
-- Reason: Prevents BI month-end heavy extracts from contending with Marketing's
-- real-time dashboard queries. Snowflake's decoupled compute allows this.
-- =============================================================================
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_MARKETING
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Dedicated compute for Marketing team dashboards. Isolated from BI extracts.';

-- =============================================================================
-- Step 3: Grant Warehouse Usage
-- =============================================================================
GRANT USAGE ON WAREHOUSE COMPUTE_XS         TO ROLE BI_ANALYST_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_MARKETING  TO ROLE MARKETING_ROLE;

-- =============================================================================
-- Step 4: Grant Schema Usage
-- =============================================================================
GRANT USAGE ON DATABASE EVENT_ANALYTICS     TO ROLE BI_ANALYST_ROLE;
GRANT USAGE ON SCHEMA ANALYTICS             TO ROLE BI_ANALYST_ROLE;

GRANT USAGE ON DATABASE EVENT_ANALYTICS     TO ROLE MARKETING_ROLE;
GRANT USAGE ON SCHEMA ANALYTICS             TO ROLE MARKETING_ROLE;

-- =============================================================================
-- Step 5: Grant Object-Level Permissions
-- BI Team: Full read on all tables (fact + dimension + gold aggregates)
-- Marketing Team: Read ONLY on Gold aggregate tables — NO access to raw fact_events
-- =============================================================================

-- BI Analyst: All tables
GRANT SELECT ON TABLE fact_events               TO ROLE BI_ANALYST_ROLE;
GRANT SELECT ON TABLE dim_date                  TO ROLE BI_ANALYST_ROLE;
GRANT SELECT ON TABLE dim_users                 TO ROLE BI_ANALYST_ROLE;
GRANT SELECT ON TABLE dim_products              TO ROLE BI_ANALYST_ROLE;
GRANT SELECT ON DYNAMIC TABLE gold_daily_active_users_dt TO ROLE BI_ANALYST_ROLE;
GRANT SELECT ON DYNAMIC TABLE gold_conversion_funnel_dt  TO ROLE BI_ANALYST_ROLE;
GRANT SELECT ON DYNAMIC TABLE gold_daily_revenue_dt      TO ROLE BI_ANALYST_ROLE;
GRANT SELECT ON DYNAMIC TABLE gold_top_products_dt       TO ROLE BI_ANALYST_ROLE;

-- Marketing: Aggregate tables ONLY (least privilege)
GRANT SELECT ON DYNAMIC TABLE gold_daily_active_users_dt TO ROLE MARKETING_ROLE;
GRANT SELECT ON DYNAMIC TABLE gold_conversion_funnel_dt  TO ROLE MARKETING_ROLE;
GRANT SELECT ON DYNAMIC TABLE gold_daily_revenue_dt      TO ROLE MARKETING_ROLE;

-- =============================================================================
-- Step 6: Assign Roles to Service Accounts / Users
-- =============================================================================
-- Create mock users so this script runs successfully in a portfolio/trial account
-- Note: We create them without passwords to avoid exposing secrets in source control.
CREATE USER IF NOT EXISTS tableau_svc_account;
CREATE USER IF NOT EXISTS marketing_analyst;

-- In production: Tableau connects via service account, not a personal user
GRANT ROLE BI_ANALYST_ROLE TO USER tableau_svc_account;
GRANT ROLE MARKETING_ROLE  TO USER marketing_analyst;

-- =============================================================================
-- Step 7: Verify Role Grants
-- =============================================================================
SHOW GRANTS TO ROLE BI_ANALYST_ROLE;
SHOW GRANTS TO ROLE MARKETING_ROLE;
SHOW GRANTS ON TABLE fact_events;
