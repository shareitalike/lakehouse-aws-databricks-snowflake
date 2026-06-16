-- =============================================================================
-- Snowflake Governance & Security (Senior Level) — RetailEdge Commerce Project
-- Purpose: Implements enterprise-grade Row-Level Security (RLS) and Dynamic Data Masking.
-- =============================================================================
USE DATABASE EVENT_ANALYTICS;
USE SCHEMA ANALYTICS;

-- =============================================================================
-- 1. DYNAMIC DATA MASKING (PII Protection)
-- =============================================================================
-- Business Rule: BI_ANALYST_ROLE can see raw data. MARKETING_ROLE sees masked data.

-- Create the masking policy for emails/PII identifiers
CREATE OR REPLACE MASKING POLICY email_mask AS (val string) RETURNS string ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'BI_ANALYST_ROLE') THEN val
        ELSE '***MASKED***'
    END;

-- Create the masking policy for sensitive geographical/device data
CREATE OR REPLACE MASKING POLICY geo_device_mask AS (val string) RETURNS string ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'BI_ANALYST_ROLE') THEN val
        ELSE SHA2(val) -- One-way hash so it can still be used for counting distinct values
    END;

-- Apply masking policies to the Dimension tables
-- Note: Applying to dim_users assuming we have a primary_country and primary_device
ALTER TABLE dim_users MODIFY COLUMN primary_device SET MASKING POLICY geo_device_mask;
ALTER TABLE dim_users MODIFY COLUMN primary_country SET MASKING POLICY geo_device_mask;


-- =============================================================================
-- 2. ROW-LEVEL SECURITY (RLS)
-- =============================================================================
-- Business Rule: Regional Managers should only see sales data for their specific country.

-- Create an Entitlement Mapping Table
CREATE TABLE IF NOT EXISTS security_entitlements (
    user_name VARCHAR(100),
    country_code VARCHAR(5)
);

-- Insert sample entitlements
INSERT INTO security_entitlements (user_name, country_code) VALUES
    ('us_manager@company.com', 'US'),
    ('uk_manager@company.com', 'UK'),
    ('global_vp@company.com', 'ALL'); -- 'ALL' grants access to everything

-- Create the Row Access Policy
CREATE OR REPLACE ROW ACCESS POLICY country_rls_policy AS (event_country VARCHAR) RETURNS BOOLEAN ->
    EXISTS (
        SELECT 1 FROM security_entitlements
        WHERE user_name = CURRENT_USER()
          AND (country_code = event_country OR country_code = 'ALL')
    )
    OR CURRENT_ROLE() = 'ACCOUNTADMIN'; -- Break-glass admin override

-- Apply the Row Access Policy to the Fact table
ALTER TABLE fact_events ADD ROW ACCESS POLICY country_rls_policy ON (country);

-- Verify policy assignment
SELECT * FROM TABLE(
  INFORMATION_SCHEMA.POLICY_REFERENCES(
    POLICY_NAME => 'country_rls_policy'
  )
);
