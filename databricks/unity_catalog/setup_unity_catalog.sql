-- ==============================================================================
-- Unity Catalog Setup & Governance
-- ==============================================================================
-- Purpose:
--   Establishes the 3-level namespace (Catalog → Schema → Table), AWS S3
--   External Location, RBAC grants, and column masking for the RetailEdge project.
--   Run this notebook ONCE before deploying the DLT pipeline.
--
-- Requires: ACCOUNTADMIN or METASTORE_ADMIN privilege in Databricks.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. Create Catalog & Schemas
-- ------------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS retailedge;
USE CATALOG retailedge;

CREATE SCHEMA IF NOT EXISTS bronze
    COMMENT 'Raw, unprocessed events straight from S3 (Auto Loader)';

CREATE SCHEMA IF NOT EXISTS silver
    COMMENT 'Cleansed, deduplicated, and typed events (DLT output)';

CREATE SCHEMA IF NOT EXISTS gold
    COMMENT 'Business-level aggregations and serving tables (DLT output)';

-- ------------------------------------------------------------------------------
-- 2. AWS S3 External Location Setup
-- ------------------------------------------------------------------------------
-- NOTE: Before running this, you must have created a "Storage Credential" in 
-- the Databricks UI using the AWS IAM Role we discussed.
-- Replace `<YOUR_AWS_ACCOUNT_ID>` below.

CREATE EXTERNAL LOCATION IF NOT EXISTS retailedge_s3_storage
    URL 's3://retailedge-analytics-prod/'
    WITH (STORAGE CREDENTIAL `aws-databricks-storage-role`)
    COMMENT 'Main S3 data lake storage for RetailEdge project';

-- ------------------------------------------------------------------------------
-- 3. Role-Based Access Control (RBAC)
-- ------------------------------------------------------------------------------
-- Create groups (these would normally sync from Azure AD or AWS IAM Identity Center)
-- Example: CREATE GROUP data_engineers; CREATE GROUP data_analysts;

-- Grant Engineers full control over the catalog to build pipelines
GRANT ALL PRIVILEGES ON CATALOG retailedge TO `data_engineers`;

-- Grant Analysts read-only access strictly to the Gold layer (Serving)
GRANT USAGE ON CATALOG retailedge TO `data_analysts`;
GRANT USAGE ON SCHEMA retailedge.gold TO `data_analysts`;
GRANT SELECT ON SCHEMA retailedge.gold TO `data_analysts`;

-- Explicitly deny Analysts access to raw PII in Bronze
DENY SELECT ON SCHEMA retailedge.bronze TO `data_analysts`;

-- ------------------------------------------------------------------------------
-- 4. Dynamic Column Masking (Security & Compliance)
-- ------------------------------------------------------------------------------
-- Column-level masking function: returns the real user_id for data engineers,
-- masked value for all other roles. Apply with ALTER TABLE ... SET MASK.

CREATE OR REPLACE FUNCTION retailedge.gold.mask_user_id(user_id STRING)
  RETURN CASE
    WHEN is_account_group_member('data_engineers') THEN user_id
    ELSE '***-MASKED-***'
  END;

-- To apply this (after the DLT pipeline runs and creates the table):
-- ALTER TABLE retailedge.gold.daily_active_users 
-- ALTER COLUMN user_id SET MASK retailedge.gold.mask_user_id;
