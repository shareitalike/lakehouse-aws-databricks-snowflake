-- =============================================================================
-- [LEGACY PATTERN — For Reference] S3 → Snowflake Storage Integration & External Stage
-- =============================================================================
-- ⚠️  SUPERSEDED BY: databricks/workflows/snowflake_export.py
--
-- This script demonstrates the LEGACY SnowPipe ingestion pattern:
--   S3 Stage → COPY INTO → Snowflake
--
-- In the CURRENT architecture, Gold data is pushed directly from Databricks
-- to Snowflake using the Spark-Snowflake Connector (see snowflake_export.py).
-- This script is preserved as a reference to show the architectural evolution
-- and to demonstrate Snowflake SnowPro Core exam knowledge.
--
-- Interview Talking Point:
-- "We started with SnowPipe for event-driven S3 ingestion, but migrated to the
-- Spark-Snowflake Connector because it gives us Unity Catalog 3-level namespace
-- reads, Databricks Secrets integration, and fine-grained write mode control
-- (append vs overwrite), all within a single orchestrated Workflow task."
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- 1. Create the Storage Integration linking Snowflake to AWS IAM
CREATE OR REPLACE STORAGE INTEGRATION s3_silver_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::987684850401:role/snowflake-s3-access-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://retailedge-analytics-prod/silver/events/');

-- 2. Retrieve the Snowflake IAM User ARN and External ID to update the AWS Trust Policy
DESC INTEGRATION s3_silver_integration;
-- (Copy the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from the output)
-- (Update the AWS IAM Role Trust Policy with these exact values)

-- 3. Grant usage on the integration to the SYSADMIN role
GRANT USAGE ON INTEGRATION s3_silver_integration TO ROLE SYSADMIN;

-- 4. Switch to SYSADMIN to create the stage
USE ROLE SYSADMIN;
USE DATABASE EVENT_ANALYTICS;
USE SCHEMA SILVER;

-- 5. Create the External Stage using the Storage Integration (NO HARDCODED KEYS)
CREATE OR REPLACE STAGE silver_events_stage
  URL = 's3://retailedge-analytics-prod/silver/events/'
  STORAGE_INTEGRATION = s3_silver_integration
  FILE_FORMAT = (TYPE = PARQUET);

-- 6. Verify the stage can read the Delta Lake / Parquet files
LIST @silver_events_stage;
