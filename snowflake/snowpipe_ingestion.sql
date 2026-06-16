-- =============================================================================
-- Automated Ingestion Pipeline (Snowpipe)
-- Purpose: Continuously and automatically load Parquet files from S3 Silver Layer 
-- into the Snowflake base fact table (fact_events) as soon as they land.
-- =============================================================================

USE DATABASE EVENT_ANALYTICS;
USE SCHEMA ANALYTICS;

-- 1. Create a dedicated pipe for the fact_events table
CREATE OR REPLACE PIPE silver_to_fact_events_pipe
    AUTO_INGEST = TRUE
    -- In production, this SNS topic would be configured in AWS S3 Event Notifications
    -- AWS_SNS_TOPIC = 'arn:aws:sns:ap-southeast-7:987684850401:s3-silver-events-topic'
AS
COPY INTO fact_events (
    event_id, event_time, event_date, user_id, event_type, 
    product_id, price, device, country, ingestion_timestamp, processing_timestamp
)
FROM (
    SELECT 
        $1:event_id::VARCHAR,
        $1:event_time::TIMESTAMP,
        TO_DATE($1:event_time::TIMESTAMP), -- Extract DATE from TIMESTAMP
        $1:user_id::INT,
        $1:event_type::VARCHAR,
        $1:product_id::INT,
        $1:price::DECIMAL(10,2),
        $1:device::VARCHAR,
        $1:country::VARCHAR,
        $1:ingestion_timestamp::TIMESTAMP,
        $1:processing_timestamp::TIMESTAMP
    FROM @silver_events_stage/
)
PATTERN = '.*\.parquet'
ON_ERROR = CONTINUE;

-- 2. Verify the pipe was created successfully
SHOW PIPES LIKE 'silver_to_fact_events_pipe';

-- Note: To backfill historical data or run a one-time manual load instead of waiting for Snowpipe:
-- ALTER PIPE silver_to_fact_events_pipe REFRESH;
