-- =============================================================================
-- Snowflake Cortex (AI/ML) — RetailEdge Commerce Project
-- Purpose: Implement native ML Anomaly Detection on daily revenue to automatically
-- flag sudden drops or spikes in sales.
-- =============================================================================
USE DATABASE EVENT_ANALYTICS;
USE SCHEMA ANALYTICS;
USE WAREHOUSE COMPUTE_XS;

-- =============================================================================
-- 1. Create a View for the ML Model Training Data
-- =============================================================================
-- The model requires a timestamp column and a target metric column
CREATE OR REPLACE VIEW revenue_training_data_vw AS
SELECT 
    event_date::TIMESTAMP_NTZ AS ts, 
    total_revenue AS y
FROM gold_daily_revenue_dt;

-- =============================================================================
-- 2. Train the Anomaly Detection Model
-- =============================================================================
-- We train the model using Snowflake's built-in ML functions. 
-- No external Python infrastructure required!
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION revenue_anomaly_model(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'revenue_training_data_vw'),
    TIMESTAMP_COLNAME => 'ts',
    TARGET_COLNAME => 'y',
    LABEL_COLNAME => ''
);

-- =============================================================================
-- 3. Detect Anomalies (Inference)
-- =============================================================================
-- Call the model to detect anomalies on the last 7 days of data
SELECT * FROM TABLE(revenue_anomaly_model!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'revenue_training_data_vw'),
    TIMESTAMP_COLNAME => 'ts',
    TARGET_COLNAME => 'y'
))
WHERE is_anomaly = TRUE;

-- Note: In a production pipeline, this query would be scheduled via a Task
-- to send an email/Slack alert when an anomaly is detected.
