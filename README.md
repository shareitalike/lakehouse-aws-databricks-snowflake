<div align="center">

# RetailEdge — Enterprise Lakehouse Pipeline

### AWS · Databricks · Delta Lake · Snowflake · Unity Catalog

[![AWS](https://img.shields.io/badge/AWS-us--east--1-FF9900?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/)
[![Databricks](https://img.shields.io/badge/Databricks-14.3%20LTS-FF3621?logo=databricks&logoColor=white)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.x-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake&logoColor=white)](https://snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://python.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

**Near real-time e-commerce event analytics platform built on the Medallion Lakehouse architecture.**  
Reduced pipeline latency from **8 hours → 15 minutes** with fully declarative, observable, and governed data pipelines.

</div>

---

## Overview

RetailEdge is a production-grade, end-to-end data engineering platform for a high-volume e-commerce business. The architecture ingests streaming clickstream events from AWS Kinesis, processes them through a Medallion Lakehouse (Bronze → Silver → Gold) using Databricks Delta Live Tables, and serves curated analytics data to business intelligence teams via Snowflake.

**Key outcomes:**
- ⚡ Pipeline latency reduced from **8 hours to under 15 minutes**
- 🛡️ Zero PII exposure via Unity Catalog column masking + Snowflake Row Access Policies
- 💰 ~40% infrastructure cost reduction via Liquid Clustering, Triggered DLT mode, and Snowflake Resource Monitors
- 🔄 **5-8% of silently dropped events** recovered via an automated Quarantine Recovery pipeline

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER  ·  AWS  ·  us-east-1                                      │
│                                                                             │
│  Event Producer ──▶  Kinesis Data Streams ──▶  AWS Lambda                   │
│  (Python simulator)   (1 shard · 24h retention)  (4-layer DQ validation)   │
│                                                      │             │        │
│                                              ┌───────┘     ┌──────┘        │
│                                              ▼             ▼               │
│                                         S3 Bronze     S3 Quarantine        │
│                                         (valid NDJSON) (rejected events)   │
└─────────────────────────────────────────────────────────────────────────────┘
                                               │
┌──────────────────────────────────────────────▼──────────────────────────────┐
│  PROCESSING LAYER  ·  Databricks Data Intelligence Platform                 │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Delta Live Tables Pipeline  (Triggered · Declarative ETL)          │   │
│  │                                                                     │   │
│  │  Bronze  ──▶  Silver  ──▶  Gold                                    │   │
│  │  Auto Loader  CDC MERGE    5 Business Aggregation Tables            │   │
│  │  cloudFiles   apply_changes  DAU · Revenue · Funnel · Products      │   │
│  │              expect_or_drop  Device Mix                              │  │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
│  Databricks Workflows (5-task DAG, Asset Bundle IaC)                       │
│  DLT Pipeline → Optimize + Quarantine Recovery → Snowflake Export → Health  │
└─────────────────────────────────────────────────────────────────────────────┘
                                               │
┌──────────────────────────────────────────────▼──────────────────────────────┐
│  GOVERNANCE LAYER  ·  Unity Catalog                                         │
│                                                                             │
│  retailedge.bronze.*  ·  retailedge.silver.*  ·  retailedge.gold.*          │
│  External Locations  ·  RBAC GRANTs/DENYs  ·  Column Masking                │
│  System Lineage (system.lineage.table_lineage)  ·  Access Audit Log         │
└─────────────────────────────────────────────────────────────────────────────┘
                                               │
┌──────────────────────────────────────────────▼──────────────────────────────┐
│  SERVING LAYER  ·  Snowflake  (Best-of-Breed BI & Analytics)                │
│                                                                             │
│  Spark-Snowflake Connector  →  Dynamic Tables (TARGET_LAG: 1h – 12h)        │
│  Row Access Policies (entitlement-based RLS)                                │
│  Dynamic Data Masking  ·  Snowflake Cortex ML  ·  Secure Data Sharing       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| **Streaming** | Amazon Kinesis | — | Ordered, durable event stream |
| **Ingestion** | AWS Lambda | Python 3.11 | Serverless 4-layer data quality gate |
| **Storage** | Amazon S3 | — | Decoupled compute/storage Lakehouse |
| **Processing** | Databricks DLT | 14.3 LTS | Declarative pipelines, Auto Loader, CDC |
| **Table Format** | Delta Lake | 3.x | ACID, schema evolution, time travel, CDF |
| **Governance** | Unity Catalog | — | RBAC, column masking, system lineage |
| **Orchestration** | Databricks Workflows | — | DAG scheduling, DABs IaC deployment |
| **Data Warehouse** | Snowflake | — | Dynamic Tables, RLS, Cortex ML, Data Sharing |
| **IaC** | Asset Bundles (DABs) | ≥ 0.209 | One-command CI/CD deployment |

---

## Pipeline DAG (Databricks Workflow)

```
run_dlt_pipeline
       │
       ├──────────────────────────┐
       ▼                          ▼
optimize_delta_tables    quarantine_recovery
       │                          │
       └──────────┬───────────────┘
                  ▼
         export_to_snowflake
                  │
                  ▼
        pipeline_health_check
```

All tasks run on a shared `m5d.large` SPOT cluster in `us-east-1a` — minimising DBU cost while maintaining production reliability.

---

## Repository Structure

```
lakehouse-aws-databricks-snowflake/
│
├── databricks.yml                      ← Asset Bundle: full pipeline IaC
├── config/
│   └── config.yaml                     ← Central config (AWS, Databricks, Snowflake)
│
├── databricks/
│   ├── dlt/
│   │   ├── pipeline_bronze_to_silver.py ← Auto Loader ingestion + CDC deduplication
│   │   └── pipeline_silver_to_gold.py   ← 3 Gold business aggregation tables
│   │
│   ├── workflows/
│   │   ├── optimize_tables.py           ← OPTIMIZE + VACUUM: all 3 Medallion layers
│   │   ├── quarantine_recovery.py       ← DLQ processor: repair → Bronze / dead-letter
│   │   └── snowflake_export.py          ← Spark-Snowflake connector export
│   │
│   ├── observability/
│   │   ├── dlt_pipeline_health.py       ← DLT Event Log SLA & expectation failure report
│   │   ├── schema_evolution_runbook.py  ← mergeSchema + Auto Loader evolution runbook
│   │   └── cost_optimization_finops.py  ← Liquid Clustering, CDF, FinOps framework
│   │
│   ├── unity_catalog/
│   │   ├── setup_unity_catalog.sql      ← External Location, Catalog, RBAC, masking
│   │   └── system_lineage_queries.sql   ← Compliance lineage + access audit queries
│   │
│   └── setup/
│       └── secrets_setup_guide.py       ← Databricks CLI secret scope setup reference
│
├── snowflake/
│   ├── schema.sql                       ← Database, schemas, warehouse, table DDL
│   ├── dynamic_tables.sql               ← Declarative aggregations with TARGET_LAG SLA
│   ├── rbac_governance.sql              ← Roles, warehouses, least-privilege grants
│   ├── governance_policies.sql          ← Column masking + row access policies
│   ├── performance_tuning.sql           ← Clustering, result cache, query profiling
│   ├── cost_optimization.sql            ← Resource monitors, auto-suspend strategy
│   ├── data_sharing.sql                 ← Secure Data Sharing with partner accounts
│   ├── ml_anomaly_detection.sql         ← Snowflake Cortex ML on revenue metrics
│   ├── analytics_queries_annotated.sql  ← Funnel, cohort, and revenue BI queries
│   ├── storage_integration_setup.sql    ← [Legacy] SnowPipe S3 integration reference
│   └── legacy_load_gold_batch.sql       ← [Legacy] Pre-DLT batch pattern (reference)
│
├── lambda/
│   ├── transform_handler.py             ← Lambda entry point + Kinesis routing logic
│   └── data_quality.py                  ← Reusable 4-layer validation pipeline
│
└── producer/
    └── event_generator.py               ← Kinesis event simulator (5,000 events/run)
```

---

## Engineering Highlights

### 1 · Delta Live Tables — Declarative ETL
Replaced a fragile manual PySpark pipeline (`foreachBatch` + handwritten `MERGE INTO`) with a fully declarative DLT pipeline:

```python
# CDC deduplication in 5 lines vs 40+ lines of manual MERGE boilerplate
dlt.apply_changes(
    target="events_silver",
    source="events_silver_cleaned",
    keys=["event_id"],
    sequence_by="ingestion_timestamp"
)
```

Data quality enforced declaratively — rows failing expectations are dropped and logged to the DLT Event Log automatically:
```python
@dlt.expect_or_drop("valid_event_id",   "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
```

### 2 · Unity Catalog — Enterprise Governance
Three-level namespace with explicit access boundaries:

```sql
-- Engineers: full access to build pipelines
GRANT ALL PRIVILEGES ON CATALOG retailedge TO `data_engineers`;

-- Analysts: read-only on curated Gold layer only
GRANT SELECT ON SCHEMA retailedge.gold TO `data_analysts`;
DENY  SELECT ON SCHEMA retailedge.bronze TO `data_analysts`;

-- PII masking evaluated at query time, not at table creation
ALTER TABLE retailedge.gold.daily_active_users
ALTER COLUMN user_id SET MASK retailedge.gold.mask_user_id;
```

### 3 · Quarantine Recovery Pipeline
Events rejected by Lambda (5–8% of volume) are automatically triaged rather than discarded:

| Classification | Rejection Reason | Action |
|---------------|-----------------|--------|
| Repairable | `anomaly_login_has_price` | Null out price field, re-route to Bronze |
| Repairable | `anomaly_future_timestamp` | Replace with `ingestion_timestamp`, re-route to Bronze |
| Unrecoverable | `missing_fields:event_id` | Write to Dead Letter Delta table for review |

### 4 · Snowflake — Best-of-Breed Serving Layer
Gold Delta tables are exported to Snowflake via the Spark connector. Snowflake Dynamic Tables then maintain analyst-facing aggregations with tiered freshness SLAs — zero orchestration required:

```sql
CREATE OR REPLACE DYNAMIC TABLE gold_daily_revenue_dt
    TARGET_LAG = '1 hour'   -- Snowflake refreshes this automatically
    WAREHOUSE  = COMPUTE_XS
AS SELECT event_date, SUM(price) AS total_revenue, COUNT(*) AS purchase_count
   FROM fact_events WHERE event_type = 'purchase' GROUP BY event_date;
```

### 5 · Asset Bundle IaC (DABs)
The entire pipeline, workflow DAG, and cluster configuration is version-controlled in `databricks.yml`. Deploy from any environment with a single command:

```bash
databricks bundle deploy               # push pipeline + workflow to workspace
databricks bundle run retailedge_daily_run  # trigger the full orchestration
```

---

## Quick Start

### Prerequisites
- AWS account (`us-east-1`) with IAM AdministratorAccess
- Databricks Enterprise workspace on AWS
- Snowflake account (any edition)
- Databricks CLI ≥ 0.209 (`pip install databricks-cli`)

### Step 1 — AWS Infrastructure
```bash
# Deploy Lambda function
cd lambda && pip install -r ../requirements.txt -t package/
zip -r lambda_package.zip transform_handler.py data_quality.py package/
aws lambda create-function --function-name kinesis-event-processor \
  --runtime python3.11 --zip-file fileb://lambda_package.zip \
  --role arn:aws:iam::<ACCOUNT_ID>:role/lambda-kinesis-role \
  --handler transform_handler.lambda_handler --region us-east-1
```

### Step 2 — Databricks Secrets
```bash
databricks secrets create-scope --scope snowflake
databricks secrets put --scope snowflake --key sfURL      # your Snowflake URL
databricks secrets put --scope snowflake --key sfUser
databricks secrets put --scope snowflake --key sfPassword
databricks secrets create-scope --scope aws
databricks secrets put --scope aws --key s3_bucket_name   # retailedge-analytics-prod
databricks secrets put --scope aws --key aws_region       # us-east-1
```

### Step 3 — Unity Catalog
Open your Databricks workspace → SQL Editor → run `databricks/unity_catalog/setup_unity_catalog.sql`

### Step 4 — Deploy & Run
```bash
# Edit databricks.yml: set your workspace URL under targets.dev.workspace.host
databricks bundle deploy
databricks bundle run retailedge_daily_run
```

### Step 5 — Snowflake Layer
Run scripts in this order in your Snowflake worksheet:
1. `snowflake/schema.sql`
2. `snowflake/rbac_governance.sql`
3. `snowflake/dynamic_tables.sql`
4. `snowflake/governance_policies.sql`

---

## FinOps Summary

| Optimisation | Platform | Saving |
|---|---|---|
| Triggered DLT (vs Continuous) | Databricks | ~70% DBU reduction |
| Liquid Clustering (vs ZORDER) | Databricks | ~40% OPTIMIZE cost reduction |
| SPOT cluster with fallback | Databricks | ~60% job cluster savings |
| Auto-suspend 60s | Snowflake | Near-zero idle warehouse cost |
| Dynamic Table lag tiering (1h / 4h / 12h) | Snowflake | ~55% DT refresh credit reduction |
| Resource Monitor (20 credit hard cap) | Snowflake | Prevents runaway query spend |

---

## License

MIT License — see [LICENSE](LICENSE) for details.
