# Snowflake Architecture & Analytics Layer

This directory contains the core Snowflake engineering scripts that power the analytics layer of our Lakehouse platform. 

As the platform matured and enterprise adoption grew, we evolved our architecture from standard imperative batch loading to **modern declarative pipelines** and implemented **enterprise-grade data governance**.

## Key Architectural Features Highlighted in this Module:

### 1. Declarative Pipelines (Dynamic Tables)
*   **File:** [`dynamic_tables.sql`](./dynamic_tables.sql)
*   Migrated legacy `MERGE` and `Tasks` to **Snowflake Dynamic Tables**.
*   This declarative approach guarantees a 1-hour data freshness SLA while significantly reducing the maintenance footprint and orchestration complexity.

### 2. Enterprise Governance & Security
*   **File:** [`governance_policies.sql`](./governance_policies.sql)
*   **Row-Level Security (RLS):** Implemented an entitlement-based Row Access Policy to ensure regional managers only query data belonging to their approved countries.
*   **Dynamic Data Masking:** Obfuscated PII (geo, device, and email identifiers) from standard business users while allowing full access for authorized BI administrators.

### 3. FinOps & Resource Optimization
*   **File:** [`cost_optimization.sql`](./cost_optimization.sql)
*   Implemented strict cost controls via **Resource Monitors** to alert at 75%/90% and suspend compute at 100% of our monthly credit budget.
*   **File:** [`performance_tuning.sql`](./performance_tuning.sql)
*   Enabled **Query Acceleration Service (QAS)** to offload heavy scan workloads for concurrent marketing dashboards without sizing up the warehouse. 
*   Added **Search Optimization Service (SOS)** for sub-second point lookups on large fact tables.

### 4. Data Sharing & Native ML
*   **File:** [`data_sharing.sql`](./data_sharing.sql)
*   Leveraged **Secure Views** to share curated product data securely with external retail partners (Zero-Copy Data Sharing).
*   **File:** [`ml_anomaly_detection.sql`](./ml_anomaly_detection.sql)
*   Utilized **Snowflake Cortex ML functions** directly in SQL to train an anomaly detection model that monitors daily revenue for sudden spikes or drops.

---
*Note: This layer acts as the Gold consumer of the AWS/Databricks Lakehouse processing engine.*
