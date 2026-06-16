# Databricks Processing Layer — Bronze → Silver

This directory contains the PySpark notebooks for the core data processing layer of the RetailEdge Lakehouse. These scripts transform raw, unvalidated JSON events from the S3 Bronze layer into clean, deduplicated, query-optimized Delta Lake tables in the S3 Silver layer.

---

## 📂 Files

| File | Purpose |
|------|---------|
| `bronze_to_silver.py` | **Primary batch job.** Schema enforcement, deduplication via Window functions, Delta Lake write. |
| `silver_to_gold.py` | **Aggregation job.** Watermark-based incremental reads → 5 Gold aggregation tables via Delta MERGE. |
| `schemas.py` | **Central schema registry.** All `StructType` schema definitions live here. Never define schemas inline. |
| `streaming_bronze_to_silver.py` | **Streaming variant.** Structured Streaming alternative to the batch job for sub-minute latency. |

---

## ▶️ Execution Order

```
1. bronze_to_silver.py     ← Run first (produces Silver Delta tables)
2. silver_to_gold.py       ← Run after (reads Silver, writes Gold aggregations)
```

> ⚠️ Both scripts must be run in this exact order. `silver_to_gold.py` reads from the Delta tables created by `bronze_to_silver.py`.

---

## 🏗️ Key Design Decisions

### Why PySpark over SQL-only?
Deduplication logic requires stateful `Window` functions with `ROW_NUMBER()` partitioned by `event_id` and ordered by `ingestion_timestamp`. This complex, distributed operation is natural in PySpark but awkward and slow in pure SQL engines.

### Why Delta Lake over Parquet?
Delta Lake adds three features that Parquet cannot provide:
1. **ACID MERGE** — idempotent upserts (safe to re-run on failures)
2. **Schema enforcement** — rejects incompatible writes at write time
3. **Time Travel** — `VERSION AS OF` for debugging production data issues

### Why Incremental Processing (Watermark)?
The watermark in `metadata/watermark.json` stores the last processed date. `silver_to_gold.py` reads only data after this date, so:
- At 5,000 events/day: runs in seconds
- At 50M events/day: runs in minutes (not hours like a full recompute)

### Why not Databricks Delta Live Tables (DLT)?
DLT is an excellent declarative pipeline framework but requires a **paid Databricks workspace** (not available in Community Edition). Since Snowflake Dynamic Tables handle our declarative Gold pipeline needs in the serving layer, we deliberately kept the Databricks layer as standard PySpark to avoid double-paying for orchestration capabilities.

---

## 🚀 How to Run (Databricks Community Edition)

1. Log in to [Databricks Community Edition](https://community.cloud.databricks.com/)
2. Create a cluster: **Runtime 13.3 LTS, single-node**
3. Mount S3 in a separate notebook cell:
   ```python
   dbutils.fs.mount(
       source="s3a://YOUR_ACCESS_KEY:YOUR_SECRET_KEY@retailedge-analytics-prod",
       mount_point="/mnt/s3_prod"
   )
   ```
4. Import `bronze_to_silver.py` → update `S3_BUCKET` variable → Run All
5. Import `silver_to_gold.py` → update `S3_BUCKET_NAME` variable → Run All
