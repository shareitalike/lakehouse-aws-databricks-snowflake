# Data Flow Deep Dive — Record-Level Walkthrough

Every event that flows through this platform goes through exactly 7 transformation steps. This document traces a single real event from producer to Snowflake dashboard.

---

## Sample Event

```json
{
  "event_id": "a3f9e2d1-8c4b-11ef-b864-0242ac120002",
  "event_time": "2026-04-10T14:23:45.123456",
  "user_id": 84231,
  "event_type": "purchase",
  "product_id": 1042,
  "price": 149.99,
  "device": "mobile",
  "country": "IN"
}
```

---

## Step 1: Producer → Kinesis

**Code:** `producer/event_generator.py`

The producer serializes the event to JSON and sends it to Kinesis with the `user_id` as the partition key (ensures all events for the same user arrive in order within a shard).

```python
kinesis_client.put_record(
    StreamName="retailedge-user-activity",
    Data=json.dumps(event).encode("utf-8"),
    PartitionKey=str(event["user_id"])  # user_id = partition key → ordered delivery per user
)
```

**What Kinesis does:**
- Assigns a `SequenceNumber` (monotonically increasing, per-shard)
- Stores for 24 hours (default retention)
- Buffers until Lambda polls the shard

**S3 state at this point:** Nothing yet. Event is in Kinesis memory.

---

## Step 2: Lambda Reads from Kinesis

**Code:** `lambda/transform_handler.py`

Lambda polls Kinesis every second (or is triggered by batch_size threshold). Receives a batch of up to 100 records. Each record's data is base64-encoded.

```python
# Kinesis payload per record
{
    "kinesis": {
        "sequenceNumber": "49627...",
        "data": "eyJldmVudF9pZCI6...",   # base64-encoded JSON
        "partitionKey": "84231"
    }
}

# After decode
raw = base64.b64decode(kinesis_record["kinesis"]["data"])
record = json.loads(raw)
# → {"event_id": "a3f9e2d1-...", "event_time": "2026-04-10...", ...}
```

---

## Step 3: Lambda Validates the Record

**Code:** `lambda/data_quality.py`

4-layer validation in fail-fast order:

| Check | This event | Result |
|-------|-----------|--------|
| `validate_schema` | All 6 required fields present | ✅ Pass |
| `validate_types` | event_id is str(≥8), event_time matches ISO pattern, user_id is int | ✅ Pass |
| `validate_enums` | event_type="purchase" ∈ valid set, device="mobile" ∈ valid set, country="IN" ∈ valid set | ✅ Pass |
| `detect_anomalies` | price=149.99 ∈ [0.01, 50000]; user_id=84231 ∈ [1, 1M]; event_time not future; purchase has product_id AND price | ✅ Pass |

Valid → Lambda enriches the record:

```python
record["ingestion_timestamp"] = "2026-04-10T08:53:45.312847+00:00"  # UTC wall clock
record["processing_date"] = "2026-04-10"                            # for partitioning
```

---

## Step 4: Lambda Writes to S3 Bronze

**Code:** `lambda/transform_handler.py` → `write_records_to_s3()`

Lambda collects all valid records in the batch and writes them as NDJSON (newline-delimited JSON) to a Hive-partitioned path in S3.

**S3 path:**
```
s3://retailedge-analytics-prod/
  bronze/events/
    year=2026/month=04/day=10/
      c7a3f201de4b11ef.json        ← UUID filename (no overwrite conflicts)
```

**File content** (one JSON object per line):
```json
{"event_id":"a3f9e2d1-8c4b-11ef-b864-0242ac120002","event_time":"2026-04-10T14:23:45.123456","user_id":84231,"event_type":"purchase","product_id":1042,"price":149.99,"device":"mobile","country":"IN","ingestion_timestamp":"2026-04-10T08:53:45.312847+00:00","processing_date":"2026-04-10"}
{"event_id":"b5e7c3a2-8c4b-11ef-...", ...}
...
```

**S3 state:** Event is now durably stored in Bronze. Immutable. Even if all downstream processing fails, we can replay from here.

---

## Step 5: Databricks — Bronze → Silver

**Code:** `databricks/bronze_to_silver.py`

Databricks reads all NDJSON files from `bronze/events/`. Applied in order:

### 5a. Schema Enforcement
```python
BRONZE_SCHEMA = StructType([
    StructField("event_id",           StringType(), nullable=False),
    StructField("event_time",         StringType(), nullable=False),
    ...
])
df_bronze = spark.read.schema(BRONZE_SCHEMA).json(BRONZE_PATH)
```
Files that don't match the schema → `null` values (caught in downstream null checks).

### 5b. Type Casting
```python
df_typed = df_bronze
    .withColumn("event_time",         F.to_timestamp("event_time"))
    .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
    .withColumn("processing_timestamp", F.current_timestamp())
```
`event_time` was a string in Bronze; becomes a proper `TimestampType` in Silver.

### 5c. Deduplication
```python
window_spec = Window.partitionBy("event_id").orderBy(F.col("ingestion_timestamp").asc())
df_deduped = df_typed
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
```
If `event_id = "a3f9e2d1-..."` appears twice (Kinesis at-least-once delivery), keep the one with the earliest `ingestion_timestamp`.

### 5d. Add Partition Columns + Write Delta
```python
df_silver = df_deduped
    .withColumn("year",  F.date_format("event_time", "yyyy"))
    .withColumn("month", F.date_format("event_time", "MM"))
    .withColumn("day",   F.date_format("event_time", "dd"))

df_silver.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .save(SILVER_PATH)
```

**S3 state after Bronze→Silver:**
```
s3://retailedge-analytics-prod/
  silver/events/
    year=2026/month=04/day=10/
      part-00000-a1b2c3.snappy.parquet   ← Parquet columnar format
      _delta_log/
        00000000000000000001.json        ← Delta transaction log
```

**Schema at Silver:**
```
event_id:             string
event_time:           timestamp     ← was string in Bronze
user_id:              integer
event_type:           string
product_id:           integer (nullable)
price:                double (nullable)
device:               string
country:              string
ingestion_timestamp:  timestamp     ← was string in Bronze
processing_date:      string
processing_timestamp: timestamp     ← added by Silver step
year:                 string        ← partition column
month:                string        ← partition column
day:                  string        ← partition column
```

---

## Step 6: Databricks — Silver → Gold

**Code:** `databricks/silver_to_gold.py`

### 6a. Watermark Read
```python
# Reads: {"last_processed_date": "2026-04-09", "updated_at": "..."}
last_processed = read_watermark(spark, WATERMARK_PATH)
# → "2026-04-09"
```

### 6b. Incremental Filter
```python
df_silver = spark.read.format("delta").load(SILVER_PATH)
    .filter(F.to_date(F.col("event_time")) > F.lit("2026-04-09"))
# → Only reads year=2026/month=04/day=10/ partition (partition pruning)
```

### 6c. Gold Table 1 — Daily Active Users
Our event:
- `user_id=84231`, `event_type=purchase`, `event_date=2026-04-10`

Aggregated with all other events for 2026-04-10:
```
event_date=2026-04-10:
  daily_active_users = COUNT(DISTINCT user_id) = 3842
  total_events       = COUNT(*)                = 47,291
  purchasing_users   = COUNT(DISTINCT user_id WHERE event_type='purchase') = 428
```

Written via Delta MERGE:
```python
dt_dau.alias("target").merge(
    df_dau.alias("source"),
    "target.event_date = source.event_date"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

If `event_date=2026-04-10` row existed → UPDATE. If not → INSERT.

### 6d. Gold Table 3 — Daily Revenue
Our event contributes:
```
event_date=2026-04-10:
  purchase_count  = 428
  total_revenue   = SUM(price WHERE event_type='purchase') = 64,218.37
  avg_order_value = 149.95
```

### 6e. Watermark Update
```python
write_watermark(spark, WATERMARK_PATH, "2026-04-10")
# → {"last_processed_date": "2026-04-10", "updated_at": "2026-04-10T09:15:23.000Z"}
```

**S3 state after Silver→Gold:**
```
s3://retailedge-analytics-prod/
  gold/
    daily_active_users/
      part-00000.snappy.parquet
    daily_revenue/
      part-00000.snappy.parquet
    conversion_funnel/
      part-00000.snappy.parquet
    top_products/
      part-00000.snappy.parquet
    events_by_device/
      part-00000.snappy.parquet
  metadata/
    last_processed_date.json    ← "last_processed_date": "2026-04-10"
```

---

## Step 7: Snowflake Load

**Code:** `snowflake/load_gold.sql`

```sql
COPY INTO gold_daily_revenue (
    event_date, purchase_count, total_revenue, avg_order_value, min_order, max_order, loaded_at
)
FROM (
    SELECT
        $1:event_date::DATE,
        $1:purchase_count::INT,
        $1:total_revenue::DECIMAL(12,2),
        $1:avg_order_value::DECIMAL(10,2),
        $1:min_order::DECIMAL(10,2),
        $1:max_order::DECIMAL(10,2),
        CURRENT_TIMESTAMP()
    FROM @gold_stage/daily_revenue/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = CONTINUE;
```

**Snowflake state:**
```sql
SELECT * FROM gold_daily_revenue WHERE event_date = '2026-04-10';
-- event_date    purchase_count  total_revenue  avg_order_value  loaded_at
-- 2026-04-10    428             64218.37       149.95           2026-04-10 09:18:44
```

**Dashboard query (Tableau / Looker):**
```sql
SELECT event_date, total_revenue, daily_active_users
FROM gold_daily_revenue r
JOIN gold_daily_active_users d USING (event_date)
ORDER BY event_date DESC
LIMIT 30;
-- Executes in 0.8s (cold), 0.1s (cached)
```

---

## Failure Scenarios Walkthrough

### Scenario 1: Lambda crashes mid-batch
- **What happens:** Half the batch is written to S3 Bronze, half not
- **Recovery:** Kinesis retries the entire batch from the same sequence number (at-least-once). Lambda processes duplicates. Bronze has duplicates. Silver dedup removes them.
- **Result:** No data loss. No manual intervention.

### Scenario 2: Databricks Bronze→Silver crashes mid-write
- **What happens:** Delta Lake transaction not committed. Silver is unchanged.
- **Recovery:** Rerun Bronze→Silver. Re-reads the same Bronze files. Re-deduplicates. Overwrites Silver cleanly.
- **Result:** Silver is consistent (ACID guarantee — partial write was rolled back).

### Scenario 3: Silver→Gold crashes after Gold write, before watermark update
- **What happens:** Gold tables have 2026-04-10's data. Watermark still says "2026-04-09".
- **Recovery:** Next run reads watermark "2026-04-09", reprocesses 2026-04-10 data, MERGE updates existing rows.
- **Result:** Same Gold values as if the first run succeeded. No double-counting.

### Scenario 4: Snowflake COPY INTO fails
- **What happens:** Snowflake tables are stale. S3 Gold is correct.
- **Recovery:** Rerun `COPY INTO`. Idempotent — same Parquet files produce same rows.
- **Result:** No data loss. Snowflake catch-up to S3 Gold state.
