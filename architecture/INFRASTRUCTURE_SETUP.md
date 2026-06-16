# Infrastructure Setup Guide
## RetailEdge Commerce — AWS Data Platform Provisioning

> **Audience:** Internal data team (post-consulting handover) and future engineers setting up a dev/staging environment.  
> All commands use AWS CLI v2. Region: `ap-southeast-7` (Mumbai). Account ID placeholder: `ACCOUNT_ID`.

---

## Prerequisites

```bash
# Verify tools
aws --version          # aws-cli/2.x.x
python3 --version      # Python 3.11+
databricks --version   # Databricks CLI 0.18+

# Configure AWS credentials
aws configure
# AWS Access Key ID: ****
# AWS Secret Access Key: ****
# Default region: ap-southeast-7
# Default output format: json
```

---

## Step 1: IAM Roles and Policies

### 1a: Lambda Execution Role

```bash
# Create trust policy
cat > /tmp/lambda-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

# Create the role
aws iam create-role \
    --role-name retailedge-lambda-kinesis-role \
    --assume-role-policy-document file:///tmp/lambda-trust-policy.json

# Attach managed policies
aws iam attach-role-policy \
    --role-name retailedge-lambda-kinesis-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole

aws iam attach-role-policy \
    --role-name retailedge-lambda-kinesis-role \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

# Verify
aws iam get-role --role-name retailedge-lambda-kinesis-role
```

### 1b: Databricks S3 Access (DBFS Mount)

```bash
# Create Databricks access policy
cat > /tmp/databricks-s3-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
    "Resource": [
      "arn:aws:s3:::retailedge-analytics-prod",
      "arn:aws:s3:::retailedge-analytics-prod/*"
    ]
  }]
}
EOF

aws iam create-policy \
    --policy-name retailedge-databricks-s3-policy \
    --policy-document file:///tmp/databricks-s3-policy.json
```

---

## Step 2: S3 Bucket

```bash
# Create bucket (must be globally unique)
aws s3 mb s3://retailedge-analytics-prod --region ap-southeast-7

# Enable versioning (recommended for audit trail)
aws s3api put-bucket-versioning \
    --bucket retailedge-analytics-prod \
    --versioning-configuration Status=Enabled

# Enable server-side encryption (AES-256)
aws s3api put-bucket-encryption \
    --bucket retailedge-analytics-prod \
    --server-side-encryption-configuration '{
        "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
    }'

# Block public access
aws s3api put-public-access-block \
    --bucket retailedge-analytics-prod \
    --public-access-block-configuration \
        "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Create folder structure
for PREFIX in "bronze/events" "silver/events" "gold/daily_active_users" \
              "gold/conversion_funnel" "gold/daily_revenue" \
              "gold/top_products" "gold/events_by_device" \
              "quarantine/events" "metadata" "athena-results"; do
    aws s3api put-object \
        --bucket retailedge-analytics-prod \
        --key "${PREFIX}/"
done

# Verify structure
aws s3 ls s3://retailedge-analytics-prod/ --recursive
```

### Lifecycle Policy (Cost Optimization)

```bash
# Move quarantine data to Glacier after 90 days (rarely accessed)
cat > /tmp/lifecycle-policy.json << 'EOF'
{
  "Rules": [
    {
      "ID": "quarantine-to-glacier",
      "Filter": {"Prefix": "quarantine/"},
      "Status": "Enabled",
      "Transitions": [{"Days": 90, "StorageClass": "GLACIER"}]
    },
    {
      "ID": "bronze-to-ia",
      "Filter": {"Prefix": "bronze/"},
      "Status": "Enabled",
      "Transitions": [{"Days": 30, "StorageClass": "STANDARD_IA"}]
    }
  ]
}
EOF

aws s3api put-bucket-lifecycle-configuration \
    --bucket retailedge-analytics-prod \
    --lifecycle-configuration file:///tmp/lifecycle-policy.json
```

---

## Step 3: Kinesis Data Stream

```bash
# Create stream (1 shard = 1 MB/s ingress, 2 MB/s egress, 1000 records/sec)
aws kinesis create-stream \
    --stream-name retailedge-user-activity \
    --shard-count 1 \
    --region ap-southeast-7

# Wait for stream to become ACTIVE
aws kinesis wait stream-exists \
    --stream-name retailedge-user-activity

# Verify
aws kinesis describe-stream-summary \
    --stream-name retailedge-user-activity

# Enable enhanced monitoring (for CloudWatch metrics)
aws kinesis enable-enhanced-monitoring \
    --stream-name retailedge-user-activity \
    --shard-level-metrics IncomingBytes IncomingRecords GetRecords.IteratorAgeMilliseconds
```

### Scaling Kinesis (When Needed)

```bash
# If throughput exceeds 80% of shard capacity, add shards
aws kinesis update-shard-count \
    --stream-name retailedge-user-activity \
    --target-shard-count 2 \
    --scaling-type UNIFORM_SCALING
```

---

## Step 4: Lambda Function

```bash
# Package Lambda
cd lambda/
pip install boto3 -t ./package/ --quiet
cp transform_handler.py data_quality.py ./package/
cd package/ && zip -r ../lambda_package.zip . -x "*.pyc" -x "__pycache__/*"
cd ..

# Get Lambda role ARN
LAMBDA_ROLE_ARN=$(aws iam get-role \
    --role-name retailedge-lambda-kinesis-role \
    --query 'Role.Arn' --output text)

# Deploy Lambda
aws lambda create-function \
    --function-name retailedge-event-processor \
    --runtime python3.11 \
    --role $LAMBDA_ROLE_ARN \
    --handler transform_handler.lambda_handler \
    --zip-file fileb://lambda_package.zip \
    --timeout 60 \
    --memory-size 256 \
    --environment Variables="{
        S3_BUCKET=retailedge-analytics-prod,
        BRONZE_PREFIX=bronze/events,
        QUARANTINE_PREFIX=quarantine/events
    }" \
    --description "RetailEdge event processor: validates Kinesis records, routes to S3 bronze/quarantine"

# Get Kinesis stream ARN
KINESIS_ARN=$(aws kinesis describe-stream-summary \
    --stream-name retailedge-user-activity \
    --query 'StreamDescriptionSummary.StreamARN' --output text)

# Create Kinesis trigger
aws lambda create-event-source-mapping \
    --function-name retailedge-event-processor \
    --event-source-arn $KINESIS_ARN \
    --starting-position TRIM_HORIZON \
    --batch-size 100 \
    --maximum-batching-window-in-seconds 5

# Configure Dead Letter Queue (DLQ) for unprocessable records
aws sqs create-queue --queue-name retailedge-lambda-dlq

DLQ_ARN=$(aws sqs get-queue-attributes \
    --queue-url $(aws sqs get-queue-url --queue-name retailedge-lambda-dlq --query 'QueueUrl' --output text) \
    --attribute-names QueueArn \
    --query 'Attributes.QueueArn' --output text)

aws lambda update-function-configuration \
    --function-name retailedge-event-processor \
    --dead-letter-config TargetArn=$DLQ_ARN
```

### Update Lambda After Code Changes

```bash
# Rebuild and update
cd package/ && zip -r ../lambda_package.zip . && cd ..
aws lambda update-function-code \
    --function-name retailedge-event-processor \
    --zip-file fileb://lambda_package.zip
```

---

## Step 5: CloudWatch Alarms

```bash
# Alarm: Kinesis iterator age too high (processing lagging)
aws cloudwatch put-metric-alarm \
    --alarm-name "retailedge-kinesis-lag-high" \
    --metric-name GetRecords.IteratorAgeMilliseconds \
    --namespace AWS/Kinesis \
    --dimensions Name=StreamName,Value=retailedge-user-activity \
    --statistic Maximum \
    --period 300 \
    --threshold 300000 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 2 \
    --alarm-description "Kinesis consumer is lagging > 5 minutes"

# Alarm: Lambda error rate
aws cloudwatch put-metric-alarm \
    --alarm-name "retailedge-lambda-errors" \
    --metric-name Errors \
    --namespace AWS/Lambda \
    --dimensions Name=FunctionName,Value=retailedge-event-processor \
    --statistic Sum \
    --period 300 \
    --threshold 10 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 1 \
    --alarm-description "Lambda errors > 10 in 5 minutes"
```

---

## Step 6: Databricks S3 Mount (Community Edition)

In a Databricks notebook, run this once to mount S3:

```python
# In Databricks notebook — run once
dbutils.fs.mount(
    source="s3a://retailedge-analytics-prod",
    mount_point="/mnt/retailedge",
    extra_configs={
        "fs.s3a.access.key": dbutils.secrets.get(scope="aws", key="access-key"),
        "fs.s3a.secret.key": dbutils.secrets.get(scope="aws", key="secret-key"),
    }
)

# After mount, use /mnt/retailedge/silver/events instead of s3a:// paths
# Community Edition: use environment variables or hardcode for local testing only
```

---

## Step 7: Snowflake Stage + Permissions

```sql
-- Run in Snowflake as ACCOUNTADMIN
-- Create dedicated role for data loading
CREATE ROLE IF NOT EXISTS DATA_LOADER;
GRANT ROLE DATA_LOADER TO USER your_username;

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE COMPUTE_XS TO ROLE DATA_LOADER;

-- Grant database + schema access
GRANT USAGE ON DATABASE EVENT_ANALYTICS TO ROLE DATA_LOADER;
GRANT USAGE ON SCHEMA EVENT_ANALYTICS.ANALYTICS TO ROLE DATA_LOADER;
GRANT INSERT, UPDATE, SELECT ON ALL TABLES IN SCHEMA EVENT_ANALYTICS.ANALYTICS TO ROLE DATA_LOADER;

-- Create storage integration (more secure than hardcoded keys)
CREATE STORAGE INTEGRATION retailedge_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://retailedge-analytics-prod/gold/');

-- Get Snowflake's IAM details to update AWS trust policy
DESC INTEGRATION retailedge_s3_integration;
-- Note STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
```

---

## Verification Checklist

After completing all steps, verify end-to-end:

```bash
# 1. Produce 100 test events
python producer/event_generator.py --total-events 100 --eps 5

# 2. Check Kinesis received them
aws kinesis get-records \
    --shard-iterator $(aws kinesis get-shard-iterator \
        --stream-name retailedge-user-activity \
        --shard-id shardId-000000000000 \
        --shard-iterator-type TRIM_HORIZON \
        --query 'ShardIterator' --output text) \
    --query 'Records | length(@)'

# 3. Check Bronze S3
aws s3 ls s3://retailedge-analytics-prod/bronze/events/ --recursive

# 4. Check Quarantine S3
aws s3 ls s3://retailedge-analytics-prod/quarantine/events/ --recursive

# 5. Check Lambda metrics
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Invocations \
    --dimensions Name=FunctionName,Value=retailedge-event-processor \
    --start-time $(date -u -v-1H +%Y-%m-%dT%H:%M:%SZ) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
    --period 300 \
    --statistics Sum
```

---

## Cost Monitoring

```bash
# Get current month's costs by service
aws ce get-cost-and-usage \
    --time-period Start=$(date +%Y-%m-01),End=$(date +%Y-%m-%d) \
    --granularity MONTHLY \
    --metrics UnblendedCost \
    --group-by Type=DIMENSION,Key=SERVICE \
    --query 'ResultsByTime[0].Groups[*].[Keys[0],Metrics.UnblendedCost.Amount]' \
    --output table
```

**Expected monthly costs:**

| Service | Expected | Alert if > |
|---------|---------|-----------|
| Kinesis (1 shard) | ~$11 | $15 |
| Lambda | < $1 | $5 |
| S3 | < $2 | $5 |
| CloudWatch | < $1 | $3 |
| **Total** | **~$15** | **$25** |
