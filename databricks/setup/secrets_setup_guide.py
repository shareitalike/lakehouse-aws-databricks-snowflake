# Databricks notebook source
# ==============================================================================
# Secrets Setup Guide — Databricks CLI Reference
# ==============================================================================
# Purpose:
#   One-time setup of Databricks Secret Scopes backed by AWS Secrets Manager.
#   All notebooks reference secrets via dbutils.secrets.get() — no credentials
#   are ever stored in code, logs, or version control.
#
# This file is a CLI reference guide, NOT a runnable notebook.
# Execute these commands in your local terminal using the Databricks CLI.
#
# Prerequisites:
#   - Databricks CLI installed  (pip install databricks-cli)
#   - Personal Access Token from your Databricks workspace
# ==============================================================================

# ------------------------------------------------------------------------------
# Step 1: Authenticate the CLI
# ------------------------------------------------------------------------------
# databricks configure --token
# (Enter your Databricks workspace URL and Personal Access Token when prompted)

# ------------------------------------------------------------------------------
# Step 2: Create Secret Scopes
# ------------------------------------------------------------------------------
# databricks secrets create-scope --scope snowflake
# databricks secrets create-scope --scope aws

# ------------------------------------------------------------------------------
# Step 3: Populate Snowflake Credentials
# ------------------------------------------------------------------------------
# databricks secrets put --scope snowflake --key sfURL
# (Enter: <your-account>.snowflakecomputing.com)

# databricks secrets put --scope snowflake --key sfUser
# (Enter: your Snowflake service account username)

# databricks secrets put --scope snowflake --key sfPassword
# (Enter: your Snowflake service account password)

# ------------------------------------------------------------------------------
# Step 4: Populate AWS Config
# ------------------------------------------------------------------------------
# databricks secrets put --scope aws --key s3_bucket_name
# (Enter: retailedge-analytics-prod)

# databricks secrets put --scope aws --key aws_region
# (Enter: us-east-1)

# ------------------------------------------------------------------------------
# Step 5: Verify
# ------------------------------------------------------------------------------
# databricks secrets list --scope snowflake
# databricks secrets list --scope aws

# ------------------------------------------------------------------------------
# Step 6: Usage Pattern in Notebooks
# ------------------------------------------------------------------------------
# Access secrets in any Databricks notebook:
#
#   password    = dbutils.secrets.get(scope="snowflake", key="sfPassword")
#   bucket_name = dbutils.secrets.get(scope="aws",       key="s3_bucket_name")
#
# Secret values are never printed or written to logs — output shows [REDACTED].
# Access is governed by Unity Catalog permission groups.

print("""
Secrets Architecture:
  AWS Secrets Manager
        ↓
  Databricks Secret Scope (reference layer)
        ↓
  dbutils.secrets.get(scope, key)  ← used in all notebooks
        ↓
  [REDACTED]  ← printed value is always masked

Scopes configured:
  snowflake → sfURL, sfUser, sfPassword
  aws       → s3_bucket_name, aws_region
""")
