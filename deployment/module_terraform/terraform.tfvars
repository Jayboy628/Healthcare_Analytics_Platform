##############################################################################
# module_terraform/terraform.tfvars
#
# PURPOSE
#   Environment-specific values for the production deployment.
#   Safe to commit — contains no secrets. Secrets (Databricks host/token,
#   principal ARNs) are injected at runtime as TF_VAR_* environment variables
#   via: source scripts/load_databricks_secrets.sh
#
# ⚠️  Do NOT add tokens, passwords, or secret keys here.
##############################################################################

# ── Project identity ──────────────────────────────────────────────────────────
# Prefix applied to every AWS resource name for uniqueness and filterability.
# Pattern: <project_name>-<resource-type>-<environment>
# Example: healthcare-data-platform-file_arrival-prod
project_name = "healthcare-data-platform"
environment  = "prod"
aws_region   = "us-east-1"

# ── S3 data lake ──────────────────────────────────────────────────────────────
# Central bucket for all pipeline layers: landing → bronze → silver → gold.
# KMS encryption is enforced — see modules/kms for the key configuration.
# Must be globally unique. Changing after deploy requires Terraform destroy.
data_lake_bucket_name = "hc-data-lake-prod"

# ── Networking ────────────────────────────────────────────────────────────────
# VPC is required for ElastiCache (Redis) — it cannot exist outside a VPC.
# Lambda and SQS/Kinesis use public endpoints and do not need VPC placement,
# but placing Lambda in the VPC allows private routing to ElastiCache.
# Three AZs ensures Redis replication can survive an AZ failure.
vpc_cidr             = "10.0.0.0/16"
private_subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
public_subnet_cidrs  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
availability_zones   = ["us-east-1a", "us-east-1b", "us-east-1c"]

# ── Databricks Unity Catalog — two-pass deployment ────────────────────────────
#
# WHY TWO PASSES?
#   Unity Catalog external locations require an IAM role that trusts the
#   Databricks account. That role does not exist until Terraform creates it
#   in Pass 1. The role ARN (unity_catalog_iam_arn) is only known AFTER
#   Pass 1 completes, so KMS and IAM cannot reference it until Pass 2.
#
# PASS 1 (first terraform apply):
#   databricks_s3_role_exists = false
#   databricks_principal_arn  = ""
#   → Terraform creates the IAM role for Databricks S3 access.
#   → After apply, run: terraform output storage_credential_iam_arn
#   → Copy the ARN into databricks_principal_arn below.
#
# PASS 2 (second terraform apply):
#   databricks_s3_role_exists = true
#   databricks_principal_arn  = "<ARN from Pass 1 output>"
#   → Terraform updates KMS key policy to include the Databricks principal.
#   → External locations are created and Unity Catalog is fully operational.
#
# See TERRAFORM_DEPLOY_RUNBOOK.md — Phase 5 for the complete sequence.
databricks_s3_role_exists = false  # flip to true after Pass 1
databricks_principal_arn  = ""     # paste ARN from: terraform output storage_credential_iam_arn
databricks_external_id    = ""     # from Databricks workspace → Account Settings → External ID

# ── Databricks resources ──────────────────────────────────────────────────────
# Unity Catalog: all tables referenced as healthcare_catalog.<schema>.<table>
databricks_catalog_name = "healthcare_catalog"

# SQL Warehouse: serverless compute for Streamlit dashboard + ad-hoc SQL.
# Small = 2 DBUs/hr. Auto-stops after 10 minutes of inactivity.
# Scale up to Medium if dashboard queries exceed 10-second response time.
databricks_sql_warehouse_name = "healthcare-sql-warehouse-prod"
databricks_sql_warehouse_size = "Small"

# Databricks Job IDs — created once in the Databricks UI, then referenced
# here so the Lambda trigger knows which job to start via the Jobs API.
# Get IDs via: databricks jobs list
databricks_job_id_etl       = ""  # Healthcare ETL Pipeline (Bronze→Silver→Gold→ML)
databricks_job_id_streaming = ""  # Healthcare RT Streaming (Kinesis ADT→Bronze→Silver)

# ── Glue Crawler ──────────────────────────────────────────────────────────────
# Scans s3://hc-data-lake-prod/bronze/sftp/ and registers schemas in Glue Catalog.
# Databricks Auto Loader reads those schemas via glue_utils.get_glue_schema_hints().
# Schedule: every 30 min during active shift hours (5 AM – 11 PM UTC).
# Hospitals do not send files at 3 AM — running the crawler outside shift
# hours wastes money without adding value.
glue_crawler_schedule_expression = "cron(0/30 5-23 * * ? *)"

# ── Kinesis ───────────────────────────────────────────────────────────────────
# Real-time stream for ADT events: ADMIT / DISCHARGE / TRANSFER / callouts.
#
# shard_count = 4 → ~4,000 records/sec sustained write throughput.
#   Each shard = 1,000 records/sec OR 1 MB/sec (whichever is lower).
#   Scale up if IncomingBytes CloudWatch metric consistently > 70% capacity.
#
# retention_hours = 168 (7 days):
#   Allows full reprocessing after Databricks streaming job failure.
#   Also covers Monday morning bursts after a weekend gap.
#
# stream_mode = PROVISIONED:
#   Predictable cost for a known, steady event rate.
#   Switch to ON_DEMAND if volume is highly spiky.
kinesis_shard_count     = 4
kinesis_retention_hours = 168
kinesis_stream_mode     = "PROVISIONED"

# ── ElastiCache Redis ─────────────────────────────────────────────────────────
# Serves pre-computed KPIs to Streamlit dashboard at sub-second latency.
# Databricks Gold remains the system of record; Redis is a read-through cache.
# cache.r7g.large = 12.3 GB RAM — sufficient for KPI summaries across
# hundreds of facilities. Scale to xlarge if memory utilization > 70%.
redis_node_type = "cache.r7g.large"

# ── SNS alert recipients ──────────────────────────────────────────────────────
# Subscribed to the ops_alerts SNS topic. Each address receives an AWS
# subscription confirmation email on first apply — must click to confirm.
# Alerts include: Lambda errors, DQ pass rate < 90%, Databricks job failures,
# schema drift from Glue Crawler, file_arrival DLQ messages > 0.
alert_email_endpoints = [
  "primary-oncall@your-domain.com",
  "secondary-oncall@your-domain.com",
]
