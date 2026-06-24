##############################################################################
# modules/glue/main.tf
#
# PURPOSE
#   Creates the AWS Glue Crawler and Glue Catalog database for schema
#   discovery on the Bronze S3 layer. Glue's role in this platform is
#   SCHEMA CATALOG ONLY — it does NOT run ETL jobs or write to Delta tables.
#   All ETL is handled exclusively by Databricks notebooks.
#
# WHAT GLUE DOES IN THIS PIPELINE
#   1. Scans s3://hc-data-lake-prod/bronze/sftp/ on schedule
#   2. Infers column names and types from CSV files
#   3. Registers the schema in the Glue Catalog database
#   4. Databricks Auto Loader reads those schemas via glue_utils.py:
#        get_glue_schema_hints(spark, "sftp", GLUE_DB)
#        → queries Glue information_schema.columns
#        → returns "facility_id STRING, work_date STRING, ..." as schemaHints
#   5. If new columns are detected → fires SNS ops_alert (schema drift)
#
# WHAT GLUE DOES NOT DO
#   - Run PySpark ETL jobs (Databricks handles all transformations)
#   - Write to Delta tables (Databricks Auto Loader does this)
#   - Replace Databricks notebooks (Glue is metadata only)
#
# SCHEDULE
#   cron(0/30 5-23 * * ? *) = every 30 minutes during shift hours (5AM–11PM UTC)
#   Hospitals do not send files at 3 AM — running outside shift hours wastes
#   money without adding schema discovery value.
#   Set via var.crawler_schedule (from terraform.tfvars).
#
# DOCS
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_crawler
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_catalog_database
#   https://docs.aws.amazon.com/glue/latest/dg/crawler-configuration.html
##############################################################################

##############################################################################
# aws_glue_catalog_database — metadata store for Bronze schemas
#
# Name: healthcare-data-platform_bronze_prod
# This database name is referenced in two places:
#   1. modules/glue (here) — where the crawler writes schema metadata
#   2. databricks_load/common/utils/glue_utils.py — where Databricks reads it
#
# Note the database name uses underscores as separators (not hyphens)
# except for the project_name prefix which uses hyphens.
# Glue database names must be lowercase, 1–255 characters, alphanumeric + underscore.
##############################################################################
resource "aws_glue_catalog_database" "bronze" {
  name        = "${var.project_name}_bronze_${var.environment}"
  description = "Healthcare Bronze layer schema catalog — populated by Glue Crawler, read by Databricks Auto Loader schemaHints"
}

##############################################################################
# aws_glue_crawler — bronze-crawler-prod
#
# role          → IAM role ARN with permissions to:
#                   s3:GetObject, s3:ListBucket on hc-data-lake-prod/bronze/sftp/
#                   glue:CreateTable, glue:UpdateTable on the catalog database
#                   kms:Decrypt on the platform KMS key (S3 objects are encrypted)
#                 Created in modules/iam as the Glue role.
#
# database_name → the Glue Catalog database created above
#
# s3_target     → the S3 prefix to crawl. The crawler discovers all CSV files
#                 under bronze/sftp/ and infers the schema from a sample.
#                 exclusions: checkpoints/ and delta_log/ prefixes should be
#                 excluded to prevent the crawler from attempting to parse
#                 Delta Lake transaction log JSON as schema.
#
# schedule      → EventBridge cron expression from var.crawler_schedule
#                 Set to every 30 min during shift hours.
#
# schema_change_policy
#   UPDATE_IN_DATABASE: when new columns appear, update the table definition
#   LOG: when the S3 prefix disappears (e.g. after cleanup), only log it —
#        do NOT delete the catalog table. Prevents accidental schema loss.
#
# configuration (JSON string)
#   CrawlerOutput.Tables.AddOrUpdateBehavior = "MergeNewColumns"
#   Merges newly discovered columns into the existing table definition
#   instead of creating a new table version. Required for schema evolution.
##############################################################################
resource "aws_glue_crawler" "bronze" {
  name          = "${var.project_name}-bronze-crawler-${var.environment}"
  # role          = var.glue_role_arn   ← from modules/iam output
  database_name = aws_glue_catalog_database.bronze.name
  # schedule      = var.crawler_schedule  ← "cron(0/30 5-23 * * ? *)"

  s3_target {
    path = "s3://${var.bucket_name}/bronze/sftp/"
    # exclusions = ["**.json", "**/_delta_log/**", "**/checkpoints/**"]
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  # configuration = jsonencode({
  #   Version = 1.0
  #   CrawlerOutput = {
  #     Tables = { AddOrUpdateBehavior = "MergeNewColumns" }
  #   }
  # })

  # tags = { Environment = var.environment, Project = var.project_name }
}
