##############################################################################
# modules/databricks/main.tf
#
# PURPOSE
#   Provisions the Databricks Unity Catalog hierarchy and optional SQL
#   Warehouse using the Databricks Terraform provider. This module runs
#   AGAINST the existing Databricks workspace (created manually or via
#   a separate workspace provisioning step) — it does not create the workspace.
#
# RESOURCES CREATED
#   databricks_storage_credential  IAM role-backed credential that allows
#                                  Databricks to access S3 using the Unity
#                                  Catalog IAM role created in modules/iam.
#   databricks_external_location   One per S3 prefix (bronze, silver, gold,
#                                  ml-ready, checkpoints). Scopes which S3
#                                  paths the catalog can access.
#   databricks_catalog             The "healthcare_catalog" Unity Catalog
#                                  root — all tables live under this.
#   databricks_schema              Five schemas: bronze, silver, gold,
#                                  ml_ready, batch_control.
#   databricks_sql_endpoint        Optional SQL Warehouse for Streamlit
#                                  dashboard queries and ad-hoc SQL.
#
# TWO-PASS DEPLOYMENT (required for Unity Catalog)
#   Unity Catalog external locations require an IAM role that trusts the
#   Databricks account number. That role's ARN is not known until Terraform
#   creates it (Pass 1). The role ARN must then be given to Databricks as the
#   storage credential, and the Databricks Unity Catalog system creates a new
#   IAM role ARN (unity_catalog_iam_arn) that must be added back to the IAM
#   trust policy (Pass 2). This chicken-and-egg situation is the reason the
#   deployment runbook has a two-pass sequence.
#
#   After Pass 1: run `terraform output storage_credential_iam_arn`
#                 copy the ARN into terraform.tfvars as databricks_principal_arn
#   After Pass 2: Unity Catalog external locations are fully operational.
#
# DOCS
#   https://registry.terraform.io/providers/databricks/databricks/latest/docs
#   https://docs.databricks.com/aws/en/data-governance/unity-catalog/get-started.html
#   https://docs.databricks.com/aws/en/connect/storage/tutorial-s3-instance-profile.html
##############################################################################

##############################################################################
# databricks_storage_credential — IAM role-backed S3 credential
#
# Registers the Unity Catalog IAM role with Databricks so the catalog can
# read/write S3 objects. The role is created in modules/iam and its ARN is
# passed here as var.databricks_s3_role_arn.
#
# After this resource is created, Databricks returns a NEW IAM role ARN
# (unity_catalog_iam_arn in outputs.tf). This ARN must be added to the
# original IAM role's trust policy — that is what Pass 2 does.
##############################################################################
resource "databricks_storage_credential" "s3" {
  name = "${var.project_name}_s3_credential_${var.environment}"

  aws_iam_role {
    # role_arn = var.databricks_s3_role_arn  ← from modules/iam output
  }

  comment = "AWS IAM role-backed storage credential for healthcare lakehouse"
}

##############################################################################
# databricks_external_location — one per S3 zone
#
# External locations define which S3 paths the Unity Catalog is allowed to
# access. Databricks enforces this at the storage layer — a notebook running
# as a non-admin user cannot read S3 paths outside registered external locations.
#
# for_each = var.external_locations
#   A map of { zone_name → s3_path } defined in variables.tf.
#   Example:
#     bronze    = "s3://hc-data-lake-prod/bronze/"
#     silver    = "s3://hc-data-lake-prod/silver/"
#     gold      = "s3://hc-data-lake-prod/gold/"
#     ml_ready  = "s3://hc-data-lake-prod/ml-ready/"
#     checkpoints = "s3://hc-data-lake-prod/checkpoints/"
#
# force_destroy = true
#   Allows Terraform to delete external locations even if managed tables
#   exist under them. Required during redeploy/teardown sequences.
#   In steady-state prod, this is safe — tables are Delta Lake files on S3
#   and are not deleted by removing the external location registration.
##############################################################################
resource "databricks_external_location" "locations" {
  for_each = var.external_locations

  name            = "${var.project_name}_${each.key}_${var.environment}"
  url             = each.value
  credential_name = databricks_storage_credential.s3.name
  force_destroy   = true
  comment         = "External location for ${each.key} zone"

  depends_on = [databricks_storage_credential.s3]
}

##############################################################################
# databricks_catalog — healthcare_catalog
#
# The root of the Unity Catalog three-level namespace:
#   <catalog>.<schema>.<table>
#   e.g. healthcare_catalog.bronze.stg_staffing
#
# storage_root → the S3 path where managed table data is stored by default.
#   Managed tables created without an explicit LOCATION clause land here.
#   External tables (created via CREATE TABLE ... LOCATION 's3://...') are
#   stored at their specified location instead.
#
# depends_on → external locations must exist before the catalog is created
#   because catalog creation validates that storage_root is accessible via
#   a registered external location.
##############################################################################
resource "databricks_catalog" "this" {
  name         = var.catalog_name  # "healthcare_catalog"
  comment      = "Healthcare staffing analytics — Medallion architecture"
  # storage_root = var.catalog_storage_root  ← s3://hc-data-lake-prod/unity-catalog/

  depends_on = [databricks_external_location.locations]
}

##############################################################################
# databricks_schema — bronze, silver, gold, ml_ready, batch_control
#
# for_each creates one schema per entry in the set.
# Schemas group related tables and apply shared permissions.
# Each schema maps to a Medallion layer except batch_control which holds
# pipeline operational tables (file_registry, pipeline_runs, file_schedule).
##############################################################################
resource "databricks_schema" "schemas" {
  for_each = toset([
    "bronze",
    "silver",
    "gold",
    "ml_ready",
    "batch_control",
  ])

  catalog_name = databricks_catalog.this.name
  name         = each.key
  comment      = "Healthcare ${each.key} layer"

  depends_on = [databricks_catalog.this]
}

##############################################################################
# databricks_sql_endpoint — SQL Warehouse for dashboard + ad-hoc queries
#
# count = var.create_sql_warehouse ? 1 : 0
#   Conditional creation — set to false in dev to save cost.
#   The warehouse is only needed when Streamlit dashboard is running.
#
# cluster_size = "Small" (2 DBUs/hr)
#   Sufficient for Streamlit dashboard queries against pre-aggregated Gold
#   tables. If query latency exceeds 10 seconds consistently, upgrade to Medium.
#
# max_num_clusters = 3 in prod, 1 in dev
#   Allows the warehouse to scale out to 3 clusters under concurrent load
#   (e.g. multiple executives running the dashboard simultaneously in prod).
#
# auto_stop_mins = 10
#   Warehouse stops after 10 minutes of inactivity.
#   Prevents idle billing between pipeline runs and dashboard sessions.
##############################################################################
resource "databricks_sql_endpoint" "warehouse" {
  count = var.create_sql_warehouse ? 1 : 0

  name             = var.sql_warehouse_name  # "healthcare-sql-warehouse-prod"
  cluster_size     = var.sql_warehouse_size  # "Small"
  max_num_clusters = var.environment == "prod" ? 3 : 1
  auto_stop_mins   = 10

  depends_on = [databricks_schema.schemas]
}
