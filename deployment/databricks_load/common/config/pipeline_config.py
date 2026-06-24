# =============================================================================
# databricks_load/common/config/pipeline_config.py
#
# PURPOSE
#   Single source of truth for all configuration values shared across every
#   Databricks notebook in the platform. Imported (via %run) at the top of
#   every pipeline notebook so that changing one value here propagates to all
#   pipelines without editing each notebook individually.
#
# HOW IT IS LOADED
#   Databricks notebooks cannot import .py files directly when stored as .ipynb
#   format. Each notebook loads this config using the Databricks %run magic:
#
#     %run /Shared/healthcare/common/config/pipeline_config
#
#   This executes the notebook in the caller's scope, making all variables
#   (BUCKET, CATALOG, PATHS, TABLES, etc.) available as local variables.
#   Each %run must be in its own notebook cell.
#
# WHAT IT CONTAINS
#
#   BUCKET     → S3 bucket name: "hc-data-lake-prod"
#   CATALOG    → Unity Catalog name: "healthcare_catalog"
#   GLUE_DB    → Glue Catalog database name: "healthcare-data-platform_bronze_prod"
#                Used by glue_utils.get_glue_schema_hints() to query Glue
#                information_schema.columns before Auto Loader reads files.
#
#   PATHS      → dict of S3 paths for each pipeline layer.
#                Keys: landing, bronze_src, bronze_delta, silver, gold,
#                      ml_ready, audit, quarantine
#                Used in notebooks as: PATHS["bronze_src"]
#
#   CHECKPOINTS → dict of S3 paths for Auto Loader and streaming checkpoints.
#                 Checkpoints track which files have been processed so that
#                 Auto Loader does not reprocess files on restart.
#                 Keys: bronze_autoloader, bronze_schema, silver_streaming,
#                       adt_events
#                 ⚠️  Deleting a checkpoint path causes full reprocessing from
#                 the beginning — only do this deliberately during testing.
#
#   TABLES     → dict of fully qualified Unity Catalog table names.
#                Format: "<catalog>.<schema>.<table>"
#                Keys: bronze_staffing, bronze_adt, silver_staffing,
#                      gold_fact_staffing, gold_fact_overtime, gold_dim_facility,
#                      ml_features, bc_file_registry, bc_pipeline_runs,
#                      bc_scd2_audit, bc_file_schedule
#                Used in notebooks as: TABLES["silver_staffing"]
#                This avoids hardcoding table names in multiple places.
#
#   DQ_RULES   → dict of data quality thresholds used by dq_utils.py and Lambda.
#                Keys:
#                  max_patient_census     (1500)  — census values above this are invalid
#                  max_overtime_hours     (24)    — OT hours above this trigger quarantine
#                  required_fields        list    — fields that cannot be null or empty
#                  understaffed_threshold (0.25)  — nurse/patient ratio below this = understaffed
#                  overtime_blended_rate  (65.0)  — $/hr used to estimate overtime cost in Gold
#
#   KINESIS    → dict with stream name and region.
#                Used by notebooks that read from Kinesis (bronze ADT streaming).
#
#   AWS        → dict with account ID and quarantine SQS URL.
#                Used when Lambda or notebooks need to publish to the FIFO queue.
#
# ENVIRONMENT HANDLING
#   This file does not have separate dev/prod branches — environment
#   differences are handled at the Terraform level (separate tfvars).
#   All notebooks run against the same Databricks workspace; environment
#   isolation is achieved via Unity Catalog permissions and separate clusters.
#
# =============================================================================
