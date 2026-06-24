# =============================================================================
# databricks_load/common/utils/dynamo_utils.py
#
# PURPOSE
#   Shared boto3 DynamoDB utility functions for writing operational metadata
#   from Databricks notebooks to the six DynamoDB control-plane tables.
#   This closes the observability loop: Lambda writes ledger/DQ/quarantine
#   entries on file arrival; notebooks write job bookmarks and pipeline logs
#   on successful completion.
#
# HOW IT IS LOADED
#   %run /Shared/healthcare/common/utils/dynamo_utils
#   (in its own cell, after pipeline_config is loaded)
#
# AWS CREDENTIALS
#   DynamoDB access from Databricks Serverless requires explicit AWS credentials
#   because serverless notebooks do not run on EC2 instances with instance
#   profiles. Credentials are stored in a Databricks secret scope named "aws":
#
#     databricks secrets create-scope aws
#     databricks secrets put-secret aws access_key   --string-value <IAM_KEY>
#     databricks secrets put-secret aws secret_key   --string-value <IAM_SECRET>
#     databricks secrets put-secret aws session_token --string-value <TOKEN>  # if using temp creds
#
#   _get_dynamo_resource() calls dbutils.secrets.get() to retrieve these at
#   runtime. The IAM user/role must have dynamodb:PutItem + dynamodb:GetItem
#   on all six operational tables.
#
# FUNCTIONS
#
#   write_job_bookmark(job_name, source_name, record_count, batch_id, run_status)
#     → Writes a row to job_bookmark_prod after a successful notebook run.
#     → Keyed on job_name (HASH) + source_name (RANGE).
#       source_name = the S3 path or Delta table name that was read.
#     → batch_id = Databricks run ID from spark.conf.get("spark.databricks.job.runId")
#       Auto-detected if not provided.
#     → Non-fatal: if DynamoDB is unreachable, logs a warning but does NOT
#       raise an exception. The pipeline should not fail because of metadata.
#     → Called at the end of every Bronze, Silver, and Gold notebook.
#
#   write_pipeline_log(pipeline_name, records_processed, records_quarantined,
#                      duration_seconds, status, error_message)
#     → Writes a job execution event to pipeline_log_prod.
#     → One row per notebook run: pipeline name, status, record counts, duration.
#     → Generates a UUID pipeline_id as the HASH key.
#     → status: "SUCCESS" or "FAILED"
#     → error_message truncated to 500 chars (DynamoDB item size limit is 400KB).
#     → Called at the end of every notebook alongside write_job_bookmark().
#
#   write_dq_results(dataset_name, total_records, valid_records, quarantined,
#                    error_summary, source_file)
#     → Writes a DQ summary row to data_quality_results_prod.
#     → Computes pass_rate = valid / total * 100.
#     → Sets quality_status = "PASS" (0 quarantined), "WARN" (pass_rate >= 90%),
#       or "FAIL" (pass_rate < 90%).
#     → error_summary is a dict: {"NULL_FACILITY_ID": 2, "CENSUS_OUT_OF_RANGE": 1}
#     → Currently only Lambda writes DQ results; this function allows notebooks
#       to also write post-transformation DQ summaries in the future.
#
#   write_schema_registry(dataset_name, fieldnames)
#     → Registers the column schema for a dataset using a conditional PutItem.
#     → schema_version = "v_<first 8 chars of MD5 of sorted column list>"
#     → Conditional write: if schema_version already exists, does nothing.
#       This is safe to call on every run — it only creates a new row when
#       the column set genuinely changes (schema drift).
#     → After writing, the dynamo_ops_queries.py --query schema_drift command
#       shows datasets with multiple registered versions.
#
# =============================================================================
