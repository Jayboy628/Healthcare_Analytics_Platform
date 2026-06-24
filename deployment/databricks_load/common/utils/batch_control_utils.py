# =============================================================================
# databricks_load/common/utils/batch_control_utils.py
#
# PURPOSE
#   Shared utility functions for pipeline run tracking and file registry
#   management. Written to the batch_control Delta Lake tables in Unity Catalog.
#   Every Databricks notebook calls these at start and end of each run to
#   create an auditable record of what was processed and when.
#
# HOW IT IS LOADED
#   %run /Shared/healthcare/common/utils/batch_control_utils
#   (in its own cell, after pipeline_config is loaded)
#
# FUNCTIONS
#
#   start_pipeline_run(spark, pipeline_name, layer, triggered_by, runs_table)
#     → Inserts a RUNNING row into batch_control.pipeline_runs.
#     → Returns run_id (UUID string) — pass this to complete_pipeline_run().
#     → Calls _ensure_runs_table() first to handle cold-start (table not yet
#       created). This prevents the notebook from crashing on first run before
#       the table exists.
#     → triggered_by: "SCHEDULE" (cron), "SNS" (batch_complete trigger),
#       or "MANUAL" (operator triggered databricks jobs run-now).
#
#   complete_pipeline_run(spark, run_id, records_read, records_written,
#                         records_failed, watermark_to, error_message, runs_table)
#     → Updates the RUNNING row to SUCCESS or FAILED.
#     → Calculates duration_seconds from the started_at timestamp.
#     → Sets watermark_to: the timestamp of the last record processed.
#       The next run reads this via dq_utils.get_watermark() to determine
#       where to start the incremental read from Bronze.
#     → If error_message is provided, status = "FAILED"; otherwise "SUCCESS".
#     → Always call this in the except block with error_message=str(e) so
#       failures are recorded, then re-raise the exception.
#
#   register_file(spark, file_checksum, source_path, bronze_path, hospital_id,
#                 file_date, record_count, quarantine_count, bronze_run_id)
#     → Inserts a row into batch_control.file_registry using MERGE ON file_checksum.
#     → MERGE (not INSERT) prevents duplicates if the same file is processed twice.
#     → Sets bronze_status = "INGESTED", silver_status = "PENDING",
#       gold_status = "PENDING" on first insert.
#     → Returns file_id (UUID) for use in downstream status updates.
#
#   update_file_layer_status(spark, file_checksum, layer, status, run_id)
#     → Updates the bronze/silver/gold status and timestamp for a file.
#     → Called by Silver notebook when it merges a file's records to Silver,
#       and by Gold notebook when it processes Silver records to Gold.
#     → layer: "bronze", "silver", or "gold"
#     → status: "INGESTED", "TRANSFORMED", "AGGREGATED", "FAILED"
#
# USAGE PATTERN (in every pipeline notebook)
#   run_id = start_pipeline_run(spark, "Bronze_Ingestion", "BRONZE")
#   try:
#       ... pipeline logic ...
#       complete_pipeline_run(spark, run_id, records_read, records_written, 0)
#   except Exception as e:
#       complete_pipeline_run(spark, run_id, 0, 0, 0, error_message=str(e))
#       raise
#
# =============================================================================
