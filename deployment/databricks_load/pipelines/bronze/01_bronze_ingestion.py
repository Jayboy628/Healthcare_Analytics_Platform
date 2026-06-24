# =============================================================================
# databricks_load/pipelines/bronze/01_bronze_ingestion.py
#
# PURPOSE
#   Ingests validated CSV files from S3 bronze/sftp/ into the Bronze Delta
#   table (healthcare_catalog.bronze.stg_staffing) using Databricks Auto Loader.
#
# DESIGN PATTERN: DECLARATIVE
#   Auto Loader manages file tracking, schema evolution, deduplication,
#   and checkpointing automatically. The notebook declares WHAT to ingest,
#   not HOW to track files. This is why there is no explicit file registry
#   write in this notebook — Auto Loader handles it internally via the
#   checkpoint location.
#
# TASK POSITION IN ETL JOB
#   Task 2 of 5. Depends on Run_Glue_Crawler completing successfully.
#   Must complete before Silver_Transformation_Workflow starts.
#
# UTILITIES LOADED (each %run in its own cell)
#   %run /Workspace/Shared/healthcare/common/config/pipeline_config
#   %run /Workspace/Shared/healthcare/common/utils/glue_utils
#   %run /Workspace/Shared/healthcare/common/utils/batch_control_utils
#   %run /Workspace/Shared/healthcare/common/utils/dynamo_utils
#
# WHAT THE NOTEBOOK DOES
#
#   Step 1 — Start pipeline run tracking
#     start_pipeline_run(spark, "Bronze_Ingestion", "BRONZE")
#     Returns run_id for use in complete_pipeline_run() at the end.
#
#   Step 2 — Get Glue schema hints
#     get_glue_schema_hints(spark, "sftp", GLUE_DB)
#     Returns "facility_id STRING, work_date STRING, ..." string.
#     Used as cloudFiles.schemaHints to override Auto Loader type inference.
#
#   Step 3 — Configure Auto Loader readStream
#     cloudFiles.format = "csv"
#     cloudFiles.schemaLocation → CHECKPOINTS["bronze_schema"]
#       Auto Loader writes the inferred schema here after the first run.
#       On subsequent runs it reads the schema from here instead of re-inferring.
#     cloudFiles.schemaHints → from Glue (Step 2)
#     cloudFiles.inferColumnTypes = "false"
#       All columns ingested as STRING — type casting is Silver's job.
#     cloudFiles.schemaEvolutionMode = "addNewColumns"
#       When a hospital adds a new column, Auto Loader adds it to the Bronze
#       table automatically instead of failing. New columns appear as NULL
#       in records from hospitals that don't send that column.
#     header = "true" → CSV files include a header row
#     Source path → PATHS["bronze_src"] = s3://hc-data-lake-prod/bronze/sftp/
#
#   Step 4 — Add metadata columns
#     _source_file  → F.col("_metadata.file_path")  (Unity Catalog safe way)
#       Tracks which S3 file each row came from — essential for lineage.
#     _ingested_at  → F.current_timestamp()
#       Used by Silver watermark filter to find new Bronze records.
#     _record_index → F.expr("uuid()")
#       Unique ID per row — safe in streaming context (unlike monotonically_increasing_id).
#
#   Step 5 — Write Delta stream
#     format("delta")
#     outputMode("append") — Bronze is append-only, never updated
#     checkpointLocation → CHECKPOINTS["bronze_autoloader"]
#       Auto Loader stores file tracking metadata here. Tracks which S3 files
#       have already been processed. NEVER delete this unless you intend to
#       reprocess all Bronze files from scratch.
#     mergeSchema = "true"
#       Allows new columns to be added to the Delta table when hospitals
#       expand their CSV schema (combined with schemaEvolutionMode above).
#     path → PATHS["bronze_delta"] = s3://hc-data-lake-prod/bronze/delta/stg_staffing
#     trigger(availableNow=True)
#       Processes all new files then stops. Does NOT run continuously.
#       "availableNow" is equivalent to a one-time batch over new files —
#       correct for a scheduled job (vs. a continuously running stream).
#
#   Step 6 — Register table in Unity Catalog
#     CREATE TABLE IF NOT EXISTS healthcare_catalog.bronze.stg_staffing
#     USING DELTA LOCATION 's3://hc-data-lake-prod/bronze/delta/stg_staffing'
#     If the table already exists, this is a no-op.
#
#   Step 7 — Record metrics
#     records_written = spark.table(TABLES["bronze_staffing"]).count()
#     complete_pipeline_run(spark, run_id, records_written, records_written, 0)
#     write_job_bookmark("bronze_sftp_ingestion", PATHS["bronze_src"], records_written)
#     write_pipeline_log("Bronze_Ingestion", records_written)
#
# ERROR HANDLING
#   All steps are wrapped in try/except.
#   On exception: complete_pipeline_run(..., error_message=str(e)), then re-raise.
#   This ensures the pipeline_runs table always has a terminal status row.
#
# =============================================================================
