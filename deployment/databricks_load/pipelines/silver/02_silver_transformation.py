# =============================================================================
# databricks_load/pipelines/silver/02_silver_transformation.py
#
# PURPOSE
#   Incrementally transforms Bronze staffing records into Silver Delta
#   (healthcare_catalog.silver.staffing_standardized). Applies type casting,
#   date normalisation, DQ flagging, deduplication, and MERGE upsert.
#
# DESIGN PATTERN: INCREMENTAL MERGE
#   Watermark on _ingested_at reads only new Bronze records since last run.
#   Deduplicates on staffing_id BEFORE MERGE to prevent
#   DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW errors.
#
# TASK POSITION IN ETL JOB: Task 3 of 5
#
# UTILITIES LOADED (each in its own cell)
#   %run /Workspace/Shared/healthcare/common/config/pipeline_config
#   %run /Workspace/Shared/healthcare/common/utils/dq_utils
#   %run /Workspace/Shared/healthcare/common/utils/batch_control_utils
#   %run /Workspace/Shared/healthcare/common/utils/dynamo_utils
#
# STEP-BY-STEP
#
#   1. get_watermark(spark, TABLES["silver_staffing"])
#      Returns "1970-01-01 00:00:00" on first run (process all Bronze).
#      Subsequent runs return max _ingested_at from Silver → incremental.
#
#   2. Filter Bronze: .filter(col("_ingested_at") > lit(wm_from).cast("timestamp"))
#      Exit early with NO_NEW_RECORDS if records_read == 0.
#
#   3. cast_silver_types(bronze_df) — from dq_utils:
#      a. parse_work_date() — normalise date from ANY hospital format to DATE
#      b. staffing_id — SHA256("facility_id|work_date|role_code") using parsed date
#         so the key is format-independent across all hospitals
#      c. Type casts: staff_count→INT, patient_census→INT, hours→DOUBLE
#      d. shift_type → UPPER + TRIM
#      e. nurse_patient_ratio = staff_count / patient_census (NULL if census=0)
#      f. overtime_pct = hours_overtime / hours_worked (NULL if worked=0)
#
#   4. build_dq_flags_column() — ARRAY<STRING> of rule violations per record
#      Records with empty arrays are clean; records with entries are DQ-flagged.
#      DQ-flagged records are still written to Silver (not quarantined again —
#      Lambda already quarantined truly bad records at ingestion time).
#
#   5. DEDUP before MERGE (CRITICAL STEP):
#      ROW_NUMBER OVER (PARTITION BY staffing_id ORDER BY _ingested_at DESC)
#      Keep only _rn == 1 (most recent record per key).
#      Materialise via createOrReplaceTempView → spark.sql() to break
#      Catalyst lineage (prevents Catalyst re-expanding the window during
#      MERGE planning — a known Spark/Delta issue).
#      Verify: groupBy staffing_id .count().filter("count > 1").count() == 0
#      Raise ValueError if any duplicates remain before proceeding to MERGE.
#
#   6. Write to Silver:
#      FIRST RUN (table not exists):
#        .write.format("delta").partitionBy("work_date").saveAsTable(...)
#      SUBSEQUENT RUNS:
#        DeltaTable.forName().merge(silver_dedup, "s.staffing_id = n.staffing_id")
#        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#
#   7. Metrics: complete_pipeline_run / write_job_bookmark / write_pipeline_log
#
# WHY WATERMARK NOT CDF?
#   CDF was not enabled on Bronze at creation time. Enabling CDF retroactively
#   only tracks changes forward from the enable date — historical records are
#   not in the change feed. Watermark on _ingested_at achieves the same
#   incremental result with no CDF dependency.
#
# =============================================================================
