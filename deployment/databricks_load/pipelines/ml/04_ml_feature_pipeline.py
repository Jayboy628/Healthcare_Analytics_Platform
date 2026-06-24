# =============================================================================
# databricks_load/pipelines/ml/04_ml_feature_pipeline.py
#
# PURPOSE
#   Engineers features for the overtime prediction ML model from Gold fact tables.
#   Writes to healthcare_catalog.ml_ready.overtime_features as a full overwrite
#   on every run (not incremental — rolling windows require the full history).
#
# DESIGN PATTERN: PROCEDURAL (window functions)
#   PySpark window functions compute rolling aggregates that require looking
#   back N days for each row. This cannot be expressed as a simple MERGE —
#   the full Gold table must be read and all features recomputed on each run.
#
# TASK POSITION IN ETL JOB: Task 5 of 5
#
# UTILITIES LOADED
#   %run /Workspace/Shared/healthcare/common/config/pipeline_config
#   %run /Workspace/Shared/healthcare/common/utils/batch_control_utils
#   %run /Workspace/Shared/healthcare/common/utils/dynamo_utils
#
# FEATURES PRODUCED (one row per date_key + facility_id + role_code)
#
#   Source columns (from gold.fact_staffing):
#     date_key, facility_id, role_code, staff_count, patient_census,
#     hours_overtime, nurse_patient_ratio, overtime_pct, is_understaffed
#
#   Rolling window features (7-day lookback, partitioned by facility + role):
#     avg_census_7d     → avg(patient_census) over 7 days
#     avg_staff_7d      → avg(staff_count) over 7 days
#     avg_ot_pct_7d     → avg(overtime_pct) over 7 days
#     total_ot_hours_7d → sum(hours_overtime) over 7 days
#     census_vs_avg     → patient_census - avg_census_7d (deviation from norm)
#
#   Temporal features:
#     is_weekend        → dayofweek() in (1=Sunday, 7=Saturday)
#
#   Target variable (supervised learning label):
#     will_overtime_next_day → lead(overtime_pct, 1) > 0
#       True if the NEXT day has overtime for this facility+role combination.
#       This is what the ML model learns to predict.
#
#   is_understaffed → cast to INT (1/0) for ML model compatibility
#
# WRITE STRATEGY
#   .write.format("delta").mode("overwrite").option("overwriteSchema","true")
#   .option("path", ML_PATH).partitionBy("date_key").saveAsTable(ML_TABLE)
#
#   WHY OVERWRITE (not MERGE)?
#     Rolling window features for a row on date D depend on records from
#     days D-6 through D. If any of those historical records change (e.g.
#     hospital corrects a past submission), ALL downstream feature rows must
#     be recomputed. Incremental MERGE cannot handle this dependency cleanly.
#     Full overwrite is simpler and correct. The table is small relative to
#     compute cost (days * facilities * roles rows).
#
#   IMPORTANT: The path s3://hc-data-lake-prod/ml-ready/overtime_features
#   contains a Delta log from a previous run. Any write to this path MUST use
#   format("delta") — Parquet or CSV writes will fail with "Delta table already
#   exists at this location" error.
#
# EARLY EXIT
#   If gold.fact_staffing is empty (ETL job ran but Gold has no data),
#   exit with dbutils.notebook.exit("NO_GOLD_DATA") instead of crashing.
#   This prevents the ML notebook from writing an empty feature table.
#
# =============================================================================
