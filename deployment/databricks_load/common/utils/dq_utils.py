# =============================================================================
# databricks_load/common/utils/dq_utils.py
#
# PURPOSE
#   Reusable PySpark data quality functions shared by the Silver transformation
#   notebook. Centralises DQ logic so rule changes propagate to all pipelines
#   by editing one file.
#
# HOW IT IS LOADED
#   %run /Shared/healthcare/common/utils/dq_utils
#   (in its own cell)
#
#   Note: in 02_silver_transformation.py, dq_utils is loaded with importlib.reload()
#   to force Databricks to pick up the latest version during interactive
#   development (Databricks caches module imports across cells).
#
# FUNCTIONS
#
#   parse_work_date(col_name="work_date")
#     → Returns a PySpark Column expression that parses work_date from ANY
#       of the date formats used by different hospitals:
#         MM/dd/yyyy    (US format — most common from hospital systems)
#         yyyy-MM-dd    (ISO 8601 — some modern hospital EHRs)
#         dd/MM/yyyy    (EU format — used by some international systems)
#         yyyyMMdd      (compact — used by legacy SFTP feeds)
#         MM-dd-yyyy    (US with dashes)
#         yyyy-MM-dd HH:mm:ss (timestamp — when column stored as TIMESTAMP)
#
#     Uses PySpark try_to_date() which returns NULL on parse failure instead
#     of throwing an exception. Coalesce picks the first non-null match.
#     Casts to STRING first to handle cases where the column is already a
#     TIMESTAMP in Bronze (Auto Loader sometimes infers TIMESTAMP for date-like
#     columns — casting to string then re-parsing normalises this).
#
#     WHY THIS MATTERS: hospital A sends "01/21/2026", hospital B sends
#     "2026-01-21", hospital C sends "20260121". All must normalise to
#     DATE type "2026-01-21" before the SHA256 staffing_id is computed.
#     If formats are mixed, the same shift (facility+date+role) gets different
#     staffing_id hashes → MERGE creates duplicates instead of upserts.
#
#   build_dq_flags_column(required_fields, max_census, max_ot)
#     → Returns a PySpark Column expression producing ARRAY<STRING> of DQ
#       violation codes for each row. Rows with an empty array are clean;
#       rows with entries are flagged (quarantined by Lambda, DQ-flagged in Silver).
#
#     Rule checks applied:
#       NULL_<FIELDNAME>      required field is null or empty string
#       CENSUS_OUT_OF_RANGE   patient_census > max_census (1500)
#       NEGATIVE_CENSUS       patient_census < 0
#       OT_HOURS_EXCEED_MAX   hours_worked_overtime > max_ot (24)
#       NEGATIVE_STAFF_COUNT  staff_count < 0
#
#     Uses F.array_compact() to remove null entries from the array so the
#     result is a clean list of only the rules that fired.
#
#   cast_silver_types(df)
#     → Applies all type casts and derived column computations to a Bronze
#       DataFrame, returning a Silver-ready DataFrame.
#
#     Step-by-step transformations:
#       1. parse_work_date()    → work_date_parsed (DATE)
#       2. staffing_id          → SHA256 of "facility_id|work_date|role_code"
#                                 Uses parsed date so the key is format-independent
#       3. work_date            → replace raw column with parsed version, drop temp
#       4. staff_count          → cast to INT
#       5. patient_census       → cast to INT
#       6. hours_worked         → cast to DOUBLE
#       7. hours_overtime       → renamed from hours_worked_overtime, cast to DOUBLE
#       8. shift_type           → UPPER + TRIM for normalisation
#       9. nurse_patient_ratio  → staff_count / patient_census (DOUBLE, 3 decimal places)
#                                 NULL when patient_census = 0 (avoid division by zero)
#      10. overtime_pct         → hours_overtime / hours_worked (DOUBLE, 3 decimal places)
#                                 NULL when hours_worked = 0
#
#   get_watermark(spark, table_name, ts_column="_ingested_at")
#     → Queries the Silver table for the maximum value of ts_column.
#     → Returns "1970-01-01 00:00:00" on first run (table empty or doesn't exist)
#       so that the Bronze filter selects ALL records on the initial load.
#     → Used in 02_silver_transformation to filter Bronze records:
#         bronze_df.filter(F.col("_ingested_at") > F.lit(wm_from).cast("timestamp"))
#     → On subsequent runs, only new Bronze records (after the last Silver run)
#       are processed — this is what makes the pipeline incremental.
#
# =============================================================================
