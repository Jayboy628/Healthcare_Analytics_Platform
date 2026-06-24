# =============================================================================
# databricks_load/pipelines/gold/03_gold_unified_analytics.py
#
# PURPOSE
#   Builds all Gold fact and dimension tables by combining batch staffing data
#   from Silver and real-time ADT census data from silver.adt_events_standardized.
#   Produces the business-ready datasets consumed by the Streamlit dashboard,
#   Databricks SQL Warehouse, Power BI, and the ML feature pipeline.
#
# DESIGN PATTERN: MIXED (declarative SQL logic, procedural SCD2)
#   Fact tables use declarative MERGE (DeltaTable API).
#   dim_facility uses procedural SCD2 logic (find changed rows → expire old →
#   insert new) because SCD2 requires multi-step conditional logic.
#
# TASK POSITION IN ETL JOB: Task 4 of 5
#
# UTILITIES LOADED
#   %run /Workspace/Shared/healthcare/common/config/pipeline_config
#   %run /Workspace/Shared/healthcare/common/utils/batch_control_utils
#   %run /Workspace/Shared/healthcare/common/utils/dynamo_utils
#
# TABLES PRODUCED
#   gold.fact_staffing        Daily staffing grain: (date_key, facility_id, role_code)
#   gold.fact_overtime        OT records: (date_key, facility_id, role_code, shift_type)
#   gold.fact_census_realtime Hourly ADT census: (facility_id, unit_id, event_date, event_hour)
#   gold.dim_facility         SCD2 facility dimension: (facility_key, facility_id, ...)
#
# STEP-BY-STEP
#
#   Part 1 — DEDUP SILVER BEFORE ALL MERGES (CRITICAL)
#     Silver can have multiple rows per (facility_id, work_date, role_code) from
#     multiple incremental runs. Gold MERGE keys must map 1:1 from source to target.
#     Window: ROW_NUMBER OVER (PARTITION BY facility_id, work_date, role_code
#                              ORDER BY _processed_at DESC)
#     Keep _rn == 1. Drop _rn column.
#     This is applied ONCE to silver_df and reused by all fact table builds.
#
#   Part 2 — fact_staffing
#     Grain: one row per (date_key, facility_id, role_code)
#     date_key = date_format("work_date", "yyyyMMdd").cast("int")
#     Derived: is_understaffed = nurse_patient_ratio < DQ_RULES["understaffed_threshold"]
#     MERGE key: (date_key, facility_id, role_code)
#
#   Part 3 — fact_overtime
#     Grain: one row per (date_key, facility_id, role_code, shift_type)
#     WHY include shift_type in MERGE key?
#       A nurse can work both day AND night shift on the same date, both generating
#       overtime independently. Without shift_type in the key, two OT rows for the
#       same role/date collide in the MERGE.
#     overtime_cost_est = hours_overtime * DQ_RULES["overtime_blended_rate"] ($65/hr)
#
#   Part 4 — fact_census_realtime (if silver.adt_events_standardized exists)
#     Running census = cumulative SUM(census_delta) per (facility, unit) ordered by event_ts
#     Aggregated to hourly grain: last census value per (facility, unit, date, hour)
#     + count of admits/discharges/transfers per hour
#     MERGE key: (facility_id, unit_id, event_date, event_hour)
#
#   Part 5 — dim_facility SCD2
#     Source: all distinct facility_ids from silver_df (+ adt Silver if available)
#     FIRST RUN: write with is_current=True, effective_from=today, effective_to=NULL
#     SUBSEQUENT RUNS:
#       a. Find facilities not yet in dim (left_anti join) → insert new rows
#       b. Find facilities where tracked attributes changed → expire old row
#          (set is_current=False, effective_to=today)
#       c. Insert new current row for changed facilities
#
#   Part 6 — Write job bookmark and pipeline log
#     write_job_bookmark("gold_unified_analytics", TABLES["silver_staffing"], total)
#     write_pipeline_log("Gold_Unified_Analytics", total)
#
# =============================================================================
