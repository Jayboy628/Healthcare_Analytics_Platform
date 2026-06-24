# =============================================================================
# databricks_load/pipelines/batch_control/00_init_batch_control.py
#
# PURPOSE
#   One-time initialisation notebook. Creates all batch_control schema tables
#   and seeds file_schedule with the expected ingestion schedule per hospital.
#
# WHEN TO RUN
#   ONCE after: terraform apply (both passes) + create_all_tables.sql executed.
#   All statements are IF NOT EXISTS — safe to re-run without data loss.
#
# ⚠️  WARNING — CELL 4 IS DESTRUCTIVE (DEV ONLY)
#   Cell 4 drops file_registry, pipeline_runs, and scd2_audit then recreates
#   them empty. This is a development reset utility — it MUST NOT run in prod.
#   Comment out or delete Cell 4 before committing to the prod workspace.
#
# CELLS IN ORDER
#
#   Cell 1 — Set catalog context
#     spark.sql("USE CATALOG healthcare_catalog")
#     All subsequent SQL executes in healthcare_catalog context.
#
#   Cell 2 — Create batch_control tables (IF NOT EXISTS)
#     file_registry, pipeline_runs, scd2_audit, file_schedule.
#     See create_all_tables.sql for full column definitions and TBLPROPERTIES.
#
#   Cell 3 — Seed file_schedule via MERGE
#     Inserts expected schedule rows: MERGE ON (facility_id, file_type).
#     One row per (facility_id × file_type) combination:
#       STAFFING      → daily staffing roster from SFTP
#       SCHEDULE_DELTA → intra-shift callouts and reassignments
#       CALLOUT        → no-show / emergency callout via Kinesis
#     schedule_cron, grace_period_minutes, alert_severity vary per hospital SLA.
#     15 rows seeded for 5 test facilities × 3 file types.
#
#   Cell 4 — DEV ONLY: drop and recreate tables (⚠️  DESTRUCTIVE)
#     Used during development to reset schema without Terraform destroy.
#     Drops: file_registry, pipeline_runs, scd2_audit (NOT file_schedule).
#     ⚠️  DO NOT run in production.
#
#   Cell 5 — Verification
#     SELECT COUNT(*) from each table.
#     Expected: file_schedule has N rows, all others have 0 rows.
#
# =============================================================================
