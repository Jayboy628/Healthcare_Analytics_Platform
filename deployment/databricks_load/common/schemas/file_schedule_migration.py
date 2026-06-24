# =============================================================================
# databricks_load/common/schemas/file_schedule_migration.py
#
# PURPOSE
#   Migrates the batch_control.file_schedule table from the original schema
#   (created by 00_init_batch_control.py) to the new schema that supports
#   richer scheduling configuration and alert severity levels.
#
# WHEN TO RUN
#   Run ONCE in a Databricks notebook cell AFTER 00_init_batch_control.py
#   has been executed. Safe to re-run — the script checks whether the new
#   columns already exist before altering the table.
#
# ORIGINAL SCHEMA (created by 00_init_batch_control.py)
#   hospital_id        STRING
#   expected_day       STRING  (e.g. "MONDAY", "DAILY")
#   expected_by_hour   INT     (e.g. 8 for 8 AM UTC)
#   grace_period_hrs   INT
#   is_active          BOOLEAN
#   last_received_at   TIMESTAMP
#   consecutive_misses INT
#
# NEW SCHEMA (after migration)
#   facility_id           STRING   (renamed from hospital_id for consistency
#                                   with all other tables that use facility_id)
#   file_type             STRING   (STAFFING, SCHEDULE_DELTA, CALLOUT — new)
#   schedule_cron         STRING   (full cron expression replacing expected_day
#                                   + expected_by_hour — more flexible)
#   grace_period_minutes  INT      (renamed from grace_period_hrs and converted
#                                   to minutes for finer control)
#   alert_severity        STRING   (CRITICAL, WARN, INFO — new)
#   is_active             BOOLEAN  (unchanged)
#   last_received_at      TIMESTAMP (unchanged)
#   consecutive_misses    INT      (unchanged)
#
# HOW THE MIGRATION WORKS
#   1. Reads the existing file_schedule table into a DataFrame
#   2. Renames hospital_id → facility_id
#   3. Adds file_type column (default: "STAFFING" for all existing rows)
#   4. Converts expected_day + expected_by_hour → schedule_cron expression
#      e.g. expected_day="DAILY", expected_by_hour=8 → "0 8 * * *"
#   5. Converts grace_period_hrs (INT) → grace_period_minutes (INT * 60)
#   6. Adds alert_severity column (default: "WARN" for all existing rows)
#   7. Drops original columns (expected_day, expected_by_hour, grace_period_hrs)
#   8. Creates a backup table (file_schedule_backup_<date>) before overwriting
#   9. Overwrites the original table with the migrated DataFrame
#  10. Verifies row count matches original
#
# ROLLBACK
#   If migration fails, restore from backup:
#     DROP TABLE IF EXISTS healthcare_catalog.batch_control.file_schedule;
#     ALTER TABLE healthcare_catalog.batch_control.file_schedule_backup_<date>
#       RENAME TO file_schedule;
#
# =============================================================================
