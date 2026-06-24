# =============================================================================
# databricks_load/common/utils/glue_utils.py
#
# PURPOSE
#   Queries the AWS Glue Catalog to retrieve column schema hints for Databricks
#   Auto Loader. Bridges the Glue schema catalog with the Databricks ingestion
#   pipeline so that Auto Loader knows what column types to expect before it
#   reads files.
#
# HOW IT IS LOADED
#   %run /Shared/healthcare/common/utils/glue_utils
#   (in its own cell)
#
# WHY GLUE SCHEMA HINTS?
#   Auto Loader (cloudFiles format) infers column types from a sample of files
#   on first run. This inference is not always accurate — patient_census might
#   be inferred as DOUBLE when it should be INT, or a date column might be
#   inferred as STRING. Incorrect types cause Silver type-casting failures.
#
#   By querying Glue Catalog (which the Glue Crawler has already populated with
#   correct types via schema sampling), we pass explicit cloudFiles.schemaHints
#   to Auto Loader. This overrides inference and ensures consistent typing
#   regardless of the file sample Auto Loader happens to read first.
#
# FUNCTIONS
#
#   get_glue_schema_hints(spark, glue_table, glue_db)
#     → Parameters:
#         spark      — the Databricks SparkSession
#         glue_table — the Glue Catalog table name (e.g. "sftp")
#         glue_db    — the Glue Catalog database name
#                      default: "healthcare-data-platform_bronze_prod"
#
#     → Returns a comma-separated string of "column_name STRING" hints.
#       Example: "facility_id STRING, work_date STRING, staff_count STRING, ..."
#
#       Note: ALL columns are returned as STRING even if Glue inferred them
#       as INTEGER or DOUBLE. This is intentional — Auto Loader ingests
#       everything as STRING in Bronze (TABLES["bronze_staffing"] uses all
#       STRING types). Type casting happens in Silver via dq_utils.cast_silver_types().
#
#     → If the Glue query fails (permissions error, table not yet crawled,
#       or Glue Catalog unavailable), falls back to DEFAULT_SCHEMA_HINTS —
#       a hardcoded list of the expected columns. This ensures the Bronze
#       notebook can still run even if the Glue Crawler has not run yet on
#       first deployment.
#
#   DEFAULT_SCHEMA_HINTS (module-level constant)
#     → Fallback schema used when Glue is unavailable.
#       Contains the core columns all hospital SFTP files are expected to have:
#         facility_id, work_date, role_code, staff_count, patient_census,
#         hours_worked, hours_worked_overtime, shift_type
#
# IMPORTANT IMPLEMENTATION DETAIL — BACKTICK QUOTING
#   The Glue database name (healthcare-data-platform_bronze_prod) contains
#   hyphens. Hyphens are invalid in unquoted SQL identifiers. The query
#   MUST use backtick quoting:
#     `healthcare-data-platform_bronze_prod`.`information_schema`.`columns`
#   Without backticks, Spark SQL parses the hyphen as a subtraction operator
#   and raises AnalysisException.
#
# =============================================================================
