# =============================================================================
# databricks_load/pipelines/bronze/00_run_glue_crawler.py
#
# PURPOSE
#   Runs the AWS Glue Crawler before Databricks Auto Loader ingests files.
#   This ensures that the Glue Catalog schema is up to date before
#   glue_utils.get_glue_schema_hints() is called in 01_bronze_ingestion.py.
#
# WHY RUN GLUE BEFORE BRONZE?
#   When a hospital sends a CSV with new or reordered columns, the Glue
#   Crawler needs to discover those columns first. If Bronze runs before the
#   crawler, Auto Loader may infer incorrect column types or miss new columns.
#   Running the crawler first guarantees schema hints are fresh for every ETL run.
#
# TASK POSITION IN ETL JOB
#   Task 1 of 5 in the Healthcare ETL Pipeline (job 727296529764626).
#   All other tasks depend on this one completing successfully.
#
# WHAT THE NOTEBOOK DOES
#
#   Step 1 — Load AWS credentials from Databricks secret scope "aws"
#     dbutils.secrets.get(scope="aws", key="access_key")
#     dbutils.secrets.get(scope="aws", key="secret_key")
#     Optional: session_token for temporary credentials.
#     Falls back to environment credentials if secret scope is unavailable
#     (works on classic clusters with instance profiles attached).
#
#   Step 2 — Check crawler current state
#     boto3 Glue client: get_crawler(Name=CRAWLER_NAME)
#     If state = "RUNNING" → skip start and wait for current run to finish.
#     If state = "READY"   → start a new crawler run.
#
#   Step 3 — Start crawler
#     boto3: start_crawler(Name=CRAWLER_NAME)
#     CRAWLER_NAME = "healthcare-data-platform-bronze-crawler-prod"
#
#   Step 4 — Poll until READY
#     Loops with time.sleep(10) checking crawler state every 10 seconds.
#     When state = "READY", checks LastCrawl.Status:
#       "SUCCEEDED" → logs success, notebook continues
#       anything else → raises RuntimeError with LastCrawl.ErrorMessage
#     Timeout: the Databricks task timeout (1800s in job config) handles
#     overall timeout — no need to implement a separate timeout in code.
#
# CREDENTIALS SETUP (one-time)
#   databricks secrets create-scope aws
#   databricks secrets put-secret aws access_key   --string-value <KEY>
#   databricks secrets put-secret aws secret_key   --string-value <SECRET>
#   databricks secrets put-secret aws session_token --string-value <TOKEN>
#
# =============================================================================
