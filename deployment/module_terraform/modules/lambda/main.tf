##############################################################################
# modules/lambda/main.tf
#
# PURPOSE
#   Creates the file_validator Lambda function — the single validation entry
#   point for ALL records entering the platform, both batch (S3/SQS) and
#   real-time (Kinesis ESM).
#
# WHAT THIS LAMBDA DOES
#   Triggered two ways:
#     1. SQS file_arrival queue  → processes hospital CSV files from landing/
#     2. Kinesis ESM             → processes ADT event batches from rt-events-prod
#
#   For every record it:
#     ├── Checks ingestion_ledger for duplicate (idempotency by file_checksum)
#     ├── Runs DQ rules: NULL_FACILITY_ID, CENSUS_OUT_OF_RANGE,
#     │                  OT_HOURS_EXCEED_MAX, NEGATIVE_STAFF_COUNT
#     ├── Valid records  → writes to S3 bronze/sftp/ or bronze/realtime/
#     ├── Bad records    → SQS quarantine.fifo + quarantine_index DynamoDB
#     ├── Writes DQ summary    → data_quality_results DynamoDB
#     ├── Registers schema     → schema_registry DynamoDB (conditional write)
#     ├── Writes ledger entry  → ingestion_ledger DynamoDB
#     └── Writes audit record  → S3 audit/ingestion/<date>/<checksum>.json
#
# PARTIAL BATCH SUPPORT
#   If a batch of records contains a mix of valid and invalid records,
#   Lambda processes ALL records — bad ones go to quarantine, good ones
#   continue to Bronze. The entire batch is NOT rejected on first bad record.
#
# PACKAGING
#   data.archive_file.validator zips the Python source directory at plan time.
#   source_code_hash triggers a Lambda update only when the zip content changes.
#   The zip file is gitignored — it is regenerated on each terraform plan.
#
# DOCS
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lambda_function
#   https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
#   https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html
##############################################################################

##############################################################################
# data.archive_file — packages the Python source into a deployment zip
#
# source_dir  → the directory containing app.py and any requirements
# output_path → where the zip is written locally (gitignored)
#
# Terraform recalculates output_base64sha256 on every plan — Lambda is
# only redeployed when the hash changes (i.e. source code actually changed).
##############################################################################
data "archive_file" "validator" {
  type        = "zip"
  source_dir  = "${path.root}/lambda_src/file_validator"
  output_path = "${path.module}/function_file_validator.zip"
}

##############################################################################
# aws_cloudwatch_log_group — Lambda log retention
#
# Create the log group before the Lambda function so Terraform controls
# the retention period. If the log group is created by Lambda automatically,
# it defaults to "Never expire" — causing unbounded storage cost growth.
#
# Retention: 90 days in prod (HIPAA audit trail minimum), 14 days in dev.
# /aws/lambda/<function_name> is the naming convention Lambda expects.
##############################################################################
resource "aws_cloudwatch_log_group" "validator" {
  name              = "/aws/lambda/${var.project_name}-file-validator-${var.environment}"
  retention_in_days = var.environment == "prod" ? 90 : 14
  # kms_key_id      = var.kms_key_arn  # encrypt log data at rest
}

##############################################################################
# aws_lambda_function — the file validator
#
# runtime = "python3.13"
#   Keep in sync with the Python version in the dev venv (hc_staff).
#   Lambda runtimes have end-of-support dates — check AWS docs yearly.
#
# memory_size = 512 MB
#   CSV parsing and DQ checks are memory-intensive for large hospital files
#   (40,000+ records). 512 MB provides a comfortable buffer and also
#   increases the vCPU allocation (Lambda allocates CPU proportionally).
#
# timeout = 300 seconds (5 minutes)
#   Must match (or be less than) the SQS visibility_timeout_seconds on
#   the file_arrival queue to prevent duplicate processing.
#
# environment variables
#   All configuration is injected via env vars — no hardcoded values in code.
#   QUARANTINE_QUEUE_URL: the SQS FIFO quarantine queue URL
#   DYNAMO_LEDGER_TABLE:  healthcare-data-platform_ingestion_ledger_prod
#   DYNAMO_LOG_TABLE:     healthcare-data-platform_pipeline_log_prod
#   DYNAMO_DQ_TABLE:      healthcare-data-platform_data_quality_results_prod
#   DYNAMO_QINDEX_TABLE:  healthcare-data-platform_quarantine_index_prod
#   DYNAMO_SCHEMA_TABLE:  healthcare-data-platform_schema_registry_prod
#   BRONZE_BUCKET:        hc-data-lake-prod
#   BRONZE_PREFIX:        bronze/sftp/
#
# tracing_config mode = "Active"
#   Enables AWS X-Ray distributed tracing. Traces Lambda invocations end-to-end
#   through SQS → Lambda → S3 → DynamoDB for performance debugging.
#
# reserved_concurrent_executions = -1
#   -1 means unreserved (uses account-level concurrency pool).
#   Set a positive number if you want to throttle Lambda to protect downstream
#   DynamoDB or S3 write throughput during traffic spikes.
##############################################################################
resource "aws_lambda_function" "file_validator" {
  function_name    = "${var.project_name}-file-validator-${var.environment}"
  handler          = "app.lambda_handler"
  runtime          = "python3.13"
  # role             = var.lambda_role_arn   ← from modules/iam output
  memory_size      = 512
  timeout          = 300
  filename         = data.archive_file.validator.output_path
  source_code_hash = data.archive_file.validator.output_base64sha256

  environment {
    variables = {
      ENVIRONMENT           = var.environment
      BRONZE_BUCKET         = var.bucket_name
      BRONZE_PREFIX         = "bronze/sftp/"
      AUDIT_PREFIX          = "audit/ingestion/"
      QUARANTINE_PREFIX     = "quarantine/"
      # QUARANTINE_QUEUE_URL  = var.quarantine_queue_url    ← from modules/sqs output
      # DYNAMO_LEDGER_TABLE   = var.dynamo_table_names["ingestion_ledger"]
      # DYNAMO_LOG_TABLE      = var.dynamo_table_names["pipeline_log"]
      # DYNAMO_DQ_TABLE       = var.dynamo_table_names["data_quality_results"]
      # DYNAMO_QINDEX_TABLE   = var.dynamo_table_names["quarantine_index"]
      # DYNAMO_SCHEMA_TABLE   = var.dynamo_table_names["schema_registry"]
    }
  }

  reserved_concurrent_executions = -1

  tracing_config {
    mode = "Active"
  }

  depends_on = [aws_cloudwatch_log_group.validator]

  # tags = { Environment = var.environment, Project = var.project_name }
}

##############################################################################
# aws_lambda_event_source_mapping — SQS trigger (file_arrival queue)
#
# batch_size = 1
#   Process one S3 event notification at a time. Each SQS message represents
#   one S3 ObjectCreated event (one CSV file). Setting batch_size > 1 would
#   invoke Lambda with multiple file notifications per call — more complex
#   error handling for no benefit (files are processed serially anyway).
#
# function_response_types = ["ReportBatchItemFailures"]
#   Enables partial batch failure responses. If Lambda fails on one message
#   in a batch, it can return that message's receipt handle and SQS will
#   retry only that one message (not the entire batch). Essential when
#   batch_size > 1.
##############################################################################
resource "aws_lambda_event_source_mapping" "sqs_to_validator" {
  # event_source_arn        = var.file_arrival_queue_arn  ← from modules/sqs output
  function_name             = aws_lambda_function.file_validator.arn
  batch_size                = 1
  function_response_types   = ["ReportBatchItemFailures"]
}

##############################################################################
# aws_lambda_permission — allows SQS to invoke the Lambda
#
# Without this permission, SQS cannot call lambda:InvokeFunction even if the
# Lambda has the correct IAM role. This is a separate resource-based policy
# on the Lambda function itself.
##############################################################################
resource "aws_lambda_permission" "sqs_invoke" {
  statement_id  = "AllowSQSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.file_validator.function_name
  principal     = "sqs.amazonaws.com"
  # source_arn    = var.file_arrival_queue_arn
}
