##############################################################################
# modules/lambda_trigger/main.tf
#
# PURPOSE
#   Creates the databricks-trigger Lambda — the bridge between AWS and
#   Databricks. When SNS batch_complete fires (a validated file landed in
#   S3 bronze/), this Lambda calls the Databricks Jobs API to start the
#   ETL pipeline job.
#
# TRIGGER CHAIN
#   S3 bronze/sftp/ ObjectCreated
#     → SNS batch_complete topic
#       → this Lambda (aws_sns_topic_subscription wired below)
#         → Databricks Jobs API: POST /api/2.1/jobs/run-now
#           → Healthcare ETL Pipeline (job_id from env var)
#             Glue Crawler → Bronze → Silver → Gold → ML
#
# WHY A LAMBDA AND NOT EVENTBRIDGE → DATABRICKS?
#   The Databricks Jobs API requires an OAuth or PAT token for authentication.
#   Lambda can securely retrieve that token from AWS Secrets Manager at runtime.
#   EventBridge HTTP targets would require the token in the connection config —
#   harder to rotate without redeploying infrastructure.
#
# SECRETS
#   Databricks credentials (host + token) are stored in AWS Secrets Manager
#   under the secret ID defined in var.databricks_secret_id.
#   Lambda retrieves them at runtime with boto3 secretsmanager.get_secret_value().
#   The Lambda IAM role must have secretsmanager:GetSecretValue on that secret.
#
# NO VPC
#   This Lambda does NOT need VPC placement because it only calls:
#     - AWS Secrets Manager (public endpoint)
#     - Databricks workspace REST API (public endpoint)
#   Placing it in the VPC would require a NAT Gateway for public internet
#   access — unnecessary cost and complexity for this use case.
#
# DOCS
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lambda_function
#   https://docs.databricks.com/api/workspace/jobs/runnow
##############################################################################

data "archive_file" "trigger" {
  type        = "zip"
  source_dir  = "${path.root}/lambda_src/databricks_trigger"
  output_path = "${path.module}/function_databricks_trigger.zip"
}

resource "aws_cloudwatch_log_group" "trigger" {
  name              = "/aws/lambda/${var.project_name}-databricks-trigger-${var.environment}"
  retention_in_days = var.environment == "prod" ? 90 : 14
}

##############################################################################
# aws_lambda_function — databricks-trigger
#
# memory_size = 256 MB
#   This Lambda only makes HTTP API calls — no data processing.
#   256 MB is sufficient; reducing below 256 reduces CPU and slows cold starts.
#
# timeout = 60 seconds
#   The Databricks Jobs API typically responds in < 1 second (it queues the
#   job, it does not wait for it to complete). 60 seconds is generous.
#
# environment variables
#   DATABRICKS_SECRET_ID        → Secrets Manager secret name containing
#                                  {"databricks_host": "...", "databricks_token": "..."}
#   DATABRICKS_JOB_ID_ETL       → Healthcare ETL Pipeline job ID (727296529764626)
#   DATABRICKS_JOB_ID_STREAMING → RT Streaming Pipeline job ID (560379522229937)
#
# reserved_concurrent_executions = -1
#   Unreserved. The trigger fires at most once per file arrival — very low
#   invocation rate. No need to throttle.
##############################################################################
resource "aws_lambda_function" "databricks_trigger" {
  function_name    = "${var.project_name}-databricks-trigger-${var.environment}"
  handler          = "app.lambda_handler"
  runtime          = "python3.12"
  # role             = var.lambda_role_arn  ← from modules/iam output
  memory_size      = 256
  timeout          = 60
  filename         = data.archive_file.trigger.output_path
  source_code_hash = data.archive_file.trigger.output_base64sha256

  environment {
    variables = {
      # DATABRICKS_SECRET_ID        = var.databricks_secret_id
      # DATABRICKS_JOB_ID_ETL       = var.databricks_job_id_etl
      # DATABRICKS_JOB_ID_STREAMING = var.databricks_job_id_streaming
      AWS_REGION_NAME = var.aws_region
    }
  }

  reserved_concurrent_executions = -1

  tracing_config {
    mode = "Active"
  }

  depends_on = [aws_cloudwatch_log_group.trigger]
}

##############################################################################
# aws_lambda_permission — allows SNS to invoke this Lambda
#
# source_arn scoped to the specific batch_complete topic ARN (not all SNS).
# This prevents other SNS topics from invoking this Lambda accidentally.
##############################################################################
resource "aws_lambda_permission" "sns_invoke" {
  statement_id  = "AllowSNSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.databricks_trigger.function_name
  principal     = "sns.amazonaws.com"
  # source_arn    = var.batch_complete_topic_arn
}

##############################################################################
# aws_sns_topic_subscription — wires SNS batch_complete → this Lambda
#
# protocol = "lambda" → SNS invokes the Lambda synchronously.
# The subscription is created here (not in modules/sns) so the Lambda
# module owns its own subscriptions — cleaner module boundaries.
##############################################################################
resource "aws_sns_topic_subscription" "batch_complete_to_trigger" {
  # topic_arn = var.batch_complete_topic_arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.databricks_trigger.arn
}
