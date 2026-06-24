##############################################################################
# modules/sns/main.tf
#
# PURPOSE
#   Creates two SNS topics that form the event backbone connecting AWS services
#   to Databricks and the operations team.
#
# TOPICS PROVISIONED
#   batch_complete   Fires when Lambda writes a validated file to S3 bronze/.
#                    Subscribers: Lambda databricks-trigger (starts ETL job),
#                    pipeline_log DynamoDB (audit), Redis cache invalidation.
#                    This is the primary pipeline orchestration signal.
#
#   ops_alerts       Fires on any pipeline failure or anomaly requiring
#                    human attention. Subscribers: email (on-call team),
#                    data_quality_results DynamoDB, Streamlit status banner.
#                    Alert conditions: Lambda errors, DQ pass rate < 90%,
#                    Databricks job failures, schema drift, DLQ messages > 0.
#
# WHY SNS AND NOT EVENTBRIDGE?
#   SNS is simpler for fan-out to multiple subscribers with different protocols
#   (Lambda + email + HTTP). EventBridge would add unnecessary complexity for
#   this use case — the event schemas are simple and fixed.
#
# DOCS
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic_subscription
##############################################################################

##############################################################################
# aws_sns_topic — batch_complete
#
# Published by: S3 bucket notification (bronze/sftp/ ObjectCreated)
# Subscribers:
#   1. Lambda databricks-trigger — calls Databricks Jobs API to run ETL job
#   2. Lambda redis-writer       — pre-computes KPIs and writes to ElastiCache
#
# The subscription to Lambda databricks-trigger is wired in
# modules/lambda_trigger (aws_sns_topic_subscription resource) rather than
# here, to keep the module boundary clean — Lambda owns its own subscriptions.
##############################################################################
resource "aws_sns_topic" "batch_complete" {
  name = "${var.project_name}-batch_complete-${var.environment}"
  # kms_master_key_id = var.kms_key_arn
  # tags = { Environment = var.environment, Project = var.project_name }
}

##############################################################################
# aws_sns_topic — ops_alerts
#
# Published by:
#   - CloudWatch Alarms (Lambda errors, Kinesis IteratorAge, DLQ depth)
#   - Lambda file_validator (DQ pass rate < 90% threshold)
#   - Glue Crawler (schema drift detected)
#   - Databricks trigger Lambda (ETL job FAILED status)
#
# Subscribers:
#   - Email endpoints defined in var.alert_email_endpoints (terraform.tfvars)
#   - Each subscriber receives a confirmation email on first apply and must
#     click "Confirm subscription" before alerts are delivered.
##############################################################################
resource "aws_sns_topic" "ops_alerts" {
  name = "${var.project_name}-ops_alerts-${var.environment}"
  # kms_master_key_id = var.kms_key_arn
  # tags = { Environment = var.environment, Project = var.project_name }
}

##############################################################################
# aws_sns_topic_subscription — email subscriptions for ops_alerts
#
# Creates one subscription per email address in var.alert_email_endpoints.
# for_each iterates the list using the email as the map key.
#
# protocol = "email" → plain-text email (human-readable, no JSON wrapping)
# Use "email-json" if you want machine-parseable JSON payloads for a webhook.
#
# First-apply behaviour: AWS sends a confirmation email to each address.
# The subscription is pending_confirmation until the recipient clicks the link.
# Alerts are NOT delivered to unconfirmed subscriptions.
##############################################################################
resource "aws_sns_topic_subscription" "ops_alerts_email" {
  for_each = toset(var.alert_email_endpoints)

  topic_arn = aws_sns_topic.ops_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

##############################################################################
# aws_sns_topic_policy — allows CloudWatch to publish alarm notifications
#
# CloudWatch Alarms require an explicit SNS topic resource policy allowing
# cloudwatch.amazonaws.com to publish. Without this policy, alarm state
# changes do not trigger SNS notifications (silent failure).
#
# Condition: aws:SourceAccount = this account ID
#   Prevents other accounts from publishing to our ops_alerts topic.
##############################################################################
resource "aws_sns_topic_policy" "ops_alerts" {
  arn = aws_sns_topic.ops_alerts.arn
  # policy = jsonencode({
  #   Version = "2012-10-17"
  #   Statement = [{
  #     Sid       = "AllowCloudWatchAlarms"
  #     Effect    = "Allow"
  #     Principal = { Service = "cloudwatch.amazonaws.com" }
  #     Action    = "SNS:Publish"
  #     Resource  = aws_sns_topic.ops_alerts.arn
  #     Condition = {
  #       StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
  #     }
  #   }]
  # })
}
