##############################################################################
# modules/sqs/main.tf
#
# PURPOSE
#   Creates all four SQS queues used by the platform, each serving a
#   distinct role in the ingestion and error-handling pipeline.
#
# QUEUES PROVISIONED
#   file_arrival     (STANDARD) Receives S3 ObjectCreated notifications from
#                               landing/sftp/. Lambda file_validator consumes
#                               from here to start batch validation.
#   quarantine       (FIFO)     Receives one message per bad record from Lambda.
#                               Held here until ops team drains during remediation.
#   kinesis_dlq      (STANDARD) Kinesis Lambda ESM on_failure destination.
#                               Captures batches Lambda could not process after
#                               all retries were exhausted.
#   file_arrival_dlq (STANDARD) Dead-letter queue for file_arrival. Receives
#                               messages Lambda could not process after
#                               maxReceiveCount attempts (envelope-level failures).
#
# WHY IS QUARANTINE FIFO?
#   FIFO guarantees exactly-once delivery within a message group and preserves
#   order. This prevents the same bad record from being remediated by two
#   concurrent consumers simultaneously, which would create duplicate rows in
#   quarantine_index DynamoDB.
#
# ⚠️  CRITICAL: FIFO queues CANNOT be used as Kinesis ESM on_failure
#   destinations. The kinesis_dlq MUST be a standard queue. Using
#   quarantine.fifo as the ESM destination causes a silent delivery failure
#   with no error surfaced in Lambda or CloudWatch.
#   See: https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html
#
# ⚠️  CRITICAL: SQS QUEUE POLICY FOR S3 NOTIFICATIONS
#   The file_arrival queue must have a resource policy allowing
#   s3.amazonaws.com to call sqs:SendMessage. Without it, S3 notifications
#   are silently dropped — Lambda is never invoked and no error is logged.
#   This is the most common misconfiguration during initial setup.
#
# DOCS
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sqs_queue
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sqs_queue_policy
##############################################################################

##############################################################################
# aws_sqs_queue — file_arrival (standard)
#
# visibility_timeout_seconds = 300
#   Must be >= the Lambda function timeout (also 300s).
#   If Lambda takes longer than the visibility timeout to process a message,
#   SQS makes the message visible again — another Lambda picks it up causing
#   duplicate processing. Set to 6× Lambda timeout as a safe buffer.
#
# message_retention_seconds = 345600 (4 days)
#   If Lambda is down, messages queue here and are processed on recovery.
#   4 days covers a long weekend outage.
#
# redrive_policy
#   After maxReceiveCount=3 failed attempts, the message moves to
#   file_arrival_dlq for investigation. A CloudWatch alarm fires when
#   the DLQ has any messages (CRITICAL severity).
#
# Queue policy (aws_sqs_queue_policy below)
#   Allows s3.amazonaws.com to SendMessage to this queue.
#   aws:SourceArn restricted to the data lake bucket ARN.
#   aws:SourceAccount restricted to this AWS account ID.
##############################################################################
resource "aws_sqs_queue" "file_arrival" {
  name                       = "${var.project_name}-file_arrival-${var.environment}"
  fifo_queue                 = false
  visibility_timeout_seconds = 300
  message_retention_seconds  = 345600
  # kms_master_key_id        = var.kms_key_arn
  # redrive_policy = jsonencode({
  #   deadLetterTargetArn = aws_sqs_queue.file_arrival_dlq.arn
  #   maxReceiveCount     = 3
  # })
}

##############################################################################
# aws_sqs_queue_policy — grants S3 permission to publish to file_arrival
#
# Without this policy, S3 event notifications cannot deliver to SQS.
# The condition restricts delivery to objects from this specific bucket only
# (not any S3 bucket in the account), which is a security best practice.
#
# Principal  : "s3.amazonaws.com" (the S3 service, not a user)
# Action     : "sqs:SendMessage"
# Condition  : aws:SourceArn must match the data lake bucket ARN
##############################################################################
resource "aws_sqs_queue_policy" "file_arrival" {
  queue_url = aws_sqs_queue.file_arrival.id
  # policy = jsonencode({
  #   Version = "2012-10-17"
  #   Statement = [{
  #     Sid       = "AllowS3BucketNotifications"
  #     Effect    = "Allow"
  #     Principal = { Service = "s3.amazonaws.com" }
  #     Action    = "sqs:SendMessage"
  #     Resource  = aws_sqs_queue.file_arrival.arn
  #     Condition = {
  #       ArnLike           = { "aws:SourceArn" = "arn:aws:s3:::${var.bucket_name}" }
  #       StringEquals      = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
  #     }
  #   }]
  # })
}

##############################################################################
# aws_sqs_queue — quarantine (FIFO)
#
# Receives bad records from Lambda file_validator when DQ checks fail.
# Each message contains: raw record, DQ failure reasons, source file path,
# facility_id, and ingestion_timestamp.
#
# content_based_deduplication = true
#   Uses SHA-256 hash of the message body as the deduplication ID.
#   Prevents the same bad record from being enqueued twice if Lambda retries
#   the same batch (e.g. after a transient network error).
#
# MessageGroupId (set by Lambda, not here): "dq-failures"
#   All DQ failures share one message group → strict FIFO ordering.
#
# message_retention_seconds = 1209600 (14 days)
#   Ops team has two weeks to review and remediate before records expire.
#   Records that cannot be fixed should be marked resolved=True in
#   quarantine_index DynamoDB and then deleted from this queue manually.
#
# ⚠️  Queue name MUST end with ".fifo" — AWS requirement for FIFO queues.
##############################################################################
resource "aws_sqs_queue" "quarantine" {
  name                        = "${var.project_name}-quarantine-${var.environment}.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  visibility_timeout_seconds  = 300
  message_retention_seconds   = 1209600
  # kms_master_key_id         = var.kms_key_arn
}

##############################################################################
# aws_sqs_queue — kinesis_dlq (standard)
#
# Destination for Kinesis Lambda ESM on_failure events.
# When Lambda fails to process a Kinesis shard batch after all retries,
# the failed batch metadata is sent here for investigation.
#
# WHY STANDARD (not FIFO)?
#   Lambda ESM on_failure_destination only supports standard SQS queues.
#   Using a FIFO queue raises InvalidParameterValueException during apply.
#   Ref: https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html
#
# message_retention_seconds = 1209600 (14 days)
#   Kinesis batch failures are rare — long retention gives time to diagnose
#   and decide whether to replay the original Kinesis shard or discard.
##############################################################################
resource "aws_sqs_queue" "kinesis_dlq" {
  name                      = "${var.project_name}-kinesis_dlq-${var.environment}"
  fifo_queue                = false
  message_retention_seconds = 1209600
  # kms_master_key_id       = var.kms_key_arn
}

##############################################################################
# aws_sqs_queue — file_arrival_dlq (standard)
#
# Dead-letter queue for the file_arrival standard queue.
# Receives messages after maxReceiveCount=3 failed Lambda processing attempts.
#
# A message here means Lambda received the S3 notification but threw an
# unhandled exception — NOT a DQ failure (those are caught and quarantined
# cleanly). This indicates an envelope-level problem: malformed S3 event
# JSON, Lambda crash, missing environment variable, or IAM permission error.
#
# CloudWatch alarm: messages > 0 → CRITICAL SNS ops_alert
# Operations response: check Lambda logs for the exception traceback.
##############################################################################
resource "aws_sqs_queue" "file_arrival_dlq" {
  name                      = "${var.project_name}-file_arrival-dlq-${var.environment}"
  fifo_queue                = false
  message_retention_seconds = 1209600
  # kms_master_key_id       = var.kms_key_arn
}
