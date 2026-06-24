##############################################################################
# modules/kinesis/main.tf
#
# PURPOSE
#   Creates the real-time event stream for ADT (Admit / Discharge / Transfer)
#   events and intra-shift staffing updates (callouts, OT requests, no-shows).
#   Lambda reads from this stream via an Event Source Mapping (ESM).
#
# STREAM ROLE IN THE PIPELINE
#   Hospital ADT Systems
#     → Kinesis rt-events-prod (this stream)
#     → Lambda ESM (batch 100 records, bisect-on-error)
#     → Lambda file_validator (same Lambda as batch path)
#       ├── valid  → S3 bronze/realtime/
#       └── bad    → SQS quarantine.fifo
#     → Databricks RT Streaming job (availableNow trigger, every 5 min)
#       → bronze.adt_events_raw → silver.adt_events_standardized
#       → gold.fact_census_realtime
#
# CONFIGURATION DECISIONS
#   shard_count = 4
#     4 shards = ~4,000 events/sec sustained write throughput.
#     Each shard handles 1,000 records/sec OR 1 MB/sec (whichever is lower).
#     Monitor IncomingBytes and ReadProvisionedThroughputExceeded CloudWatch
#     metrics — scale shards if consistently above 70% of capacity.
#
#   retention_period = 168 hours (7 days)
#     Allows full stream reprocessing after a Databricks streaming job failure.
#     Also covers a 3-day weekend gap (hospitals send bursts Monday morning).
#     7 days costs ~4× more than the 24-hour default — justified for HIPAA
#     reprocessing requirements.
#
#   stream_mode = PROVISIONED
#     Predictable, sustained event rate → fixed shard cost is cheaper than
#     ON_DEMAND for a known workload. Switch to ON_DEMAND if ADT event volume
#     becomes highly variable (seasonal spikes, new hospital onboarding).
#
# LAMBDA ESM (aws_lambda_event_source_mapping)
#   batch_size = 100
#     Lambda processes up to 100 Kinesis records per invocation.
#     Adjust based on Lambda memory and average record size.
#   bisect_batch_on_function_error = true
#     When Lambda fails on a batch, Kinesis splits the batch in half and
#     retries each half independently. This isolates the single bad record
#     that is causing failures instead of blocking the entire shard.
#   starting_position = "TRIM_HORIZON"
#     On first deployment, process all records from the beginning of the
#     stream (within the retention window). After the first run, the ESM
#     tracks the sequence number automatically via checkpoints.
#   on_failure_destination_arn → kinesis_dlq (STANDARD SQS)
#     ⚠️  Must be a standard SQS queue — FIFO queues are not supported as
#     ESM on_failure destinations.
#
# DOCS
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/kinesis_stream
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lambda_event_source_mapping
#   https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html
##############################################################################

##############################################################################
# aws_kinesis_stream — rt-events-prod
#
# All ADT events and real-time staffing updates flow through this stream.
# Encrypted with the platform KMS key (enforce_consumer_deletion = false
# is the default; consumers are Lambda ESM managed by Terraform).
##############################################################################
resource "aws_kinesis_stream" "rt_events" {
  name             = "${var.project_name}-rt-events-${var.environment}"
  retention_period = var.retention_hours  # 168 hours = 7 days

  stream_mode_details {
    stream_mode = var.stream_mode  # "PROVISIONED" or "ON_DEMAND"
  }

  # Only used when stream_mode = PROVISIONED
  # shard_count = var.shard_count  # 4 shards

  # encryption_type = "KMS"
  # key_id          = var.kms_key_arn

  # tags = { Environment = var.environment, Project = var.project_name }
}

##############################################################################
# aws_lambda_event_source_mapping — connects Kinesis stream to Lambda
#
# This resource makes Lambda a Kinesis consumer. When records arrive in the
# stream, Lambda is invoked automatically with a batch of records.
#
# bisect_batch_on_function_error = true
#   Critical for data quality: if one bad record in a 100-record batch causes
#   Lambda to throw, Kinesis retries the whole batch by default (blocking the
#   shard). bisect splits it in half, isolating the bad record quickly.
#   Combined with destination_config → on_failure, bad records land in
#   kinesis_dlq for investigation without blocking the stream.
#
# maximum_retry_attempts = 3
#   After 3 retries the batch is sent to kinesis_dlq. Without a limit,
#   a permanently bad record would block the shard forever.
##############################################################################
resource "aws_lambda_event_source_mapping" "kinesis_to_validator" {
  event_source_arn               = aws_kinesis_stream.rt_events.arn
  # function_name                  = var.lambda_validator_arn  ← from modules/lambda output
  starting_position              = "TRIM_HORIZON"
  batch_size                     = 100
  bisect_batch_on_function_error = true
  maximum_retry_attempts         = 3

  # destination_config {
  #   on_failure {
  #     destination_arn = var.kinesis_dlq_arn  ← MUST be standard SQS, not FIFO
  #   }
  # }
}
