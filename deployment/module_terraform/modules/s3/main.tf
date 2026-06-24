##############################################################################
# modules/s3/main.tf
#
# PURPOSE
#   Creates the central S3 data lake bucket and all supporting configurations.
#   One bucket holds every pipeline layer: landing → bronze → silver → gold →
#   ml-ready, plus audit, quarantine, and Auto Loader checkpoint prefixes.
#   Layers are separated by S3 prefix (folder), not by separate buckets.
#
# WHY ONE BUCKET?
#   Simplifies IAM policies (one bucket ARN), KMS key policy (one SourceArn),
#   and Cross-Origin resource sharing. Databricks Unity Catalog external
#   locations are scoped per prefix, so governance is still enforced.
#
# HIPAA NOTE
#   All objects are encrypted with a customer-managed KMS key (CMK).
#   HIPAA requires that encryption key usage be auditable — AWS KMS logs
#   every Encrypt/Decrypt call to CloudTrail, satisfying that requirement.
#   SSE-S3 (AWS-managed keys) does NOT provide per-key audit trails and
#   is therefore not compliant for PHI storage.
#
# DOCS
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_server_side_encryption_configuration
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_notification
#   https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html
##############################################################################

##############################################################################
# aws_s3_bucket — the central data lake bucket
#
# bucket       → from var.bucket_name (set in terraform.tfvars as "hc-data-lake-prod")
# force_destroy → false in prod; prevents accidental delete via terraform destroy.
#                 Set true only in dev/test where teardown is routine.
#
# Tags are applied for cost allocation and compliance reporting.
##############################################################################
resource "aws_s3_bucket" "data_lake" {
  # bucket        = var.bucket_name
  # force_destroy = var.environment != "prod"

  # tags = {
  #   Environment = var.environment
  #   Project     = var.project_name
  #   HIPAA       = "true"
  # }
}

##############################################################################
# aws_s3_bucket_versioning
#
# WHY VERSIONING?
#   - Supports accidental-deletion recovery (HIPAA audit requirement)
#   - Allows rollback if a bad pipeline run overwrites clean Bronze data
#   - Delta Lake transaction log relies on consistent object versions
#
# Note: Versioning can be enabled on a bucket that already has objects —
# no data loss. "Suspending" versioning (once enabled) only stops creating
# new versions; existing versions are retained until explicitly deleted.
##############################################################################
resource "aws_s3_bucket_versioning" "data_lake" {
  # bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

##############################################################################
# aws_s3_bucket_public_access_block
#
# All four flags set to true = most restrictive posture.
# The bucket stores patient-adjacent healthcare data — it must NEVER be
# publicly accessible regardless of bucket policy or object ACL.
#
# block_public_acls       → prevents any public ACL from being applied
# block_public_policy     → prevents any public bucket policy from being set
# ignore_public_acls      → ignores any existing public ACLs (defence in depth)
# restrict_public_buckets → blocks all cross-account public access
#
# Without this resource, a mis-applied bucket policy or ACL could accidentally
# expose data. This is a belt-and-suspenders control on top of IAM policies.
##############################################################################
resource "aws_s3_bucket_public_access_block" "data_lake" {
  # bucket = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

##############################################################################
# aws_s3_bucket_server_side_encryption_configuration
#
# sse_algorithm = "aws:kms"
#   Uses the customer-managed KMS key (CMK) created in modules/kms.
#   Every object written to this bucket is envelope-encrypted with the CMK.
#   Every read requires KMS Decrypt permission — unauthorised readers see
#   only ciphertext, enforcing access control at the data layer.
#
# kms_master_key_id
#   The ARN of the CMK from modules/kms output. The KMS key policy must
#   explicitly grant S3 the GenerateDataKey + Decrypt permissions.
#   Without this, S3 PUT operations fail with "KMS key not accessible".
#
# bucket_key_enabled = true
#   Reduces KMS API call volume by generating a bucket-level data key
#   that is cached per prefix. Each prefix generates one key request per
#   hour instead of one per object — reduces KMS costs by up to 99% on
#   high-volume buckets (Bronze receives hundreds of files per day).
##############################################################################
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  # bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      # kms_master_key_id = var.kms_key_arn  ← output from modules/kms
    }
    bucket_key_enabled = true
  }
}

##############################################################################
# aws_s3_bucket_lifecycle_configuration
#
# Automatically moves objects to cheaper storage tiers and expires them
# when they exceed the retention window. Controls total storage cost.
#
# RETENTION POLICY (by prefix):
#   landing/     90 days   Raw hospital SFTP files — replaced by validated
#                          Bronze copy after Lambda processes them.
#   bronze/       1 year   Validated source records — needed if Silver must
#                          be reprocessed from scratch.
#   silver/       3 years  Standardised records — regulatory requirement.
#   gold/         7 years  Business KPIs — HIPAA minimum retention.
#   audit/        7 years  HIPAA requires ≥ 6 years; 7 is a safe buffer.
#   quarantine/  180 days  Bad records — held for ops remediation review.
#   ml-ready/     1 year   Feature datasets refreshed on every ETL run.
#
# TRANSITION TIERS (cost optimisation):
#   Standard → Standard-IA at 30 days   (infrequent access, same speed)
#   Standard-IA → Glacier IR at 90 days  (archived, minutes retrieval)
#   Glacier IR → Deep Archive at 365 days (compliance storage, 12hr retrieval)
##############################################################################
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  # bucket = aws_s3_bucket.data_lake.id
  # depends_on = [aws_s3_bucket_versioning.data_lake]

  # TODO: one rule block per prefix — example:
  # rule {
  #   id     = "landing-90-day-expiry"
  #   status = "Enabled"
  #   filter { prefix = "landing/" }
  #   expiration { days = 90 }
  #   transition { days = 30; storage_class = "STANDARD_IA" }
  # }
}

##############################################################################
# aws_s3_bucket_notification — event-driven pipeline entry point
#
# HOW IT WORKS
#   When a CSV file lands in landing/sftp/, S3 fires a notification to the
#   file_arrival SQS queue. Lambda consumes that message and starts validation.
#   This is the first step of the entire batch ingestion pipeline:
#     S3 ObjectCreated → SQS file_arrival → Lambda file_validator
#
#   A second notification fires to SNS batch_complete when a validated
#   file is written to bronze/sftp/ — this triggers the Databricks ETL job
#   via the Lambda databricks-trigger.
#
# CRITICAL: SQS QUEUE POLICY REQUIRED
#   S3 cannot deliver notifications to SQS unless the queue has a resource
#   policy explicitly allowing s3.amazonaws.com to call sqs:SendMessage with
#   aws:SourceArn = this bucket ARN. That policy is in modules/sqs.
#   If it is missing, S3 notifications are silently dropped — no error is
#   logged anywhere and Lambda is never invoked. This is the most common
#   misconfiguration during initial setup.
#
# FILE EXTENSIONS WIRED
#   .csv, .tsv, .gz (landing/ prefix only)
#   Bronze/ and above are read by Databricks Auto Loader, not by S3 trigger.
##############################################################################
resource "aws_s3_bucket_notification" "data_lake" {
  # bucket     = aws_s3_bucket.data_lake.id
  # depends_on = [aws_sqs_queue_policy.file_arrival]  ← from modules/sqs output

  # SQS notification — landing CSV files trigger Lambda file_validator
  # queue_configuration {
  #   id            = "landing-csv-to-file-arrival"
  #   queue_arn     = var.file_arrival_queue_arn   ← from modules/sqs output
  #   events        = ["s3:ObjectCreated:*"]
  #   filter_prefix = "landing/"
  #   filter_suffix = ".csv"
  # }

  # SNS notification — bronze writes trigger the Databricks ETL job
  # topic_configuration {
  #   id            = "bronze-sftp-to-batch-complete"
  #   topic_arn     = var.batch_complete_topic_arn  ← from modules/sns output
  #   events        = ["s3:ObjectCreated:*"]
  #   filter_prefix = "bronze/sftp/"
  #   filter_suffix = ".csv"
  # }
}
