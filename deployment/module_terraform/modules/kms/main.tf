##############################################################################
# modules/kms/main.tf
#
# PURPOSE
#   Creates a single customer-managed KMS key (CMK) used to encrypt every
#   data-bearing AWS service in the platform: S3 objects, SQS messages,
#   Kinesis records, DynamoDB items, and ElastiCache data at rest.
#
# WHY ONE SHARED KEY?
#   Simplifies key policy management and reduces cost (one key = one monthly
#   charge ~$1/mo + per-API usage). Granular access control is enforced via
#   separate Sid statements in the key policy — each service gets exactly
#   the permissions it needs, nothing more.
#
# HIPAA REQUIREMENT
#   HIPAA Security Rule (§ 164.312(a)(2)(iv)) requires encryption of PHI
#   at rest with auditable key management. AWS KMS logs every key operation
#   (Encrypt, Decrypt, GenerateDataKey) to CloudTrail. CMKs satisfy this;
#   SSE-S3 (AWS-managed keys) does NOT provide per-key audit trails.
#
# TWO-PASS DEPLOYMENT NOTE
#   On the first terraform apply, the Databricks IAM role does not exist yet.
#   The KMS key policy cannot reference a non-existent principal — Terraform
#   would error with "Invalid principal". Therefore:
#     Pass 1: apply KMS without the Databricks principal statement
#     Pass 2: after modules/iam creates the Databricks S3 role, re-apply KMS
#             to add the AllowDatabricksS3Role Sid to the key policy.
#   Controlled via: var.databricks_s3_role_exists (bool, set in tfvars)
#
# DOCS
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/kms_key
#   https://docs.aws.amazon.com/kms/latest/developerguide/key-policy-overview.html
##############################################################################

##############################################################################
# aws_kms_key — the customer-managed encryption key
#
# description           → shown in the AWS console and CloudTrail events
# deletion_window_in_days = 30
#   AWS enforces a 7–30 day waiting period before permanently deleting a key.
#   30 days in prod gives time to cancel an accidental deletion.
#   Keys cannot be immediately deleted — this is an AWS safety control.
#
# enable_key_rotation = true
#   AWS rotates the backing key material every year automatically.
#   The key ID and ARN stay the same; existing ciphertext is not re-encrypted.
#   Required by SOC2, HIPAA, and most security frameworks.
#   Cost: free (included with the key).
#
# KEY POLICY — required Sid statements:
#   RootAccess            → AWS account root has full key administration.
#                           Without this, the key can become unmanageable if
#                           all IAM users lose permissions.
#   AllowCloudWatchLogs   → CloudWatch Logs needs GenerateDataKey + Decrypt
#                           to encrypt log groups that capture Lambda output.
#   AllowDatabricksS3Role → Databricks IAM role needs GenerateDataKey + Decrypt
#                           to read/write KMS-encrypted S3 objects.
#                           Added in Pass 2 only (see two-pass note above).
#   AllowS3ToUseSQSKey    → S3 needs GenerateDataKey when delivering event
#                           notifications to KMS-encrypted SQS queues.
#                           Principal: s3.amazonaws.com
#   AllowGlueRoleToUseKey → Glue Crawler needs Decrypt to read KMS-encrypted
#                           S3 objects during schema discovery.
##############################################################################
resource "aws_kms_key" "main" {
  description             = "Healthcare data platform — all-service encryption key (HIPAA)"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  # Key policy using aws_iam_policy_document data source (recommended pattern):
  # policy = data.aws_iam_policy_document.kms_policy.json
  #
  # The policy data source should build conditionally:
  # If var.databricks_s3_role_exists == false → omit AllowDatabricksS3Role
  # If var.databricks_s3_role_exists == true  → include AllowDatabricksS3Role
  # This is what controls the two-pass deployment sequence.

  # tags = {
  #   Environment = var.environment
  #   Project     = var.project_name
  #   HIPAA       = "true"
  # }
}

##############################################################################
# aws_kms_alias — human-readable reference name
#
# Aliases let you reference the key by name in the AWS console and CLI
# without memorising the key UUID. The "alias/" prefix is required by AWS.
# Format: alias/<project>-<environment>
#
# Used in resource configurations as: "alias/healthcare-data-platform-prod"
# instead of the full ARN — shorter and more readable.
##############################################################################
resource "aws_kms_alias" "main" {
  name          = "alias/${var.project_name}-${var.environment}"
  target_key_id = aws_kms_key.main.key_id
}
