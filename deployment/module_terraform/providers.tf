##############################################################################
# module_terraform/providers.tf
#
# PURPOSE
#   Declares the minimum Terraform CLI version and all provider plugins.
#   Modules inherit these — they do not re-declare providers individually
#   (except modules/databricks/versions.tf which pins the Databricks plugin).
#
# PROVIDERS IN USE
#   hashicorp/aws         All AWS resources: S3, Lambda, SQS, SNS, Kinesis,
#                         DynamoDB, ElastiCache, Glue, KMS, IAM, VPC.
#   hashicorp/archive     Packages Python Lambda source dirs into .zip files
#                         at plan time. Used by modules/lambda and
#                         modules/lambda_trigger.
#   databricks/databricks Unity Catalog, storage credentials, external
#                         locations, schemas, SQL Warehouses.
#
# AUTHENTICATION
#   AWS        → named CLI profile "de_jay_east". Set via:
#                  aws configure --profile de_jay_east
#   Databricks → host + token loaded at runtime from AWS Secrets Manager:
#                  source scripts/load_databricks_secrets.sh
#                exports TF_VAR_databricks_host + TF_VAR_databricks_token
#
# ⚠️  Never hardcode credentials in any .tf file.
#
# DOCS
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs
#   https://registry.terraform.io/providers/databricks/databricks/latest/docs
##############################################################################

terraform {
  # Minimum Terraform CLI version.
  # >= 1.6.0 enables optional() in variable type constraints
  # (required by the dynamodb module tables variable).
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      # Lower bound = version validated during initial build.
      # Upper bound = guard against auto-upgrading to a breaking major version.
      version = ">= 6.32.0, < 7.0.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.60"
    }
  }
}

# AWS provider — region and named profile only, no embedded credentials.
provider "aws" {
  region  = var.aws_region  # declared in variables.tf, set in terraform.tfvars
  profile = "de_jay_east"
}

# Databricks provider — host and token arrive as TF_VAR_* env vars at runtime.
provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# Provides the numeric AWS account ID without hardcoding it.
# Referenced throughout modules as: data.aws_caller_identity.current.account_id
data "aws_caller_identity" "current" {}
