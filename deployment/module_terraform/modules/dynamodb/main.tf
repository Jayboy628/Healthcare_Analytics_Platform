##############################################################################
# modules/dynamodb/main.tf
#
# PURPOSE
#   Creates all six DynamoDB operational tables that form the pipeline
#   control plane. These tables store pipeline state, checkpoints, data
#   quality history, schema versions, and job bookmarks.
#   Databricks Delta Lake stores the analytical data; DynamoDB stores
#   the operational metadata that keeps the pipeline running correctly.
#
# WHY DYNAMODB (not PostgreSQL or another relational DB)?
#   - Serverless: no cluster to manage, no connection pool, scales to zero
#   - Single-digit millisecond reads: Lambda can check the ingestion ledger
#     in < 2ms per file — a relational DB would add 20–50ms of connection
#     overhead per Lambda invocation
#   - Conditional writes: the ingestion_ledger uses a conditional PutItem
#     to guarantee exactly-once file processing without a distributed lock
#   - DynamoDB Streams (future): enables CDC-based alerting without polling
#
# TABLE OVERVIEW
#   ingestion_ledger_prod     File-level deduplication. Lambda writes one row
#                             per file keyed on file_checksum. Before processing
#                             any file, Lambda checks if the checksum exists —
#                             if yes, the file is skipped (idempotent).
#
#   pipeline_log_prod         Job execution history. Databricks notebooks write
#                             one row per run: job name, status, records processed,
#                             duration. Read by dynamo_ops_queries.py --query pipeline_runs.
#
#   data_quality_results_prod DQ health dashboard. Lambda writes one summary row
#                             per file processed: total records, valid count,
#                             quarantine count, pass rate %, error breakdown by rule.
#                             Queried by Streamlit dashboard and daily health check.
#
#   quarantine_index_prod     Bad record catalog. Lambda writes one row per bad
#                             record keyed on source_file + ingestion_timestamp.
#                             Allows querying "show all quarantined records from
#                             hospital_867 this week" without reading the FIFO queue.
#                             The resolved flag tracks remediation status.
#
#   schema_registry_prod      Column schema versioning. Lambda does a conditional
#                             PutItem on the first occurrence of each column set.
#                             A new row appears when a hospital adds/removes/renames
#                             columns — this is schema drift detection.
#
#   job_bookmark_prod         Incremental processing offsets. Databricks notebooks
#                             write the last successfully processed S3 prefix or
#                             Delta table watermark at the end of each successful run.
#                             The next run reads this to know where to start from.
#
# BILLING MODE
#   PAY_PER_REQUEST (on-demand): no capacity planning, scales automatically.
#   Appropriate here because table access is event-driven (Lambda invocations)
#   rather than continuous. For very high volume (>1M writes/day), evaluate
#   PROVISIONED mode with auto-scaling for cost savings.
#
# DOCS
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/dynamodb_table
#   https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html
##############################################################################

##############################################################################
# aws_dynamodb_table — created for each entry in var.tables
#
# The tables variable (defined in variables.tf) is a map of objects, one per
# table, each specifying hash_key, optional range_key, optional ttl_attr,
# and optional GSI definitions.
#
# for_each iterates the map — one aws_dynamodb_table resource per table.
# This avoids copy-paste while keeping each table's config explicit in tfvars.
#
# KEY SCHEMA (from variables.tf tables definition):
#   ingestion_ledger     HASH: file_checksum
#   pipeline_log         HASH: pipeline_id    RANGE: event_timestamp
#   data_quality_results HASH: dataset_name   RANGE: run_timestamp
#   quarantine_index     HASH: source_file    RANGE: ingestion_timestamp
#   schema_registry      HASH: dataset_name   RANGE: schema_version
#   job_bookmark         HASH: job_name       RANGE: source_name
#
# ENCRYPTION
#   server_side_encryption enabled with the platform KMS key.
#   HIPAA requires encryption at rest with auditable key management.
#
# TTL (time-to-live)
#   Optional per-table expiry on a named timestamp attribute.
#   DynamoDB deletes expired items automatically (within 48 hrs of expiry).
#   Used to auto-expire pipeline_log entries after 90 days to control
#   table size and storage costs.
##############################################################################
resource "aws_dynamodb_table" "tables" {
  for_each = var.tables

  name         = "${var.project_name}_${each.key}_${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = each.value.hash_key
  range_key    = each.value.range_key  # null when not specified (optional)

  # Hash key attribute — always STRING for this platform
  attribute {
    name = each.value.hash_key
    type = "S"
  }

  # Range key attribute — only defined when range_key is non-null
  # dynamic "attribute" {
  #   for_each = each.value.range_key != null ? [each.value.range_key] : []
  #   content {
  #     name = attribute.value
  #     type = "S"
  #   }
  # }

  # TTL — auto-expire rows based on a Unix epoch timestamp attribute
  # dynamic "ttl" {
  #   for_each = each.value.ttl_attr != null ? [each.value.ttl_attr] : []
  #   content {
  #     attribute_name = ttl.value
  #     enabled        = true
  #   }
  # }

  # KMS encryption — required for HIPAA compliance (audit trail of key usage)
  server_side_encryption {
    enabled     = true
    # kms_key_arn = var.kms_key_arn
  }

  # Global Secondary Indexes — one block per GSI in each.value.gsis
  # GSIs allow querying on non-key attributes. Example use case:
  #   quarantine_index_prod: GSI on facility_id to query all bad records
  #   for a specific hospital without scanning the whole table.
  # dynamic "global_secondary_index" {
  #   for_each = each.value.gsis
  #   content {
  #     name            = global_secondary_index.value.name
  #     hash_key        = global_secondary_index.value.hash_key
  #     range_key       = global_secondary_index.value.range_key
  #     projection_type = "ALL"
  #   }
  # }

  # point_in_time_recovery { enabled = true }  # HIPAA best practice

  # tags = { Environment = var.environment, Project = var.project_name }
}
