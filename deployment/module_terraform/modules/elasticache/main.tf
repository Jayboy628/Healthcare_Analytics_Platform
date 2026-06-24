##############################################################################
# modules/elasticache/main.tf
#
# PURPOSE
#   Creates an Amazon ElastiCache Redis cluster for low-latency KPI serving.
#   Databricks Gold Delta tables are the system of record — Redis is a
#   read-through cache that pre-stores computed aggregates for sub-second
#   dashboard response times.
#
# WHY REDIS?
#   Databricks SQL Warehouse queries against Gold tables typically return in
#   2–10 seconds depending on cluster warmup. The Streamlit dashboard queries
#   the same KPIs on every page load for every user. Redis caches those
#   results and returns them in < 5ms — dramatically better user experience
#   and lower Databricks SQL compute cost.
#
# WHAT IS CACHED
#   facility_daily_summary   15-min TTL  Invalidated by SNS batch_complete
#   np_ratio_realtime         5-min TTL  Written by Lambda after Kinesis batches
#   ot_summary_monthly       30-min TTL  Invalidated after ETL job completes
#   scorecard_quarterly       1-hr TTL   Invalidated after ETL job completes
#   dq_dashboard             10-min TTL  Invalidated by any quarantine SQS event
#   dim_facility              1-hr TTL   Invalidated by SCD2 Gold dim update
#
# NETWORK REQUIREMENT
#   ElastiCache clusters MUST be in a VPC — they cannot use public endpoints.
#   Subnet group spans all private subnets (one per AZ) for HA.
#   Lambda functions that write to Redis must also be in the VPC.
#
# NODE TYPE
#   cache.r7g.large = 12.3 GB RAM, memory-optimized, Graviton3.
#   Scale up to cache.r7g.xlarge if CloudWatch EngineCPUUtilization > 70%
#   or DatabaseMemoryUsagePercentage > 80%.
#
# DOCS
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/elasticache_replication_group
#   https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/elasticache_subnet_group
#   https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/best-practices.html
##############################################################################

##############################################################################
# aws_elasticache_subnet_group — places Redis in private subnets
#
# A subnet group maps the Redis cluster to specific subnets in the VPC.
# Use private subnets (not public) — Redis should never be internet-reachable.
# Spanning multiple AZs allows the replica to be in a different AZ than the
# primary, providing automatic failover if one AZ goes down.
##############################################################################
resource "aws_elasticache_subnet_group" "redis" {
  name        = "${var.project_name}-redis-subnet-${var.environment}"
  description = "Private subnets for Redis KPI cache"
  # subnet_ids  = var.private_subnet_ids  ← from modules/network output
}

##############################################################################
# aws_elasticache_replication_group — Redis cluster
#
# replication_group_id  → unique identifier in the AWS console
# description           → human-readable purpose
#
# node_type = "cache.r7g.large"
#   Memory-optimized instance. Redis is CPU-light and memory-heavy.
#   r7g (Graviton3) is the latest generation — better price/performance
#   than r6g or older families.
#
# num_cache_clusters = 2
#   One primary + one replica. The replica enables:
#     - Automatic failover if the primary fails (Multi-AZ)
#     - Read offload (read from replica for high-read-volume use cases)
#   In dev, set to 1 (primary only) to save cost.
#
# automatic_failover_enabled = true
#   Requires num_cache_clusters >= 2. On primary failure, ElastiCache
#   promotes the replica to primary automatically — typically < 60 seconds.
#
# multi_az_enabled = true
#   Places primary and replica in different AZs for physical isolation.
#   Requires automatic_failover_enabled = true.
#
# at_rest_encryption_enabled = true (HIPAA requirement)
#   Encrypts data stored on disk when Redis evicts pages to swap.
#   Uses the platform KMS key.
#
# transit_encryption_enabled = true (HIPAA requirement)
#   Encrypts all network traffic between Lambda/Streamlit and Redis using TLS.
#   Requires clients to connect with TLS (append ssl=True to connection string).
#
# auth_token
#   Redis AUTH token — a password clients must supply on connect.
#   Retrieved from AWS Secrets Manager at runtime by Lambda and Streamlit.
#   Store in Secrets Manager; pass ARN via var.redis_auth_token_secret_arn.
##############################################################################
resource "aws_elasticache_replication_group" "redis" {
  replication_group_id = "${var.project_name}-redis-${var.environment}"
  description          = "KPI cache for Streamlit dashboard — populated by ETL pipeline"

  node_type            = var.node_type  # "cache.r7g.large"
  num_cache_clusters   = var.environment == "prod" ? 2 : 1
  port                 = 6379

  subnet_group_name  = aws_elasticache_subnet_group.redis.name
  # security_group_ids = [var.redis_security_group_id]  ← from modules/network output

  automatic_failover_enabled = var.environment == "prod"
  multi_az_enabled           = var.environment == "prod"

  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  # kms_key_id                 = var.kms_key_arn
  # auth_token                 = var.redis_auth_token  # from Secrets Manager

  engine_version = "7.1"

  # tags = { Environment = var.environment, Project = var.project_name }
}
