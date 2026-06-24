# =============================================================================
# databricks_load/pipelines/bronze/01_bronze_adt_streaming.py
#
# PURPOSE
#   Reads ADT (Admit/Discharge/Transfer) events from the Kinesis real-time
#   stream and writes them to the Bronze Delta table
#   (healthcare_catalog.bronze.adt_events_raw).
#
# DESIGN PATTERN: REAL-TIME → BATCH MICRO
#   Uses Structured Streaming with availableNow trigger — reads all records
#   that arrived in Kinesis since the last checkpoint, processes them as a
#   micro-batch, then stops. Run every 5 minutes by the RT Streaming job
#   (560379522229937) to approximate near-real-time processing.
#
# TASK POSITION IN RT STREAMING JOB
#   Task 1 of 2 in Healthcare RT Streaming Pipeline.
#   02_silver_adt_streaming depends on this completing successfully.
#
# UTILITIES LOADED
#   %run /Workspace/Shared/healthcare/common/config/pipeline_config
#   %run /Workspace/Shared/healthcare/common/utils/dynamo_utils
#
# WHAT THE NOTEBOOK DOES
#
#   Step 1 — Load AWS credentials
#     dbutils.secrets.get(scope="aws", key="access_key")
#     dbutils.secrets.get(scope="aws", key="secret_key")
#     Optional: session_token for temporary credentials.
#     Credentials are passed directly to the Kinesis connector options
#     (awsAccessKey, awsSecretKey) because Databricks Serverless does not
#     have an EC2 instance profile to inherit credentials from.
#
#   Step 2 — Define ADT event schema
#     StructType with fields: event_type, facility_id, unit_id, patient_id,
#     event_timestamp, bed_id, attending_role.
#     Passed to from_json() for strict parsing of the decoded payload.
#
#   Step 3 — Read from Kinesis stream
#     format("kinesis")
#     streamName → KINESIS["stream_name"]
#     region     → KINESIS["region"]
#     initialPosition = "trim_horizon"
#       On the FIRST run (no checkpoint), reads from the beginning of the
#       stream (within the 7-day retention window).
#       On SUBSEQUENT runs, the checkpoint overrides this — picks up from
#       where the last run left off (sequence number per shard).
#     awsAccessKey / awsSecretKey → from Databricks secret scope
#
#   Step 4 — Decode Kinesis records
#     Kinesis delivers records as base64-encoded binary data.
#     The raw "data" column is a binary column in Spark.
#
#     IMPORTANT FIX: Use TRY_TO_BINARY(data_str, 'BASE64') via F.expr()
#     instead of base64.b64decode() or unbase64(). This avoids padding
#     errors ("Incorrect padding") that occur when Kinesis includes partial
#     base64 blocks. TRY_TO_BINARY returns NULL on decode failure instead
#     of raising an exception — allowing bad records to be filtered out
#     cleanly rather than crashing the entire streaming query.
#
#     After decode → cast to STRING → parse with from_json(adt_schema)
#     from_json mode = "PERMISSIVE": records with missing fields become
#     NULL for those fields rather than failing entirely.
#
#   Step 5 — Select and transform columns
#     event_type, facility_id, unit_id → from JSON payload
#     patient_id → SHA256 hash (HIPAA — de-identify before storing)
#     event_timestamp, bed_id, attending_role → from JSON payload
#     _kinesis_partition  → partitionKey (identifies source shard)
#     _kinesis_sequence   → sequenceNumber (unique per record in shard)
#     _kinesis_timestamp  → approximateArrivalTimestamp (when Kinesis received it)
#     _processed_at       → current_timestamp() (when Databricks processed it)
#     Filter: event_type IS NOT NULL (drops records that failed JSON parsing)
#
#   Step 6 — Write to Bronze Delta
#     format("delta")
#     outputMode("append")
#     checkpointLocation → CHECKPOINTS["adt_events"]
#     path → s3://hc-data-lake-prod/bronze/realtime/
#     trigger(availableNow=True)
#     query.awaitTermination()
#
#   Step 7 — Register in Unity Catalog
#     CREATE TABLE IF NOT EXISTS healthcare_catalog.bronze.adt_events_raw
#     USING DELTA LOCATION 's3://hc-data-lake-prod/bronze/realtime/'
#
# KNOWN ISSUE: CHECKPOINT CORRUPTION
#   If the streaming job fails mid-write, the checkpoint may become inconsistent.
#   Symptom: job fails with "Stream source checkpoint is not consistent" or
#   "CONVERSION_INVALID_INPUT" errors on restart.
#   Fix: delete the checkpoint path and restart (causes full reprocessing
#   of all Kinesis records within the 7-day retention window).
#     aws s3 rm s3://hc-data-lake-prod/checkpoints/streaming/adt_events --recursive
#
# =============================================================================
