# =============================================================================
# databricks_load/pipelines/silver/02_silver_adt_streaming.py
#
# PURPOSE
#   Reads Bronze ADT events from bronze.adt_events_raw and writes standardised
#   records to healthcare_catalog.silver.adt_events_standardized.
#   Computes census_delta per event for use in gold.fact_census_realtime.
#
# DESIGN PATTERN: DELTA READSTREAM (not Kinesis readStream)
#   Reads the Bronze Delta TABLE as a stream, not the Kinesis stream directly.
#   Does NOT use readChangeFeed=true because CDF was not enabled on the Bronze
#   ADT table when it was originally created. Regular Delta streaming reads
#   all records on first run, then only new appends via the checkpoint.
#
# TASK POSITION IN RT STREAMING JOB: Task 2 of 2
#
# UTILITIES LOADED
#   %run /Workspace/Shared/healthcare/common/config/pipeline_config
#
# STEP-BY-STEP
#
#   1. Check Bronze ADT table exists and has data.
#      Exit with NO_BRONZE_DATA if empty — avoids misleading empty stream error.
#
#   2. Enable CDF on Bronze ADT (idempotent):
#      ALTER TABLE bronze.adt_events_raw
#      SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
#      Safe to call every run. Applies to future writes only.
#
#   3. Read Bronze as Delta stream:
#      spark.readStream.format("delta")
#      .option("ignoreDeletes", "true")   — stream survives VACUUM operations
#      .option("ignoreChanges", "true")   — stream survives UPDATE operations
#      .table(SOURCE_TABLE)
#
#   4. Standardise each ADT event:
#      event_ts → parse with multiple timestamp formats (coalesce try_to_timestamp)
#      event_date → to_date(event_ts)
#      event_hour → hour(event_ts)
#      event_type_std → normalise raw codes:
#        A / ADMIT / ADMISSION → "ADMIT"
#        D / DISCHARGE / DISCH → "DISCHARGE"
#        T / TRANSFER / TRANS  → "TRANSFER"
#      event_id → SHA256(facility_id|patient_id|event_timestamp|event_type)
#        Unique event identifier for idempotent downstream MERGE operations
#      census_delta:
#        ADMIT    → +1  (one more patient in the unit)
#        DISCHARGE → -1 (one fewer patient)
#        TRANSFER  →  0 (moves between units — net zero within facility)
#        Accumulated in Gold via running SUM OVER (PARTITION BY facility, unit)
#        to compute real-time census per unit per hour.
#
#   5. Write Silver ADT stream:
#      writeStream.format("delta").outputMode("append")
#      .option("checkpointLocation", CHECKPOINTS["silver_streaming"])
#      .option("mergeSchema", "true")
#      .option("path", TARGET_PATH)
#      .trigger(availableNow=True)
#      .start() + query.awaitTermination()
#
#   6. Register in Unity Catalog:
#      CREATE TABLE IF NOT EXISTS healthcare_catalog.silver.adt_events_standardized
#      USING DELTA LOCATION 's3://hc-data-lake-prod/silver/adt_events_standardized/'
#
# =============================================================================
