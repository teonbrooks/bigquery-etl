CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring.telemetry_error_counts_v1`(
    submission_hour TIMESTAMP,
    document_type STRING,
    document_version STRING,
    error_type STRING,
    exception_class STRING,
    ping_count INT64,
    error_count INT64,
    error_ratio FLOAT64
  )
PARTITION BY
  DATE(submission_hour)
OPTIONS(
  require_partition_filter=true
)
