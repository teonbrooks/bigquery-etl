CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring.telemetry_parse_payload_error_forecast_v1`(
    submission_hour TIMESTAMP,
    forecast_timestamp TIMESTAMP,
    document_type STRING,
    forecast_value STRING,
    standard_error STRING,
    confidence_level STRING,
    prediction_interval_lower_bound STRING,
    prediction_interval_upper_bound STRING,
    confidence_interval_lower_bound STRING,
    confidence_interval_upper_bound STRING,
  )
PARTITION BY
  DATE(submission_hour)
OPTIONS(
  require_partition_filter=true
)
