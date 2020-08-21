CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring.telemetry_parse_payload_error_forecast_v1`(
    base_date DATE,
    forecast_timestamp TIMESTAMP,
    document_type STRING,
    forecast_value FLOAT64,
    standard_error FLOAT64,
    confidence_level FLOAT64,
    prediction_interval_lower_bound FLOAT64,
    prediction_interval_upper_bound FLOAT64,
    confidence_interval_lower_bound FLOAT64,
    confidence_interval_upper_bound FLOAT64,
  )
PARTITION BY
  base_date
OPTIONS
  (require_partition_filter = TRUE)
