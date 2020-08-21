SELECT
  @submission_date AS base_date,
  *,
FROM
  ML.FORECAST(
    MODEL monitoring.telemetry_parse_payload_error_model_v1,
    STRUCT<horizon INT64, confidence_level FLOAT64>(24, 0.99)
  )
