CREATE OR REPLACE MODEL
  monitoring.telemetry_parse_payload_error_model_v1 OPTIONS( MODEL_TYPE = 'ARIMA',
    TIME_SERIES_TIMESTAMP_COL = 'submission_hour',
    TIME_SERIES_DATA_COL = 'error_count',
    TIME_SERIES_ID_COL = 'document_type',
    HORIZON = 168
    ) AS
SELECT
  submission_hour,
  document_type,
  AVG(error_count) OVER (
    PARTITION BY document_type
    ORDER BY UNIX_SECONDS(submission_hour) / 3600
    ASC RANGE BETWEEN 2 PRECEDING AND CURRENT ROW
  ) error_count
FROM
  (
    SELECT
    submission_hour,
    document_type,
    SUM(error_count) AS error_count,
  FROM
    monitoring.telemetry_error_counts_v1
  WHERE
    error_type = 'ParsePayload'
    AND DATE(submission_hour) BETWEEN DATE_SUB(@submission_date, INTERVAL 7 MONTH) AND @submission_date
  GROUP BY
    submission_hour,
    document_type
  HAVING
    error_count > 100
  )
ORDER BY
  submission_hour