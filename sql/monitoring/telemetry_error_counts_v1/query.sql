WITH errors AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
    document_type,
    document_version,
    error_type,
    exception_class,
    COUNT(*) AS error_count,
  FROM
    `moz-fx-data-shared-prod.payload_bytes_error.telemetry`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_hour,
    document_type,
    document_version,
    error_type,
    exception_class
  ORDER BY
    submission_hour
),
stable AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS submission_hour,
    SUBSTR(_TABLE_SUFFIX, 0, LENGTH(_TABLE_SUFFIX) - 3) AS document_type,
    SUBSTR(_TABLE_SUFFIX, -1, 1) AS document_version,
    COUNT(*) AS ping_count,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.*`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_hour,
    document_type,
    document_version
),
combined AS (
  SELECT
    submission_hour,
    document_type,
    document_version,
    error_type,
    exception_class,
    COALESCE(ping_count, 0) + COALESCE(error_count, 0) AS ping_count,
    COALESCE(error_count, 0) AS error_count,
  FROM
    errors
  FULL JOIN
    stable
  USING
    (submission_hour, document_type, document_version)
)

SELECT
  *,
  SAFE_DIVIDE(error_count, ping_count) AS error_ratio,
FROM
  combined
ORDER BY
  submission_hour,
  error_type,
  exception_class
