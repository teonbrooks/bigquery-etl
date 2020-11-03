/*
  Generated via:
    python -m bigquery_etl.usage
  To add additional measures, see bigquery_etl/usage/config
*/
CREATE TEMP FUNCTION offsets_to_bytes(offsets ARRAY<INT64>) AS (
  (
    SELECT
      LTRIM(
        STRING_AGG(
          (
            SELECT
              -- CODE_POINTS_TO_BYTES is the best available interface for converting
              -- an array of numeric values to a BYTES field; it requires that values
              -- are valid extended ASCII characters, so we can only aggregate 8 bits
              -- at a time, and then append all those chunks together with STRING_AGG.
              CODE_POINTS_TO_BYTES(
                [
                  BIT_OR(
                    (
                      IF(
                        DIV(n - (8 * i), 8) = 0
                        AND (n - (8 * i)) >= 0,
                        1 << MOD(n - (8 * i), 8),
                        0
                      )
                    )
                  )
                ]
              )
            FROM
              UNNEST(offsets) AS n
          ),
          b''
        ),
        b'\x00'
      )
    FROM
      -- Each iteration handles 8 bits, so 256 iterations gives us
      -- 2048 bits, about 5.6 years worth.
      UNNEST(GENERATE_ARRAY(255, 0, -1)) AS i
  )
);

CREATE OR REPLACE TABLE
  desktop_usage_main_1pct_v1
AS
WITH daily AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    sample_id,
    client_id,
    COUNT(*) AS total_pings,
    SUM(
      udf.extract_histogram_sum(payload.histograms.devtools_toolbox_opened_count)
    ) AS devtools_toolbox_opened_count_sum,
    SUM(payload.processes.parent.scalars.browser_engagement_total_uri_count) AS total_uri_count_sum,
    SUM(
      COALESCE(
        payload.processes.parent.scalars.browser_engagement_active_ticks,
        payload.simple_measurements.active_ticks
      )
    ) AS active_ticks_sum,
  FROM
    telemetry.main
  WHERE
    sample_id = 0
    AND DATE(submission_timestamp)
    BETWEEN '2018-11-01'
    AND @submission_date
  GROUP BY
    submission_date,
    sample_id,
    client_id
)
SELECT
  @submission_date AS as_of_date,
  sample_id,
  client_id,
  offsets_to_bytes(
    ARRAY_AGG(
      IF(total_pings >= 1, DATE_DIFF(@submission_date, submission_date, DAY), NULL) IGNORE NULLS
    )
  ) AS days_seen_bits,
  offsets_to_bytes(
    ARRAY_AGG(
      IF(
        devtools_toolbox_opened_count_sum >= 1,
        DATE_DIFF(@submission_date, submission_date, DAY),
        NULL
      ) IGNORE NULLS
    )
  ) AS days_opened_dev_tools_bits,
  offsets_to_bytes(
    ARRAY_AGG(
      IF(
        total_uri_count_sum >= 1,
        DATE_DIFF(@submission_date, submission_date, DAY),
        NULL
      ) IGNORE NULLS
    )
  ) AS days_visited_1_uri_bits,
  offsets_to_bytes(
    ARRAY_AGG(
      IF(
        total_uri_count_sum >= 5,
        DATE_DIFF(@submission_date, submission_date, DAY),
        NULL
      ) IGNORE NULLS
    )
  ) AS days_visited_5_uri_bits,
  offsets_to_bytes(
    ARRAY_AGG(
      IF(
        total_uri_count_sum >= 10,
        DATE_DIFF(@submission_date, submission_date, DAY),
        NULL
      ) IGNORE NULLS
    )
  ) AS days_visited_10_uri_bits,
  offsets_to_bytes(
    ARRAY_AGG(
      IF(
        active_ticks_sum >= 1,
        DATE_DIFF(@submission_date, submission_date, DAY),
        NULL
      ) IGNORE NULLS
    )
  ) AS days_had_active_ticks_bits,
  offsets_to_bytes(
    ARRAY_AGG(
      IF(
        active_ticks_sum >= 8,
        DATE_DIFF(@submission_date, submission_date, DAY),
        NULL
      ) IGNORE NULLS
    )
  ) AS days_had_8_active_ticks_bits,
FROM
  daily
GROUP BY
  sample_id,
  client_id
