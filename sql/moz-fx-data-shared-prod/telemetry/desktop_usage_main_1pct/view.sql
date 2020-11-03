/*
  Generated via:
    python -m bigquery_etl.usage
  To add additional measures, see bigquery_etl/usage/config
*/
CREATE OR REPLACE VIEW
  desktop_usage_main_1pct
AS
SELECT
  as_of_date - i AS submission_date,
  sample_id,
  client_id,
  as_of_date - `moz-fx-data-shared-prod`.udf.bits_to_days_since_first_seen(
    days_seen_bits
  ) AS first_seen_date,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(days_seen_bits >> i) AS days_since_seen,
  CAST(CONCAT('0x', TO_HEX(RIGHT(days_seen_bits >> i, 4))) AS INT64) << 36 >> 36 AS days_seen_bits,
  CAST(CONCAT('0x', TO_HEX(RIGHT(days_seen_bits >> i, 4))) AS INT64) AS days_seen_bits64,
  days_seen_bits >> i AS days_seen_bytes,
  as_of_date - `moz-fx-data-shared-prod`.udf.bits_to_days_since_first_seen(
    days_opened_dev_tools_bits
  ) AS first_opened_dev_tools_date,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(
    days_opened_dev_tools_bits >> i
  ) AS days_since_opened_dev_tools,
  CAST(
    CONCAT('0x', TO_HEX(RIGHT(days_opened_dev_tools_bits >> i, 4))) AS INT64
  ) << 36 >> 36 AS days_opened_dev_tools_bits,
  CAST(
    CONCAT('0x', TO_HEX(RIGHT(days_opened_dev_tools_bits >> i, 4))) AS INT64
  ) AS days_opened_dev_tools_bits64,
  days_opened_dev_tools_bits >> i AS days_opened_dev_tools_bytes,
  as_of_date - `moz-fx-data-shared-prod`.udf.bits_to_days_since_first_seen(
    days_visited_1_uri_bits
  ) AS first_visited_1_uri_date,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(
    days_visited_1_uri_bits >> i
  ) AS days_since_visited_1_uri,
  CAST(
    CONCAT('0x', TO_HEX(RIGHT(days_visited_1_uri_bits >> i, 4))) AS INT64
  ) << 36 >> 36 AS days_visited_1_uri_bits,
  CAST(
    CONCAT('0x', TO_HEX(RIGHT(days_visited_1_uri_bits >> i, 4))) AS INT64
  ) AS days_visited_1_uri_bits64,
  days_visited_1_uri_bits >> i AS days_visited_1_uri_bytes,
  as_of_date - `moz-fx-data-shared-prod`.udf.bits_to_days_since_first_seen(
    days_visited_5_uri_bits
  ) AS first_visited_5_uri_date,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(
    days_visited_5_uri_bits >> i
  ) AS days_since_visited_5_uri,
  CAST(
    CONCAT('0x', TO_HEX(RIGHT(days_visited_5_uri_bits >> i, 4))) AS INT64
  ) << 36 >> 36 AS days_visited_5_uri_bits,
  CAST(
    CONCAT('0x', TO_HEX(RIGHT(days_visited_5_uri_bits >> i, 4))) AS INT64
  ) AS days_visited_5_uri_bits64,
  days_visited_5_uri_bits >> i AS days_visited_5_uri_bytes,
  as_of_date - `moz-fx-data-shared-prod`.udf.bits_to_days_since_first_seen(
    days_visited_10_uri_bits
  ) AS first_visited_10_uri_date,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(
    days_visited_10_uri_bits >> i
  ) AS days_since_visited_10_uri,
  CAST(
    CONCAT('0x', TO_HEX(RIGHT(days_visited_10_uri_bits >> i, 4))) AS INT64
  ) << 36 >> 36 AS days_visited_10_uri_bits,
  CAST(
    CONCAT('0x', TO_HEX(RIGHT(days_visited_10_uri_bits >> i, 4))) AS INT64
  ) AS days_visited_10_uri_bits64,
  days_visited_10_uri_bits >> i AS days_visited_10_uri_bytes,
  as_of_date - `moz-fx-data-shared-prod`.udf.bits_to_days_since_first_seen(
    days_had_active_ticks_bits
  ) AS first_had_active_ticks_date,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(
    days_had_active_ticks_bits >> i
  ) AS days_since_had_active_ticks,
  CAST(
    CONCAT('0x', TO_HEX(RIGHT(days_had_active_ticks_bits >> i, 4))) AS INT64
  ) << 36 >> 36 AS days_had_active_ticks_bits,
  CAST(
    CONCAT('0x', TO_HEX(RIGHT(days_had_active_ticks_bits >> i, 4))) AS INT64
  ) AS days_had_active_ticks_bits64,
  days_had_active_ticks_bits >> i AS days_had_active_ticks_bytes,
  as_of_date - `moz-fx-data-shared-prod`.udf.bits_to_days_since_first_seen(
    days_had_8_active_ticks_bits
  ) AS first_had_8_active_ticks_date,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(
    days_had_8_active_ticks_bits >> i
  ) AS days_since_had_8_active_ticks,
  CAST(
    CONCAT('0x', TO_HEX(RIGHT(days_had_8_active_ticks_bits >> i, 4))) AS INT64
  ) << 36 >> 36 AS days_had_8_active_ticks_bits,
  CAST(
    CONCAT('0x', TO_HEX(RIGHT(days_had_8_active_ticks_bits >> i, 4))) AS INT64
  ) AS days_had_8_active_ticks_bits64,
  days_had_8_active_ticks_bits >> i AS days_had_8_active_ticks_bytes,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.desktop_usage_main_1pct_v1`
-- The cross join parses each input row into one row per day since the client
-- was first seen, emulating the format of the existing clients_last_seen table.
CROSS JOIN
  UNNEST(GENERATE_ARRAY(0, 2048)) AS i