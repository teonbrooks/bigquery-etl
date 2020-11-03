/*
  Generated via:
    python -m bigquery_etl.usage
  To add additional measures, see bigquery_etl/usage/config
*/
CREATE OR REPLACE VIEW
  desktop_usage_{{ name }}_1pct
AS
SELECT
  as_of_date - i AS submission_date,
  sample_id,
  client_id,
{% for measure in measures %}{% for usage in measure.usages %}
  as_of_date - `moz-fx-data-shared-prod`.udf.bits_to_days_since_first_seen(days_{{ usage.name }}_bits) AS first_{{ usage.name }}_date,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(days_{{ usage.name }}_bits >> i) AS days_since_{{ usage.name }},
  CAST(CONCAT('0x', TO_HEX(RIGHT(days_{{ usage.name }}_bits >> i, 4))) AS INT64) << 36 >> 36 AS days_{{ usage.name }}_bits,
  CAST(CONCAT('0x', TO_HEX(RIGHT(days_{{ usage.name }}_bits >> i, 4))) AS INT64) AS days_{{ usage.name }}_bits64,
  days_{{ usage.name }}_bits >> i AS days_{{ usage.name }}_bytes,
  {% endfor %}{% endfor %}
FROM
  `moz-fx-data-shared-prod.telemetry_derived.desktop_usage_{{ name }}_1pct_v1`
-- The cross join parses each input row into one row per day since the client
-- was first seen, emulating the format of the existing clients_last_seen table.
CROSS JOIN
  UNNEST(GENERATE_ARRAY(0, 2048)) AS i
