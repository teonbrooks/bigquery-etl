{{ header }}
{% include "scalar_bucket_counts_v1.udf.sql" %}
{% from 'macros.sql' import enumerate_table_combinations %}

{# TODO: remove this import by factoring it out as a proper udf #}
{% include "clients_scalar_aggregates_v1.udf.sql" %}

WITH
{{
    enumerate_table_combinations(
        source_table,
        "all_combos",
        cubed_attributes,
        attribute_combinations
    )
}},
-- Ensure there is a single record per client id
deduplicated_combos AS (
  SELECT
    client_id,
    {{ attributes }},
    udf_merged_user_data(
      ARRAY_CONCAT_AGG(scalar_aggregates)
    ) AS scalar_aggregates
  FROM
    all_combos
  GROUP BY
    client_id,
    {{ attributes }}
),
bucketed_booleans AS (
  SELECT
    client_id,
    {{ attributes }},
    NULL AS range_min,
    NULL AS range_max,
    NULL AS bucket_count,
    udf_boolean_buckets(scalar_aggregates) AS scalar_aggregates,
  FROM
    deduplicated_combos
),
log_min_max AS (
  SELECT
    metric,
    key,
    LOG(IF(MIN(value) <= 0, 1, MIN(value)), 2) range_min,
    LOG(IF(MAX(value) <= 0, 1, MAX(value)), 2) range_max,
    100 as bucket_count
  FROM
    deduplicated_combos
    CROSS JOIN UNNEST(scalar_aggregates)
  WHERE
    metric_type <> "boolean"
  GROUP BY 1, 2
),
buckets_by_metric AS (
  SELECT
    *,
    ARRAY(SELECT FORMAT("%.*f", 2, bucket) FROM UNNEST(
      mozfun.glam.histogram_generate_scalar_buckets(range_min, range_max, bucket_count)
      ) as bucket
    ) AS buckets
  FROM log_min_max
),
bucketed_scalars AS (
  SELECT
    client_id,
    {{ attributes }},
    {{ aggregate_attributes }},
    agg_type,
    range_min,
    range_max,
    bucket_count,
    -- Keep two decimal places before converting bucket to a string
    SAFE_CAST(
      FORMAT("%.*f", 2, mozfun.glam.histogram_bucket_from_value(buckets, value) + 0.0001)
      AS STRING) AS bucket
  FROM
    deduplicated_combos
  CROSS JOIN UNNEST(scalar_aggregates)
  LEFT JOIN buckets_by_metric
    USING(metric, key)
  WHERE
    metric_type in ({{ scalar_metric_types }})
),
booleans_and_scalars AS (
  SELECT
    client_id,
    {{ attributes }},
    {{ aggregate_attributes }},
    agg_type,
    range_min,
    range_max,
    bucket_count,
    bucket
  FROM
    bucketed_booleans
  CROSS JOIN
    UNNEST(scalar_aggregates)
  UNION ALL
  SELECT
    client_id,
    {{ attributes }},
    {{ aggregate_attributes }},
    agg_type,
    range_min,
    range_max,
    bucket_count,
    bucket
  FROM
    bucketed_scalars
)
SELECT
  {{ attributes }},
  {{ aggregate_attributes }},
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  range_min,
  range_max,
  bucket_count,
  bucket,
  COUNT(*) AS count
FROM
  booleans_and_scalars
GROUP BY
  {{ attributes }},
  {{ aggregate_attributes }},
  client_agg_type,
  range_min,
  range_max,
  bucket_count,
  bucket
