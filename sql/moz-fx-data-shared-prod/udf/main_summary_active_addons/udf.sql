CREATE OR REPLACE FUNCTION udf.main_summary_active_addons(
  active_addons ARRAY<
    STRUCT<
      key STRING,
      value STRUCT<
        app_disabled BOOL,
        blocklisted BOOL,
        description STRING,
        has_binary_components BOOL,
        install_day INT64,
        is_system BOOL,
        name STRING,
        scope INT64,
        signed_state INT64,
        type STRING,
        update_day INT64,
        is_web_extension BOOL,
        multiprocess_compatible BOOL,
        foreign_install INT64,
        user_disabled INT64,
        version STRING
      >
    >
  >
) AS (
  ARRAY(
    SELECT AS STRUCT
      key AS addon_id,
      value.blocklisted,
      value.name,
      value.user_disabled > 0 AS user_disabled,
      value.app_disabled,
      value.version,
      value.scope,
      value.type,
      value.foreign_install > 0 AS foreign_install,
      value.has_binary_components,
      value.install_day,
      value.update_day,
      value.signed_state,
      value.is_system,
      value.is_web_extension,
      value.multiprocess_compatible
    FROM
      UNNEST(active_addons)
  )
);

-- Tests
WITH result AS (
  SELECT AS VALUE
    ARRAY_CONCAT(
      udf.main_summary_active_addons(
        [
          -- truthy fields
          (
            'a',
            (
              TRUE,
              TRUE,
              'description',
              TRUE,
              2,
              TRUE,
              'name',
              1,
              4,
              'type',
              3,
              TRUE,
              TRUE,
              1,
              1,
              "version"
            )
          ),
          -- falsey fields
          ('b', (FALSE, FALSE, '', FALSE, 0, FALSE, '', 0, 0, '', 0, FALSE, FALSE, 0, 0, "")),
          -- null value
          ('c', NULL)
        ]
      ),
      udf.main_summary_active_addons(NULL)
    )
)
SELECT
  assert.equals(3, ARRAY_LENGTH(result)),
  assert.equals(
    ('a', TRUE, 'name', TRUE, TRUE, 'version', 1, 'type', TRUE, TRUE, 2, 3, 4, TRUE, TRUE, TRUE),
    result[OFFSET(0)]
  ),
  assert.equals(
    ('b', FALSE, '', FALSE, FALSE, '', 0, '', FALSE, FALSE, 0, 0, 0, FALSE, FALSE, FALSE),
    result[OFFSET(1)]
  ),
  assert.equals('c', result[OFFSET(2)].addon_id),
  assert.all_fields_null((SELECT AS STRUCT result[OFFSET(2)].* EXCEPT (addon_id))),
FROM
  result
