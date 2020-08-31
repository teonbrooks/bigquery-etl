CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.kpi_mau_forecasts` AS (
    WITH forecast_base AS (
      SELECT
        CASE
        WHEN
          datasource = "Mobile Global MAU"
        THEN
          "nondesktop_global"
        WHEN
          datasource = "Mobile Tier1 MAU"
        THEN
          "nondesktop_tier1"
        WHEN
          datasource = "Desktop Global MAU"
        THEN
          "desktop_global"
        WHEN
          datasource = "Desktop Tier1 MAU"
        THEN
          "desktop_tier1"
        ELSE
          datasource
        END
        AS datasource,
        date,
        type,
        value AS mau,
        low90,
        high90,
        p10 AS low80,
        p90 AS high80,
        p20,
        p30,
        p40,
        p50,
        p60,
        p70,
        p80
      FROM
        `moz-fx-data-shared-prod.telemetry.simpleprophet_forecasts`
      WHERE
        datasource IN (
          'Desktop Global MAU',
          'Desktop Tier1 MAU',
          'Mobile Global MAU',
          'Mobile Tier1 MAU'
        )
        AND asofdate = (
          SELECT
            max(asofdate)
          FROM
            `moz-fx-data-shared-prod.telemetry.simpleprophet_forecasts`
        )
    ),
    --
    --
    desktop_base AS (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod.telemetry.firefox_desktop_exact_mau28_by_dimensions`
    ),
    --
    --
    nondesktop_base AS (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod.telemetry.firefox_nondesktop_exact_mau28_by_dimensions`
      WHERE
        product != 'FirefoxForFireTV'
    ),
    --
    --
    per_bucket AS (
      SELECT
        'desktop_global' AS datasource,
        'actual' AS type,
        submission_date,
        id_bucket,
        SUM(mau) AS mau
      FROM
        desktop_base
      GROUP BY
        id_bucket,
        submission_date
      UNION ALL
      SELECT
        'desktop_tier1' AS datasource,
        'actual' AS type,
        submission_date AS date,
        id_bucket,
        SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), mau, 0)) AS mau
      FROM
        desktop_base
      GROUP BY
        id_bucket,
        submission_date
      --
      UNION ALL
      --
      SELECT
        'nondesktop_global' AS datasource,
        'actual' AS type,
        submission_date,
        id_bucket,
        SUM(mau) AS mau
      FROM
        nondesktop_base
      GROUP BY
        id_bucket,
        submission_date
      UNION ALL
      SELECT
        'nondesktop_tier1' AS datasource,
        'actual' AS type,
        submission_date AS date,
        id_bucket,
        SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), mau, 0)) AS mau
      FROM
        nondesktop_base
      GROUP BY
        id_bucket,
        submission_date
    ),
    --
    --
    with_ci AS (
      SELECT
        datasource,
        type,
        submission_date,
        `moz-fx-data-shared-prod.udf_js.jackknife_sum_ci`(20, ARRAY_AGG(mau)).*
      FROM
        per_bucket
      GROUP BY
        datasource,
        type,
        submission_date
    ),
    --
    --
    with_forecast AS (
      SELECT
        datasource,
        type,
        submission_date AS `date`,
        total AS mau,
        low AS mau_low,
        high AS mau_high
      FROM
        with_ci
      UNION ALL
      SELECT
        datasource,
        type,
        `date`,
        mau,
        low90 AS mau_low,
        high90 AS mau_high
      FROM
        forecast_base
      WHERE
        type != 'original'
        AND date > (SELECT max(submission_date) FROM with_ci)
    ),
    -- We use imputed values for the period after the Armag-add-on deletion event;
    -- see https://bugzilla.mozilla.org/show_bug.cgi?id=1552558
    with_imputed AS (
      SELECT
        *
      FROM
        with_forecast
      WHERE
        NOT (
          `date`
          BETWEEN '2019-05-15'
          AND '2019-06-08'
          AND datasource IN ('desktop_global', 'desktop_tier1')
        )
      UNION ALL
      SELECT
        datasource,
        'actual' AS type,
        `date`,
        mau,
        -- The confidence interval is chosen here somewhat arbitrarily as 0.5% of
        -- the value; in any case, we should show a larger interval than we do for
        -- non-imputed actuals.
        mau - mau * 0.005 AS mau_low,
        mau + mau * 0.005 AS mau_high
      FROM
        `moz-fx-data-shared-prod.static.firefox_desktop_imputed_mau28_v1`
    )
    --
    SELECT
      *
    FROM
      with_imputed
    ORDER BY
      datasource,
      type,
      `date`
  )
