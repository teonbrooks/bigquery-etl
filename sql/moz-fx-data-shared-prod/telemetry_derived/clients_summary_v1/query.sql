WITH base AS (
  SELECT
    *,
    ARRAY(
      SELECT AS STRUCT
        udf.get_key(mozfun.hist.extract(histogram).values, 0) AS histogram
      FROM
        UNNEST(
          [
            payload.processes.content.histograms.devtools_aboutdebugging_opened_count,
            payload.processes.content.histograms.devtools_animationinspector_opened_count,
            payload.processes.content.histograms.devtools_browserconsole_opened_count,
            payload.processes.content.histograms.devtools_canvasdebugger_opened_count,
            payload.processes.content.histograms.devtools_computedview_opened_count,
            payload.processes.content.histograms.devtools_custom_opened_count,
            payload.processes.content.histograms.devtools_dom_opened_count,
            payload.processes.content.histograms.devtools_eyedropper_opened_count,
            payload.processes.content.histograms.devtools_fontinspector_opened_count,
            payload.processes.content.histograms.devtools_inspector_opened_count,
            payload.processes.content.histograms.devtools_jsbrowserdebugger_opened_count,
            payload.processes.content.histograms.devtools_jsdebugger_opened_count,
            payload.processes.content.histograms.devtools_jsprofiler_opened_count,
            payload.processes.content.histograms.devtools_layoutview_opened_count,
            payload.processes.content.histograms.devtools_memory_opened_count,
            payload.processes.content.histograms.devtools_menu_eyedropper_opened_count,
            payload.processes.content.histograms.devtools_netmonitor_opened_count,
            payload.processes.content.histograms.devtools_options_opened_count,
            payload.processes.content.histograms.devtools_paintflashing_opened_count,
            payload.processes.content.histograms.devtools_picker_eyedropper_opened_count,
            payload.processes.content.histograms.devtools_responsive_opened_count,
            payload.processes.content.histograms.devtools_ruleview_opened_count,
            payload.processes.content.histograms.devtools_scratchpad_opened_count,
            payload.processes.content.histograms.devtools_scratchpad_window_opened_count,
            payload.processes.content.histograms.devtools_shadereditor_opened_count,
            payload.processes.content.histograms.devtools_storage_opened_count,
            payload.processes.content.histograms.devtools_styleeditor_opened_count,
            payload.processes.content.histograms.devtools_webaudioeditor_opened_count,
            payload.processes.content.histograms.devtools_webconsole_opened_count,
            payload.processes.content.histograms.devtools_webide_opened_count,
          ]
        ) AS histogram
    ) AS count_histograms,
  FROM
    `moz-fx-data-shared-prod.telemetry.main`
)
SELECT
  submission_timestamp,
  client_id,
  sample_id,
  metadata.uri.app_update_channel AS channel,
  normalized_channel,
  normalized_os_version,
  metadata.geo.country,
  metadata.geo.city,
  metadata.geo.subdivision1 AS geo_subdivision1,
  metadata.geo.subdivision2 AS geo_subdivision2,
  metadata.isp.name AS isp_name,
  metadata.isp.organization AS isp_organization,
  environment.system.os.name AS os,
  environment.system.os.version AS os_version,
  SAFE_CAST(environment.system.os.service_pack_major AS INT64) AS os_service_pack_major,
  SAFE_CAST(environment.system.os.service_pack_minor AS INT64) AS os_service_pack_minor,
  SAFE_CAST(environment.system.os.windows_build_number AS INT64) AS windows_build_number,
  SAFE_CAST(environment.system.os.windows_ubr AS INT64) AS windows_ubr,
  SAFE_CAST(environment.system.os.install_year AS INT64) AS install_year,
  environment.system.is_wow64,
  SAFE_CAST(environment.system.memory_mb AS INT64) AS memory_mb,
  environment.system.cpu.count AS cpu_count,
  environment.system.cpu.cores AS cpu_cores,
  environment.system.cpu.vendor AS cpu_vendor,
  environment.system.cpu.family AS cpu_family,
  environment.system.cpu.model AS cpu_model,
  environment.system.cpu.stepping AS cpu_stepping,
  SAFE_CAST(environment.system.cpu.l2cache_kb AS INT64) AS cpu_l2_cache_kb,
  SAFE_CAST(environment.system.cpu.l3cache_kb AS INT64) AS cpu_l3_cache_kb,
  SAFE_CAST(environment.system.cpu.speed_m_hz AS INT64) AS cpu_speed_mhz,
  environment.system.gfx.features.d3d11.status AS gfx_features_d3d11_status,
  environment.system.gfx.features.d2d.status AS gfx_features_d2d_status,
  environment.system.gfx.features.gpu_process.status AS gfx_features_gpu_process_status,
  environment.system.gfx.features.advanced_layers.status AS gfx_features_advanced_layers_status,
  SAFE_CAST(environment.profile.creation_date AS INT64) AS profile_creation_date,
  payload.info.previous_build_id,
  payload.info.subsession_start_date,
  payload.info.subsession_counter,
  payload.info.subsession_length,
  environment.partner.distribution_id,
  IFNULL(
    environment.services.account_enabled,
    udf.boolean_histogram_to_boolean(payload.histograms.fxa_configured)
  ) AS fxa_configured,
  IFNULL(
    environment.services.sync_enabled,
    udf.boolean_histogram_to_boolean(payload.histograms.weave_configured)
  ) AS sync_configured,
  udf.histogram_max_key_with_nonzero_value(
    payload.histograms.weave_device_count_desktop
  ) AS sync_count_desktop,
  udf.histogram_max_key_with_nonzero_value(
    payload.histograms.weave_device_count_mobile
  ) AS sync_count_mobile,
  application.build_id AS app_build_id,
  application.display_version AS app_display_version,
  application.name AS app_name,
  application.version AS app_version,
  environment.build.build_id AS env_build_id,
  environment.build.version AS env_build_version,
  environment.build.architecture AS env_build_arch,
  environment.settings.e10s_enabled,
  environment.settings.locale,
  environment.settings.update.channel AS update_channel,
  environment.settings.update.enabled AS update_enabled,
  environment.settings.update.auto_download AS update_auto_download,
  STRUCT(
    environment.settings.attribution.source,
    environment.settings.attribution.medium,
    environment.settings.attribution.campaign,
    environment.settings.attribution.content
  ) AS attribution,
  environment.settings.sandbox.effective_content_process_level AS sandbox_effective_content_process_level,
  payload.info.timezone_offset,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.subprocess_crashes_with_dump, 'pluginhang')
  ) AS plugin_hangs,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.subprocess_abnormal_abort, 'plugin')
  ) AS aborts_plugin,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.subprocess_abnormal_abort, 'content')
  ) AS aborts_content,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.subprocess_abnormal_abort, 'gmplugin')
  ) AS aborts_gmplugin,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.subprocess_crashes_with_dump, 'plugin')
  ) AS crashes_detected_plugin,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.subprocess_crashes_with_dump, 'content')
  ) AS crashes_detected_content,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.subprocess_crashes_with_dump, 'gmplugin')
  ) AS crashes_detected_gmplugin,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.process_crash_submit_attempt, 'main-crash')
  ) AS crash_submit_attempt_main,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.process_crash_submit_attempt, 'content-crash')
  ) AS crash_submit_attempt_content,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.process_crash_submit_attempt, 'plugin-crash')
  ) AS crash_submit_attempt_plugin,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.process_crash_submit_success, 'main-crash')
  ) AS crash_submit_success_main,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.process_crash_submit_success, 'content-crash')
  ) AS crash_submit_success_content,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.process_crash_submit_success, 'plugin-crash')
  ) AS crash_submit_success_plugin,
  udf.extract_histogram_sum(
    mozfun.map.get_key(payload.keyed_histograms.subprocess_kill_hard, 'ShutDownKill')
  ) AS shutdown_kill,
  (
    SELECT
      version
    FROM
      UNNEST(environment.addons.active_plugins),
      UNNEST([STRUCT(SPLIT(version, '.') AS parts)])
    WHERE
      name = 'Shockwave Flash'
    ORDER BY
      SAFE_CAST(parts[SAFE_OFFSET(0)] AS INT64) DESC,
      SAFE_CAST(parts[SAFE_OFFSET(1)] AS INT64) DESC,
      SAFE_CAST(parts[SAFE_OFFSET(2)] AS INT64) DESC,
      SAFE_CAST(parts[SAFE_OFFSET(3)] AS INT64) DESC
    LIMIT
      1
  ) AS flash_version,
  application.vendor,
  environment.settings.is_default_browser,
  environment.settings.default_search_engine_data.name AS default_search_engine_data_name,
  environment.settings.default_search_engine_data.load_path AS default_search_engine_data_load_path,
  environment.settings.default_search_engine_data.origin AS default_search_engine_data_origin,
  environment.settings.default_search_engine_data.submission_url AS default_search_engine_data_submission_url,
  environment.settings.default_search_engine,
  udf.extract_histogram_sum(
    payload.histograms.devtools_toolbox_opened_count
  ) AS devtools_toolbox_opened_count,
  TIMESTAMP_DIFF(
    TIMESTAMP_TRUNC(submission_timestamp, SECOND),
    SAFE.PARSE_TIMESTAMP('%a, %d %b %Y %T %Z', metadata.header.date),
    SECOND
  ) AS client_clock_skew,
  TIMESTAMP_DIFF(
    TIMESTAMP_TRUNC(submission_timestamp, SECOND),
    SAFE.PARSE_TIMESTAMP('%FT%R:%E*SZ', creation_date),
    SECOND
  ) AS client_submission_latency,
  mozfun.hist.mean(
    mozfun.hist.extract(payload.histograms.places_bookmarks_count)
  ) AS places_bookmarks_count,
  mozfun.hist.mean(
    mozfun.hist.extract(payload.histograms.places_pages_count)
  ) AS places_pages_count,
  udf.extract_histogram_sum(payload.histograms.push_api_notify) AS push_api_notify,
  udf.extract_histogram_sum(payload.histograms.web_notification_shown) AS web_notification_shown,
  ARRAY(
    SELECT AS STRUCT
      SUBSTR(_key, 0, pos - 2) AS engine,
      SUBSTR(_key, pos) AS source,
      udf.extract_histogram_sum(value) AS `count`
    FROM
      UNNEST(payload.keyed_histograms.search_counts),
      UNNEST([REPLACE(key, 'in-content.', 'in-content:')]) AS _key,
      UNNEST([LENGTH(REGEXP_EXTRACT(_key, '.+[.].'))]) AS pos
  ) AS search_counts,
  udf_js.main_summary_active_addons(environment.addons.active_addons, NULL) AS active_addons,
  environment.settings.blocklist_enabled,
  environment.settings.addon_compatibility_check_enabled,
  environment.settings.telemetry_enabled,
  environment.settings.intl.accept_languages AS environment_settings_intl_accept_languages,
  environment.settings.intl.app_locales AS environment_settings_intl_app_locales,
  environment.settings.intl.available_locales AS environment_settings_intl_available_locales,
  environment.settings.intl.regional_prefs_locales AS environment_settings_intl_regional_prefs_locales,
  environment.settings.intl.requested_locales AS environment_settings_intl_requested_locales,
  environment.settings.intl.system_locales AS environment_settings_intl_system_locales,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(payload.histograms.ssl_handshake_result, '$.values.0') AS INT64
  ) AS ssl_handshake_result_success,
  (
    SELECT
      IFNULL(SUM(value), 0)
    FROM
      UNNEST(mozfun.hist.extract(payload.histograms.ssl_handshake_result).values)
    WHERE
      key
      BETWEEN 1
      AND 671
  ) AS ssl_handshake_result_failure,
  COALESCE(
    payload.processes.parent.scalars.browser_engagement_active_ticks,
    payload.simple_measurements.active_ticks,
    SAFE_CAST(
      JSON_EXTRACT_SCALAR(
        additional_properties,
        '$.payload.simpleMeasurements.activeTicks'
      ) AS INT64
    )
  ) AS active_ticks,
  COALESCE(
    payload.processes.parent.scalars.timestamps_first_paint,
    payload.simple_measurements.first_paint,
    SAFE_CAST(
      JSON_EXTRACT_SCALAR(additional_properties, '$.payload.simpleMeasurements.firstPaint') AS INT64
    )
  ) AS first_paint,
  COALESCE(
    payload.simple_measurements.session_restored,
    SAFE_CAST(
      JSON_EXTRACT_SCALAR(
        additional_properties,
        '$.payload.simpleMeasurements.sessionRestored'
      ) AS INT64
    )
  ) AS session_restored,
  SAFE_CAST(
    JSON_EXTRACT_SCALAR(payload.histograms.plugins_notification_shown, '$.values.1') AS INT64
  ) AS plugins_notification_shown,
  udf.extract_histogram_sum(payload.histograms.plugins_infobar_shown) AS plugins_infobar_shown,
  udf.extract_histogram_sum(payload.histograms.plugins_infobar_block) AS plugins_infobar_block,
  udf.extract_histogram_sum(payload.histograms.plugins_infobar_allow) AS plugins_infobar_allow,
  ARRAY(
    SELECT AS STRUCT
      key,
      value.branch AS value
    FROM
      UNNEST(environment.experiments)
  ) AS experiments,
  environment.settings.search_cohort,
  payload.processes.parent.scalars.aushelper_websense_reg_version AS scalar_parent_aushelper_websense_reg_version,
  payload.processes.parent.scalars.browser_engagement_max_concurrent_tab_count AS scalar_parent_browser_engagement_max_concurrent_tab_count,
  payload.processes.parent.scalars.browser_engagement_max_concurrent_window_count AS scalar_parent_browser_engagement_max_concurrent_window_count,
  payload.processes.parent.scalars.browser_engagement_tab_open_event_count AS scalar_parent_browser_engagement_tab_open_event_count,
  payload.processes.parent.scalars.browser_engagement_total_uri_count AS scalar_parent_browser_engagement_total_uri_count,
  payload.processes.parent.scalars.browser_engagement_unfiltered_uri_count AS scalar_parent_browser_engagement_unfiltered_uri_count,
  payload.processes.parent.scalars.browser_engagement_unique_domains_count AS scalar_parent_browser_engagement_unique_domains_count,
  payload.processes.parent.scalars.browser_engagement_window_open_event_count AS scalar_parent_browser_engagement_window_open_event_count,
  payload.processes.parent.scalars.contentblocking_trackers_blocked_count AS scalar_parent_contentblocking_trackers_blocked_count,
  payload.processes.parent.scalars.devtools_accessibility_node_inspected_count AS scalar_parent_devtools_accessibility_node_inspected_count,
  payload.processes.parent.scalars.devtools_accessibility_opened_count AS scalar_parent_devtools_accessibility_opened_count,
  payload.processes.parent.scalars.devtools_accessibility_picker_used_count AS scalar_parent_devtools_accessibility_picker_used_count,
  payload.processes.parent.scalars.devtools_accessibility_service_enabled_count AS scalar_parent_devtools_accessibility_service_enabled_count,
  payload.processes.parent.scalars.devtools_copy_full_css_selector_opened AS scalar_parent_devtools_copy_full_css_selector_opened,
  payload.processes.parent.scalars.devtools_copy_unique_css_selector_opened AS scalar_parent_devtools_copy_unique_css_selector_opened,
  payload.processes.parent.scalars.devtools_toolbar_eyedropper_opened AS scalar_parent_devtools_toolbar_eyedropper_opened,
  payload.processes.parent.scalars.dom_contentprocess_troubled_due_to_memory AS scalar_parent_dom_contentprocess_troubled_due_to_memory,
  payload.processes.parent.scalars.storage_sync_api_usage_extensions_using AS scalar_parent_storage_sync_api_usage_extensions_using,
  payload.processes.parent.scalars.webrtc_nicer_stun_retransmits AS scalar_parent_webrtc_nicer_stun_retransmits,
  payload.processes.parent.scalars.webrtc_nicer_turn_401s AS scalar_parent_webrtc_nicer_turn_401s,
  payload.processes.parent.scalars.webrtc_nicer_turn_403s AS scalar_parent_webrtc_nicer_turn_403s,
  payload.processes.parent.scalars.webrtc_nicer_turn_438s AS scalar_parent_webrtc_nicer_turn_438s,
  payload.processes.content.scalars.navigator_storage_estimate_count AS scalar_content_navigator_storage_estimate_count,
  payload.processes.content.scalars.navigator_storage_persist_count AS scalar_content_navigator_storage_persist_count,
  payload.processes.parent.keyed_scalars.browser_search_ad_clicks AS scalar_parent_browser_search_ad_clicks,
  payload.processes.parent.keyed_scalars.browser_search_with_ads AS scalar_parent_browser_search_with_ads,
  payload.processes.parent.keyed_scalars.devtools_accessibility_select_accessible_for_node AS scalar_parent_devtools_accessibility_select_accessible_for_node,
  payload.processes.parent.keyed_scalars.telemetry_event_counts AS scalar_parent_telemetry_event_counts,
  payload.processes.content.keyed_scalars.telemetry_event_counts AS scalar_content_telemetry_event_counts,
  count_histograms[OFFSET(0)].histogram AS histogram_content_devtools_aboutdebugging_opened_count,
  count_histograms[
    OFFSET(1)
  ].histogram AS histogram_content_devtools_animationinspector_opened_count,
  count_histograms[OFFSET(2)].histogram AS histogram_content_devtools_browserconsole_opened_count,
  count_histograms[OFFSET(3)].histogram AS histogram_content_devtools_canvasdebugger_opened_count,
  count_histograms[OFFSET(4)].histogram AS histogram_content_devtools_computedview_opened_count,
  count_histograms[OFFSET(5)].histogram AS histogram_content_devtools_custom_opened_count,
  count_histograms[OFFSET(6)].histogram AS histogram_content_devtools_dom_opened_count,
  count_histograms[OFFSET(7)].histogram AS histogram_content_devtools_eyedropper_opened_count,
  count_histograms[OFFSET(8)].histogram AS histogram_content_devtools_fontinspector_opened_count,
  count_histograms[OFFSET(9)].histogram AS histogram_content_devtools_inspector_opened_count,
  count_histograms[
    OFFSET(10)
  ].histogram AS histogram_content_devtools_jsbrowserdebugger_opened_count,
  count_histograms[OFFSET(11)].histogram AS histogram_content_devtools_jsdebugger_opened_count,
  count_histograms[OFFSET(12)].histogram AS histogram_content_devtools_jsprofiler_opened_count,
  count_histograms[OFFSET(13)].histogram AS histogram_content_devtools_layoutview_opened_count,
  count_histograms[OFFSET(14)].histogram AS histogram_content_devtools_memory_opened_count,
  count_histograms[OFFSET(15)].histogram AS histogram_content_devtools_menu_eyedropper_opened_count,
  count_histograms[OFFSET(16)].histogram AS histogram_content_devtools_netmonitor_opened_count,
  count_histograms[OFFSET(17)].histogram AS histogram_content_devtools_options_opened_count,
  count_histograms[OFFSET(18)].histogram AS histogram_content_devtools_paintflashing_opened_count,
  count_histograms[
    OFFSET(19)
  ].histogram AS histogram_content_devtools_picker_eyedropper_opened_count,
  count_histograms[OFFSET(20)].histogram AS histogram_content_devtools_responsive_opened_count,
  count_histograms[OFFSET(21)].histogram AS histogram_content_devtools_ruleview_opened_count,
  count_histograms[OFFSET(22)].histogram AS histogram_content_devtools_scratchpad_opened_count,
  count_histograms[
    OFFSET(23)
  ].histogram AS histogram_content_devtools_scratchpad_window_opened_count,
  count_histograms[OFFSET(24)].histogram AS histogram_content_devtools_shadereditor_opened_count,
  count_histograms[OFFSET(25)].histogram AS histogram_content_devtools_storage_opened_count,
  count_histograms[OFFSET(26)].histogram AS histogram_content_devtools_styleeditor_opened_count,
  count_histograms[OFFSET(27)].histogram AS histogram_content_devtools_webaudioeditor_opened_count,
  count_histograms[OFFSET(28)].histogram AS histogram_content_devtools_webconsole_opened_count,
  count_histograms[OFFSET(29)].histogram AS histogram_content_devtools_webide_opened_count,
FROM
  base
WHERE
  DATE(submission_timestamp) = @submission_date
  AND normalized_app_name = 'Firefox'
  AND document_id IS NOT NULL
