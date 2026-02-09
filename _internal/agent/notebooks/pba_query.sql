-- PBA query: joins intraday PBA with VP (ct) and VOVI data
-- Expects tables: vp (pivoted), vovi, intraday_pba to be loaded in DuckDB
-- PBA data spans multiple time periods (horizon_rank, horizon_day, horizon_hour)

SELECT
    CASE
        WHEN I.ofd_date = V.cpt_date::DATE THEN 'target'
        WHEN I.ofd_date = V.match_key::DATE THEN 'match'
    END AS pba_type,
    I.ofd_date || '#' || I.station || '#' || strftime(I.cpt_utc, '%H:%M') AS grid_key_utc,
    I.ofd_date || '#' || I.station || '#' || I.cpt_time_local AS grid_key_local,
    I.ofd_date AS pba_ofd_date,
    I.cpt_time_local AS pba_cpt_time_local,
    strftime(I.cpt_utc, '%H:%M') AS pba_cpt_utc,
    I.station AS node,
    I.internal_sort_code AS pba_internal_sort_code,
    I.ship_method AS pba_ship_method,
    I.horizon_rank AS pba_horizon_rank,
    I.horizon_day AS pba_horizon_day,
    I.horizon_hour AS pba_horizon_hour,
    I.horizon_minute AS pba_horizon_minute,
    I.dhm_horizon AS pba_dhm_horizon,
    I.slammed AS pba_slammed,
    I.scheduled AS pba_scheduled,
    I.soft_cap AS pba_soft_cap,
    I.hard_cap AS pba_hard_cap,
    I.cap_utilization AS pba_cap_utilization,

    P.latest_deployed_cap AS vp_latest_deployed_cap,
    P.cap_target_buffer AS vp_cap_target_buffer,
    P.automated_confidence AS vp_automated_confidence,
    P.post_cutoff_adjustment AS vp_post_cutoff_adjustment,
    P.automated_uncapped_slam_forecast AS vp_automated_uncapped_slam_forecast,
    P.auto_forecast_util AS vp_auto_forecast_util,
    P.util AS vp_util,

    V.modified_user AS vovi_modified_user,
    V.proposed_cap AS vovi_proposed_cap,
    V.post_cutoff_adjustment AS vovi_post_cutoff_adjustment,
    V.adjusted_forecast AS vovi_adjusted_forecast,
    V.forecast_source AS vovi_forecast_source,
    V.original_forecast AS vovi_original_forecast,
    V.forecast_status AS vovi_forecast_status,
    V.forecast_adjustment AS vovi_forecast_adjustment

FROM vp P
LEFT JOIN vovi V
    ON P.node = V.station
    AND P.cpts_utc = strftime(CAST(timezone('UTC', to_timestamp(V.station_cpt)) AS TIMESTAMP), '%H:%M')
INNER JOIN intraday_pba I
    ON COALESCE(P.node, V.station) = I.station
    AND P.cpts_utc = strftime(I.cpt_utc, '%H:%M')
    AND (I.ofd_date = V.cpt_date::DATE OR I.ofd_date = V.match_key::DATE)
WHERE I.horizon_minute = 0
ORDER BY
    COALESCE(P.node, V.station),
    P.cpts_utc,
    pba_type,
    pba_horizon_rank
