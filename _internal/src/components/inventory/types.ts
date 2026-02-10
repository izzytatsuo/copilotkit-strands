export interface JoinedRow {
  station: string;
  cycle: string | null;
  business_org: string | null;
  available_inputs: string;
  plan_start_date: string | null;
  ofd_dates: string | null;
  demand_types: string | null;
  cpts: string | null;
  cpts_local: string | null;
  cpts_utc: string | null;
  grid_key_local: string | null;
  grid_key_utc: string | null;
  forecast_type: string | null;
  automated_confidence: string | null;
  auto_forecast_util: number | null;
  util: number | null;
  vovi_uncapped_slam_forecast: string | null;
  uncapped_slam_forecast: string | null;
  adjusted_uncapped_slam_forecast: string | null;
  capped_slam_forecast: string | null;
  atrops_soft_cap: string | null;
  atrops_hard_cap: string | null;
  latest_deployed_cap: string | null;
  cap_target_buffer: string | null;
  current_slam: string | null;
  current_schedule: string | null;
  total_volume_available: string | null;
  total_backlog: string | null;
  in_station_backlog: string | null;
  post_cutoff_adjustment: string | null;
  net_volume_adjustments: string | null;
  vovi_adjustment: string | null;
  confidence_anomaly: string | null;
  // Setup confidence (from previous setup run)
  setup_automated_confidence: string | null;
  setup_confidence_anomaly: string | null;
  confidence_changed: string | null;
  automated_uncapped_slam_forecast: string | null;
  weekly_uncapped_slam_forecast: string | null;
  earlies_expected: string | null;
  earlies_received: string | null;
  returns: string | null;
  sideline_in: string | null;
  mnr_expected: string | null;
  mnr_received: string | null;
  vovi_modified_user: string | null;
  vovi_proposed_cap: string | null;
  vovi_post_cutoff_adjustment: string | null;
  vovi_adjusted_forecast: string | null;
  vovi_forecast_source: string | null;
  vovi_original_forecast: string | null;
  vovi_forecast_status: string | null;
  vovi_forecast_adjustment: string | null;
  vovi_current_slammed: string | null;
  vovi_current_scheduled: string | null;
  vovi_soft_cap: string | null;
  vovi_hard_cap: string | null;
  vovi_match_date: string | null;
  // Day classifier columns (from match date)
  bucket_lower: string | null;
  bucket_upper: string | null;
  peak_to_eod_drop_pct: string | null;
  constrained_after_target: string | null;
  sched_at_max_drop: string | null;
  max_drop_4hr: string | null;
  had_desched_notify: string | null;
  had_desched_execute: string | null;
  // Flatline flags (from target date)
  flatline_execute: string | null;
  flatline_notify: string | null;
  // Automated vs PBA quantile ratios
  auto_vs_p10: string | null;
  auto_vs_p30: string | null;
  auto_vs_p50: string | null;
  auto_vs_p70: string | null;
  auto_vs_p90: string | null;
  // VOVI vs PBA quantile ratios
  vovi_vs_p10: string | null;
  vovi_vs_p30: string | null;
  vovi_vs_p50: string | null;
  vovi_vs_p70: string | null;
  vovi_vs_p90: string | null;
  execution_ts: string | null;
  // Derived/computed fields
  flags: string | null;
  tab_group: string;
  // Parsed from flags string for filtering
  _flags: string[];
}

export interface PbaRow {
  grid_key: string;
  pba_type: "target" | "match" | "vp_automated" | "vp_weekly" | "vp_vovi";
  pba_ofd_date: string;
  pba_dhm_horizon: string;
  pba_bi_hourly_local: string;
  pba_horizon_rank: number;
  pba_scheduled: number;
  pba_soft_cap: number;
  pba_hard_cap: number;
  pba_slammed: number;
  pba_cap_utilization: number;
  // Cumulative fan chart fields (target only)
  pba_cumulative_median: number | null;
  pba_cumulative_median_adj: number | null;
  pba_p10: number | null;
  pba_p30: number | null;
  pba_p50: number | null;
  pba_p70: number | null;
  pba_p90: number | null;
}
