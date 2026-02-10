"use client";

import { useState, useMemo, useCallback, useRef, useEffect } from "react";
import { AgGridReact } from "ag-grid-react";
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
  type GridReadyEvent,
  type RowClickedEvent,
  type GridApi,
} from "ag-grid-community";
import dynamic from "next/dynamic";
import type { JoinedRow, PbaRow } from "./types";
import styles from "./ForecastDashboard.module.css";

// Register AG Grid modules
ModuleRegistry.registerModules([AllCommunityModule]);

// Dynamic import for Plotly (no SSR — it accesses window)
const Plot = dynamic(() => import("react-plotly.js"), { ssr: false });

// ── Column Definitions ────────────────────────────────────────────────

const columnDefs: ColDef<JoinedRow>[] = [
  { field: "station", headerName: "Station", width: 80, minWidth: 80, maxWidth: 80, pinned: "left" },
  {
    field: "cpts_local",
    headerName: "CPT",
    width: 80,
    minWidth: 80,
    maxWidth: 80,
    pinned: "left",
    valueFormatter: (params) => params.value ? String(params.value).slice(0, 5) : "",
  },
  { field: "flags", headerName: "Flags", width: 110, minWidth: 110, maxWidth: 110, pinned: "left" },
  { field: "available_inputs", headerName: "Inputs", width: 90 },
  { field: "automated_confidence", headerName: "Confidence", width: 100 },
  {
    field: "automated_uncapped_slam_forecast",
    headerName: "Automated",
    width: 150,
    type: "numericColumn",
  },
  {
    field: "vovi_uncapped_slam_forecast",
    headerName: "VOVI",
    width: 150,
    type: "numericColumn",
  },
  { field: "vovi_modified_user", headerName: "Modified User", width: 120 },
  { field: "bucket_lower", headerName: "Bucket Lo", width: 90, type: "numericColumn" },
  { field: "bucket_upper", headerName: "Bucket Hi", width: 90, type: "numericColumn" },
  { field: "peak_to_eod_drop_pct", headerName: "Peak-EOD Drop%", width: 120, type: "numericColumn" },
  { field: "constrained_after_target", headerName: "Constrained", width: 100, type: "numericColumn" },
  { field: "sched_at_max_drop", headerName: "Sched at Max Drop", width: 130, type: "numericColumn" },
  { field: "max_drop_4hr", headerName: "Max Drop 4hr", width: 110, type: "numericColumn" },
  { field: "had_desched_notify", headerName: "Desched Notify", width: 110, type: "numericColumn" },
  { field: "had_desched_execute", headerName: "Desched Exec", width: 110, type: "numericColumn" },
  { field: "flatline_execute", headerName: "Flatline Exec", width: 110, type: "numericColumn" },
  { field: "flatline_notify", headerName: "Flatline Notify", width: 110, type: "numericColumn" },
];

// ── Chart Trace Builder ───────────────────────────────────────────────

interface TraceConfig {
  field: keyof PbaRow;
  name: string;
  color: string;
  dash?: string;
  hidden?: boolean;
  isCap?: boolean;
}

const TRACE_CONFIGS: TraceConfig[] = [
  { field: "pba_scheduled", name: "sch", color: "#000000" },
  { field: "pba_slammed", name: "slm", color: "#00897B", hidden: true },
  { field: "pba_soft_cap", name: "cap", color: "#000000", dash: "4px,3px", isCap: true },
  { field: "pba_hard_cap", name: "cap", color: "#000000", isCap: true },
];

interface BuildResult {
  traces: Array<Record<string, unknown>>;
  categoryOrder: string[];
}

function buildTraces(pbaData: PbaRow[], gridKey: string): BuildResult {
  const filtered = pbaData.filter((r) => r.grid_key === gridKey);
  const traces: Array<Record<string, unknown>> = [];

  // Build sorted category order from ALL rows (target + match) by horizon_rank
  const horizonMap = new Map<string, number>();
  for (const r of filtered) {
    const existing = horizonMap.get(r.pba_dhm_horizon);
    if (existing === undefined || r.pba_horizon_rank < existing) {
      horizonMap.set(r.pba_dhm_horizon, r.pba_horizon_rank);
    }
  }
  const categoryOrder = Array.from(horizonMap.entries())
    .sort((a, b) => a[1] - b[1])
    .map(([dhm]) => dhm);

  for (const pbaType of ["target", "match"] as const) {
    const rows = filtered
      .filter((r) => r.pba_type === pbaType)
      .sort((a, b) => a.pba_horizon_rank - b.pba_horizon_rank);

    if (rows.length === 0) continue;

    const x = rows.map((r) => r.pba_dhm_horizon);
    const prefix = pbaType === "match" ? "match-" : "";

    // Link soft cap and hard cap into one legend group
    for (const cfg of TRACE_CONFIGS) {
      const isHardCap = cfg.field === "pba_hard_cap";
      const isSoftCap = cfg.field === "pba_soft_cap";
      const legendGroup = (isSoftCap || isHardCap)
        ? `${prefix}cap`
        : `${prefix}${cfg.name}`;

      traces.push({
        x,
        y: rows.map((r) => r[cfg.field] as number),
        name: isHardCap
          ? `${prefix}${cfg.name} (hard)`
          : `${prefix}${cfg.name}`,
        legendgroup: legendGroup,
        showlegend: !isHardCap,
        type: "scatter",
        mode: "lines",
        visible: cfg.hidden ? "legendonly" : true,
        line: {
          color: pbaType === "match" ? "#4DB6AC" : cfg.color,
          dash: cfg.dash ?? "solid",
          width: cfg.isCap ? 1.5 : 2,
        },
      });
    }

    // Fan chart traces — target only
    if (pbaType === "target") {
      const hasFan = rows.some((r) => r.pba_p10 != null && r.pba_p90 != null);
      if (hasFan) {
        // Outer fan: p10 lower bound (drawn first, no fill)
        traces.push({
          x,
          y: rows.map((r) => r.pba_p10 ?? r.pba_scheduled),
          name: "p10-p90",
          legendgroup: "fan-outer",
          showlegend: false,
          type: "scatter",
          mode: "lines",
          line: { color: "transparent", width: 0 },
          hoverinfo: "skip",
        });
        // Outer fan: p90 upper bound (fill down to p10)
        traces.push({
          x,
          y: rows.map((r) => r.pba_p90 ?? r.pba_scheduled),
          name: "p10-p90",
          legendgroup: "fan-outer",
          showlegend: true,
          type: "scatter",
          mode: "lines",
          fill: "tonexty",
          fillcolor: "rgba(33,150,243,0.15)",
          line: { color: "transparent", width: 0 },
          hoverinfo: "skip",
        });

        // Inner fan: p30 lower bound (no fill)
        traces.push({
          x,
          y: rows.map((r) => r.pba_p30 ?? r.pba_scheduled),
          name: "p30-p70",
          legendgroup: "fan-inner",
          showlegend: false,
          type: "scatter",
          mode: "lines",
          line: { color: "transparent", width: 0 },
          hoverinfo: "skip",
        });
        // Inner fan: p70 upper bound (fill down to p30)
        traces.push({
          x,
          y: rows.map((r) => r.pba_p70 ?? r.pba_scheduled),
          name: "p30-p70",
          legendgroup: "fan-inner",
          showlegend: true,
          type: "scatter",
          mode: "lines",
          fill: "tonexty",
          fillcolor: "rgba(33,150,243,0.35)",
          line: { color: "transparent", width: 0 },
          hoverinfo: "skip",
        });

        // Cumulative median line
        traces.push({
          x,
          y: rows.map((r) => r.pba_cumulative_median),
          name: "median",
          legendgroup: "median",
          showlegend: true,
          type: "scatter",
          mode: "lines",
          line: { color: "#1565C0", width: 2, dash: "solid" },
        });

        // Cumulative median adjusted line
        traces.push({
          x,
          y: rows.map((r) => r.pba_cumulative_median_adj),
          name: "median adj",
          legendgroup: "median-adj",
          showlegend: true,
          type: "scatter",
          mode: "lines",
          visible: "legendonly",
          line: { color: "#1565C0", width: 2, dash: "solid" },
        });
      }
    }
  }

  return { traces, categoryOrder };
}

// ── Helpers ───────────────────────────────────────────────────────────

function deriveTabGroup(row: JoinedRow): string {
  const inputs = row.available_inputs ?? "unknown";
  const confidence = row.automated_confidence ?? "none";
  return `${inputs}_${confidence}`;
}

function parseFlags(row: JoinedRow): string[] {
  if (!row.flags) return [];
  return row.flags.split(",").filter(Boolean);
}

function parseGridCsv(text: string): JoinedRow[] {
  const lines = text.trim().split("\n");
  if (lines.length < 2) return [];
  const headers = lines[0].split(",");
  return lines.slice(1).map((line) => {
    const values = line.split(",");
    const obj: Record<string, unknown> = {};
    headers.forEach((h, i) => {
      const v = values[i] ?? "";
      obj[h.trim()] = v === "" ? null : v;
    });
    // Parse numeric fields
    if (obj.auto_forecast_util != null)
      obj.auto_forecast_util = parseFloat(obj.auto_forecast_util as string);
    if (obj.util != null) obj.util = parseFloat(obj.util as string);
    // Derive tab_group, parse flags
    const row = obj as unknown as JoinedRow;
    row.tab_group = deriveTabGroup(row);
    row._flags = parseFlags(row);
    return row;
  });
}

// ── Main Component ────────────────────────────────────────────────────

interface ForecastDashboardProps {
  gridDataUrl?: string;
  visualDataUrl?: string;
}

export default function ForecastDashboard({
  gridDataUrl = "/api/grid-data",
  visualDataUrl = "/api/visual-data",
}: ForecastDashboardProps) {
  const gridRef = useRef<AgGridReact<JoinedRow>>(null);
  const [gridApi, setGridApi] = useState<GridApi<JoinedRow> | null>(null);

  // Data state
  const [gridData, setGridData] = useState<JoinedRow[]>([]);
  const [pbaData, setPbaData] = useState<PbaRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // UI state
  const [searchText, setSearchText] = useState("");
  const [activeTab, setActiveTab] = useState<string>("all");
  const [selectedGridKey, setSelectedGridKey] = useState<string | null>(null);

  // ── Data Loading ──────────────────────────────────────────────────

  useEffect(() => {
    let cancelled = false;

    async function loadData() {
      setLoading(true);
      setError(null);

      try {
        const [gridResp, pbaResp] = await Promise.allSettled([
          fetch(gridDataUrl),
          fetch(visualDataUrl),
        ]);

        if (!cancelled) {
          // Grid data — try JSON first, fall back to CSV
          if (gridResp.status === "fulfilled" && gridResp.value.ok) {
            const contentType =
              gridResp.value.headers.get("content-type") ?? "";
            if (contentType.includes("json")) {
              const json = await gridResp.value.json();
              const rows = (json as JoinedRow[]).map((r) => ({
                ...r,
                tab_group: deriveTabGroup(r),
                _flags: parseFlags(r),
              }));
              setGridData(rows);
            } else {
              const text = await gridResp.value.text();
              setGridData(parseGridCsv(text));
            }
          } else {
            console.warn("Grid data unavailable, using empty dataset");
            setGridData([]);
          }

          // PBA data — always JSON
          if (pbaResp.status === "fulfilled" && pbaResp.value.ok) {
            const json = await pbaResp.value.json();
            setPbaData(json as PbaRow[]);
          } else {
            console.warn("PBA visual data unavailable, using empty dataset");
            setPbaData([]);
          }
        }
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : "Failed to load data");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    loadData();
    return () => {
      cancelled = true;
    };
  }, [gridDataUrl, visualDataUrl]);

  // ── Tabs ──────────────────────────────────────────────────────────

  const tabGroups = useMemo(() => {
    const flagCounts = new Map<string, number>();
    const confCounts = new Map<string, number>();
    for (const row of gridData) {
      for (const f of row._flags ?? []) {
        flagCounts.set(f, (flagCounts.get(f) ?? 0) + 1);
      }
      const conf = row.automated_confidence != null ? parseFloat(String(row.automated_confidence)) : null;
      if (conf != null && !isNaN(conf)) {
        const key = conf >= 1 ? "conf:1" : "conf:0";
        confCounts.set(key, (confCounts.get(key) ?? 0) + 1);
      }
    }
    return [
      { label: "All", value: "all", count: gridData.length },
      // Flag tabs first
      ...Array.from(flagCounts.entries())
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([value, count]) => ({ label: value, value: `flag:${value}`, count })),
      // Confidence tabs
      ...(confCounts.has("conf:1") ? [{ label: "Confidence 1", value: "conf:1", count: confCounts.get("conf:1")! }] : []),
      ...(confCounts.has("conf:0") ? [{ label: "Confidence 0", value: "conf:0", count: confCounts.get("conf:0")! }] : []),
    ];
  }, [gridData]);

  // ── Filtered Data ─────────────────────────────────────────────────

  const filteredData = useMemo(() => {
    if (activeTab === "all") return gridData;
    if (activeTab.startsWith("flag:")) {
      const flag = activeTab.slice(5);
      return gridData.filter((r) => r._flags?.includes(flag));
    }
    if (activeTab === "conf:1") {
      return gridData.filter((r) => {
        const c = r.automated_confidence != null ? parseFloat(String(r.automated_confidence)) : null;
        return c != null && !isNaN(c) && c >= 1;
      });
    }
    if (activeTab === "conf:0") {
      return gridData.filter((r) => {
        const c = r.automated_confidence != null ? parseFloat(String(r.automated_confidence)) : null;
        return c != null && !isNaN(c) && c < 1;
      });
    }
    return gridData.filter((r) => r.tab_group === activeTab);
  }, [gridData, activeTab]);

  // ── Grid Events ───────────────────────────────────────────────────

  const onGridReady = useCallback((params: GridReadyEvent<JoinedRow>) => {
    setGridApi(params.api);
  }, []);

  const onRowClicked = useCallback((event: RowClickedEvent<JoinedRow>) => {
    const row = event.data;
    if (!row) return;
    const key = row.grid_key_local ?? null;
    setSelectedGridKey((prev) => (prev === key ? null : key));
  }, []);

  // Quick filter
  useEffect(() => {
    if (gridApi) {
      gridApi.setGridOption("quickFilterText", searchText);
    }
  }, [gridApi, searchText]);

  // ── Chart ─────────────────────────────────────────────────────────

  const chartResult = useMemo(() => {
    if (!selectedGridKey) return { traces: [], categoryOrder: [] };
    return buildTraces(pbaData, selectedGridKey);
  }, [pbaData, selectedGridKey]);

  const chartTitle = useMemo(() => {
    if (!selectedGridKey) return "";
    const parts = selectedGridKey.split("#");
    return `PBA: ${parts[1] ?? ""} ${parts[2] ?? ""} (${parts[0] ?? ""})`;
  }, [selectedGridKey]);

  // ── Render ────────────────────────────────────────────────────────

  if (loading) {
    return (
      <div className={styles.wrapper}>
        <div className={styles.chartPlaceholder}>Loading dashboard data...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={styles.wrapper}>
        <div className={styles.chartPlaceholder}>Error: {error}</div>
      </div>
    );
  }

  return (
    <div className={styles.wrapper}>
      {/* Toolbar: search + tabs */}
      <div className={styles.toolbar}>
        <input
          className={styles.searchInput}
          type="text"
          placeholder=""
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
        />
        <div className={styles.tabs}>
          {tabGroups.map((tg) => (
            <button
              key={tg.value}
              className={`${styles.tab} ${activeTab === tg.value ? styles.tabActive : ""}`}
              onClick={() => setActiveTab(tg.value)}
            >
              {tg.label}
              <span className={styles.tabCount}>({tg.count})</span>
            </button>
          ))}
        </div>
      </div>

      {/* Split: Grid (40%) | Chart (60%) */}
      <div className={styles.splitContainer}>
        {/* Grid Panel */}
        <div className={styles.gridPanel}>
          <div className="ag-theme-alpine" style={{ flex: 1 }}>
            <AgGridReact<JoinedRow>
              ref={gridRef}
              rowData={filteredData}
              columnDefs={columnDefs}
              defaultColDef={{
                sortable: true,
                resizable: true,
                filter: false,
              }}
              rowSelection="single"
              onGridReady={onGridReady}
              onRowClicked={onRowClicked}
              animateRows={false}
              getRowId={(params) =>
                params.data.grid_key_local ?? params.data.station
              }
            />
          </div>
        </div>

        {/* Chart Panel */}
        <div className={styles.chartPanel}>
          {selectedGridKey && chartResult.traces.length > 0 ? (
            <>
              <div className={styles.chartHeader}>{chartTitle}</div>
              <div className={styles.chartBody}>
                <Plot
                  data={chartResult.traces}
                  layout={{
                    autosize: true,
                    margin: { l: 50, r: 20, t: 10, b: 40 },
                    xaxis: {
                      title: "DHM Horizon",
                      type: "category",
                      categoryorder: "array",
                      categoryarray: chartResult.categoryOrder,
                      dtick: 12,
                      tickangle: 0,
                    },
                    yaxis: {
                      title: "Volume",
                    },
                    legend: {
                      orientation: "h",
                      y: -0.15,
                      x: 0.5,
                      xanchor: "center",
                    },
                    hovermode: "x unified",
                  }}
                  config={{
                    responsive: true,
                    displayModeBar: false,
                  }}
                  useResizeHandler
                  style={{ width: "100%", height: "100%" }}
                />
              </div>
            </>
          ) : (
            <div className={styles.chartPlaceholder}>
              {selectedGridKey
                ? "No PBA data for this row"
                : "Click a grid row to view PBA chart"}
            </div>
          )}
        </div>
      </div>

      {/* Status Bar */}
      <div className={styles.statusBar}>
        <span>
          Rows: {filteredData.length} / {gridData.length}
        </span>
        <span>PBA records: {pbaData.length}</span>
        {selectedGridKey && <span>Selected: {selectedGridKey}</span>}
      </div>
    </div>
  );
}
