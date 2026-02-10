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
  { field: "station", headerName: "Station", width: 100, pinned: "left" },
  { field: "cpts_local", headerName: "CPT", width: 70 },
  { field: "automated_confidence", headerName: "Confidence", width: 100 },
  {
    field: "automated_uncapped_slam_forecast",
    headerName: "Auto Uncapped Fcst",
    width: 150,
    type: "numericColumn",
  },
  {
    field: "vovi_uncapped_slam_forecast",
    headerName: "VOVI Uncapped Fcst",
    width: 150,
    type: "numericColumn",
  },
  { field: "vovi_modified_user", headerName: "Modified User", width: 120 },
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
  { field: "pba_scheduled", name: "Scheduled", color: "#000000" },
  { field: "pba_slammed", name: "Slammed", color: "#00897B", hidden: true },
  { field: "pba_soft_cap", name: "Soft Cap", color: "#000000", dash: "4px,3px", isCap: true },
  { field: "pba_hard_cap", name: "Hard Cap", color: "#000000", isCap: true },
];

function buildTraces(pbaData: PbaRow[], gridKey: string) {
  const filtered = pbaData.filter((r) => r.grid_key === gridKey);
  const traces: Array<Record<string, unknown>> = [];

  for (const pbaType of ["target", "match"] as const) {
    const rows = filtered
      .filter((r) => r.pba_type === pbaType)
      .sort((a, b) => a.pba_horizon_rank - b.pba_horizon_rank);

    if (rows.length === 0) continue;

    const x = rows.map((r) => r.pba_dhm_horizon);
    const suffix = pbaType === "target" ? " (Target)" : " (Match)";
    for (const cfg of TRACE_CONFIGS) {
      traces.push({
        x,
        y: rows.map((r) => r[cfg.field] as number),
        name: cfg.name + suffix,
        type: "scatter",
        mode: "lines",
        visible: cfg.hidden ? "legendonly" : true,
        line: {
          color: pbaType === "match" ? "#1565C0" : cfg.color,
          dash: cfg.dash ?? "solid",
          width: cfg.isCap ? 1.5 : 2,
        },
      });
    }
  }

  return traces;
}

// ── Helpers ───────────────────────────────────────────────────────────

function deriveTabGroup(row: JoinedRow): string {
  const inputs = row.available_inputs ?? "unknown";
  const confidence = row.automated_confidence ?? "none";
  return `${inputs}_${confidence}`;
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
    // Derive tab_group
    const row = obj as unknown as JoinedRow;
    row.tab_group = deriveTabGroup(row);
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
    const counts = new Map<string, number>();
    for (const row of gridData) {
      const tg = row.tab_group ?? "unknown_none";
      counts.set(tg, (counts.get(tg) ?? 0) + 1);
    }
    return [
      { label: "All", value: "all", count: gridData.length },
      ...Array.from(counts.entries())
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([value, count]) => ({ label: value, value, count })),
    ];
  }, [gridData]);

  // ── Filtered Data ─────────────────────────────────────────────────

  const filteredData = useMemo(() => {
    let rows = gridData;
    if (activeTab !== "all") {
      rows = rows.filter((r) => r.tab_group === activeTab);
    }
    return rows;
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

  const chartTraces = useMemo(() => {
    if (!selectedGridKey) return [];
    return buildTraces(pbaData, selectedGridKey);
  }, [pbaData, selectedGridKey]);

  const chartTitle = useMemo(() => {
    if (!selectedGridKey) return "";
    const parts = selectedGridKey.split("#");
    return `PBA: ${parts[1] ?? ""} @ ${parts[2] ?? ""} (${parts[0] ?? ""})`;
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
          placeholder="Search stations..."
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
                filter: true,
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
          {selectedGridKey && chartTraces.length > 0 ? (
            <>
              <div className={styles.chartHeader}>{chartTitle}</div>
              <div className={styles.chartBody}>
                <Plot
                  data={chartTraces}
                  layout={{
                    autosize: true,
                    margin: { l: 50, r: 20, t: 10, b: 40 },
                    xaxis: {
                      title: "DHM Horizon",
                      type: "category",
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
