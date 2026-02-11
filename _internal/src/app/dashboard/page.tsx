"use client";

import {
  useDefaultTool,
  useRenderToolCall,
  useFrontendTool,
  useCoAgent,
} from "@copilotkit/react-core";
import { CopilotKitCSSProperties, CopilotSidebar } from "@copilotkit/react-ui";
import { useEffect, useState } from "react";
import { DefaultToolComponent } from "@/components/default-tool-ui";
import { WeatherCard } from "@/components/weather";
import ForecastDashboard from "@/components/inventory/ForecastDashboard";

export default function DashboardPage() {
  const [themeColor, setThemeColor] = useState("#000000");

  useFrontendTool({
    name: "set_theme_color",
    parameters: [
      {
        name: "theme_color",
        description: "The theme color to set. Make sure to pick nice colors.",
        required: true,
      },
    ],
    handler({ theme_color }) {
      setThemeColor(theme_color);
    },
  });

  return (
    <main
      style={
        { "--copilot-kit-primary-color": themeColor } as CopilotKitCSSProperties
      }
    >
      <CopilotSidebar
        clickOutsideToClose={false}
        defaultOpen={false}
        labels={{
          title: "Knucks",
          initial: "Hey! How can I help?",
        }}
        suggestions={[
          {
            title: "Run Forecast Setup",
            message: "Run the forecast setup notebook",
          },
          {
            title: "Refresh Data",
            message: "Re-run the data pulls and refresh the dashboard",
          },
        ]}
      >
        <DashboardContent themeColor={themeColor} />
      </CopilotSidebar>
    </main>
  );
}

interface LayoutConfig {
  split: "horizontal" | "vertical";
  chartPct: number;
}

function DashboardContent({ themeColor }: { themeColor: string }) {
  const [refreshKey, setRefreshKey] = useState(0);
  const [layout, setLayout] = useState<LayoutConfig>({ split: "horizontal", chartPct: 45 });

  const { state } = useCoAgent({
    name: "strands_agent",
    initialState: {
      proverbs: ["mountain", "amzl"],
    },
  });

  useFrontendTool({
    name: "update_layout",
    description: "Update the dashboard layout direction and panel sizes.",
    parameters: [
      { name: "split", description: "Layout direction: 'horizontal' (chart top, grid bottom) or 'vertical' (chart right, grid left)", required: false },
      { name: "chart_pct", description: "Chart panel size as percentage (10-90)", required: false },
    ],
    handler({ split, chart_pct }: { split?: string; chart_pct?: number }) {
      setLayout((prev) => ({
        split: split === "vertical" ? "vertical" : split === "horizontal" ? "horizontal" : prev.split,
        chartPct: chart_pct != null ? Math.max(10, Math.min(90, chart_pct)) : prev.chartPct,
      }));
    },
  });

  useFrontendTool({
    name: "refresh_dashboard",
    description: "Refresh the dashboard grid and chart data after a notebook run completes.",
    parameters: [],
    handler() {
      setRefreshKey((k) => k + 1);
    },
  });

  useEffect(() => {
    console.log("Dashboard state:", state);
  }, [state]);

  useRenderToolCall(
    {
      name: "get_weather",
      parameters: [
        {
          name: "location",
          description: "The location to get the weather for.",
          required: true,
        },
      ],
      render: (props) => (
        <WeatherCard themeColor={themeColor} location={props.args.location} />
      ),
    },
    [themeColor],
  );

  useDefaultTool(
    {
      render: (props) => (
        <DefaultToolComponent themeColor={themeColor} {...props} />
      ),
    },
    [themeColor],
  );

  return (
    <div style={{ height: "100vh", display: "flex", flexDirection: "column" }}>
      <ForecastDashboard refreshKey={refreshKey} layout={layout} />
    </div>
  );
}
