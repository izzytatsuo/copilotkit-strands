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
  const [themeColor, setThemeColor] = useState("#6366f1");

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
            message: "Run the forecast_setup notebook",
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

function DashboardContent({ themeColor }: { themeColor: string }) {
  const { state } = useCoAgent({
    name: "strands_agent",
    initialState: {
      proverbs: ["mountain", "amzl"],
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
      <ForecastDashboard />
    </div>
  );
}
