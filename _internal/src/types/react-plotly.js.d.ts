declare module "react-plotly.js" {
  import { Component } from "react";

  interface PlotParams {
    data: Array<Record<string, unknown>>;
    layout?: Record<string, unknown>;
    config?: Record<string, unknown>;
    frames?: Array<Record<string, unknown>>;
    useResizeHandler?: boolean;
    style?: React.CSSProperties;
    className?: string;
    onInitialized?: (figure: { data: unknown[]; layout: unknown }, graphDiv: HTMLElement) => void;
    onUpdate?: (figure: { data: unknown[]; layout: unknown }, graphDiv: HTMLElement) => void;
    onPurge?: (figure: { data: unknown[]; layout: unknown }, graphDiv: HTMLElement) => void;
    onError?: (err: Error) => void;
    onClick?: (event: { points: unknown[]; event: MouseEvent }) => void;
    onHover?: (event: { points: unknown[]; event: MouseEvent }) => void;
    onUnhover?: (event: { points: unknown[]; event: MouseEvent }) => void;
    onSelected?: (event: { points: unknown[]; range?: unknown }) => void;
    revision?: number;
  }

  class Plot extends Component<PlotParams> {}
  export default Plot;
}
