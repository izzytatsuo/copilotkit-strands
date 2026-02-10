# Available Processes

The `run_process(process_name)` tool loads predefined workflow instructions from `processes/`. Use these when the user's request matches a process's purpose.

## Process List

| Process Name | Purpose | When to Use |
|--------------|---------|-------------|
| `forecast_setup` | Pull unpublished VP, VOVI, pipeline artifacts, and PBA data then join for forecast review | "run forecast setup", preparing forecast review data |
| `forecast_review` | Pull published VP, VOVI, pipeline artifacts, and PBA data for post-publish review | "run forecast review", "review published", reviewing published forecasts |

## How to Use

When a user asks for something that matches a process, run it:

```
run_process("forecast_setup")
```

The process will return step-by-step instructions to follow.
