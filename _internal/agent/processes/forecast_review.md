# Forecast Review Process

## Overview
**Purpose:** Post-publish review. Pull published VP data (local time CPTs), VOVI forecasts, pipeline artifacts, and intraday PBA, then join into a single dataset for review.
**When to use:** When user says "run forecast review", "forecast review", "review published", or asks to review published forecast data.
**Output:** `joined.csv` with site list + VP + VOVI outer join, plus pipeline artifacts and PBA data saved to context directory.

---

## Required Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `tz_bucket` | **Yes** | Timezone bucket (Eastern, Central, Mountain, Pacific, etc.) |
| `biz` | No | Business line (default: AMZL) |
| `vovi_start_utc` | No | VOVI modified_time filter start, UTC epoch ms (default: 0 — all VOVI) |
| `vovi_end_utc` | No | VOVI modified_time filter end, UTC epoch ms (default: now — all VOVI) |
| `setup_ctx_dir` | No | Path to a previous setup context directory to import confidence data (default: None — no setup confidence) |

**If tz_bucket is not provided, ask the user which timezone bucket to run.**

---

## Step 0: Find Setup Context (VOVI Start Time + Confidence Data)

Before executing, find the latest forecast **setup** run for the matching timezone bucket. This provides two things:
1. **VOVI start time** — so only modifications made after setup are included
2. **Setup confidence data** — pre-publish `automated_confidence` and `confidence_anomaly` for comparison

Context directories live at the workspace root under `data/contexts/` and are named `YYYYMMDD_HHMMSS_forecast_{tz_bucket}`.

Run this Python snippet to find the latest setup context:

```python
import os, re
from datetime import datetime, timezone
from pathlib import Path

contexts_path = os.path.join(os.environ.get('WORKSPACE_ROOT', os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))), 'data', 'contexts')
# Alternative: just use the absolute path on this machine
# contexts_path = r'C:\Users\admsia\copilotkit-strands\data\contexts'

tz_suffix = f'forecast_{tz_bucket.lower()}'
dirs = sorted([
    d for d in os.listdir(contexts_path)
    if d.startswith('20') and tz_suffix in d and 'forecast_review' not in d
])
if dirs:
    latest = dirs[-1]
    latest_path = os.path.join(contexts_path, latest)
    # Parse YYYYMMDD_HHMMSS from directory name
    m = re.match(r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})_', latest)
    if m:
        dt = datetime(int(m[1]), int(m[2]), int(m[3]), int(m[4]), int(m[5]), int(m[6]), tzinfo=timezone.utc)
        epoch_ms = int(dt.timestamp() * 1000)
        has_joined = Path(latest_path, 'joined.csv').exists()
        print(f'Latest setup: {latest} -> vovi_start_utc={epoch_ms}, joined.csv={has_joined}')
```

If a setup directory is found, present **both** values to the user in one prompt:

> "The latest setup run for **{tz_bucket}** was `{dir_name}` (`{YYYY-MM-DD HH:MM:SS} UTC`). I'll use this as the VOVI start time and import its confidence data for comparison. Does that work, or would you like a different start time?"

Then set variables:
- `vovi_start_utc` = epoch_ms from the directory timestamp
- `setup_ctx_dir` = full path to the setup context directory (if `joined.csv` exists in it), otherwise `None`

**Edge cases:**
- If `joined.csv` is missing in the setup directory: use for `vovi_start_utc` only, set `setup_ctx_dir=None`
- If no setup directory found at all: `vovi_start_utc=0`, `setup_ctx_dir=None`

Wait for the user to approve or provide an alternative before proceeding.

---

## Step 1: Execute Notebook

Use the `run_notebook` tool:

```python
run_notebook("forecast_review.ipynb", variables={"tz_bucket": "{timezone}", "biz": "AMZL", "vovi_start_utc": {epoch_ms}, "setup_ctx_dir": "{setup_ctx_dir_or_None}"})
```

The notebook will:
1. Auto-calculate target date (tomorrow Pacific)
2. Fetch CT metadata and build timezone bucket map
3. Filter site list to stations in the specified timezone bucket
4. Build published VP URLs and fetch via batch HTTP
5. Fetch VOVI forecasts (US + CA, AMZL, premium)
6. Download latest pipeline artifacts from S3
7. Download latest intraday PBA data from S3
8. Pivot VP data (local CPTs converted to UTC via station timezone, util columns, grid keys)
9. Join site list + VP + VOVI with `available_inputs` flag
10. Save `joined.csv` and `visual.json` to context directory

## Step 2: Display Results

After notebook execution, report:

1. **Target date** used
2. **Station breakdown** by `available_inputs` (vp_list, vp, list)
3. **VOVI match count** - how many stations matched VOVI data
4. **Output location** - path to context directory with `joined.csv`

## Step 3: Offer Next Steps

Ask if the user wants to:
- Refresh the dashboard with the new data
- View a sample of the joined data
- Check specific stations
- Compare to unpublished (forecast_setup) results

---

## Results Report

| Step | Status | Details |
|------|--------|---------|
| CT Metadata | PASS/FAIL | station count for timezone bucket |
| VOVI Fetch | PASS/FAIL | US + CA row counts |
| Pipeline Artifacts | PASS/FAIL | artifact count |
| PBA Download | PASS/FAIL | row count |
| VP Pivot | PASS/FAIL | station count |
| Joined Output | PASS/FAIL | total rows, station count |

---

## Example Usage

User: "run forecast review"
-> Ask for timezone bucket, then execute

User: "forecast review for Mountain"
-> Execute directly with tz_bucket=Mountain
