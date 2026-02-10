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

**If tz_bucket is not provided, ask the user which timezone bucket to run.**

---

## Step 1: Execute Notebook

Use the `run_notebook` tool:

```python
run_notebook("forecast_review.ipynb", variables={"tz_bucket": "{timezone}", "biz": "AMZL"})
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
