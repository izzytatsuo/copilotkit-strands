# Forecast Setup Process

## Overview
**Purpose:** Pull VP/CT data, VOVI forecasts, pipeline artifacts, and intraday PBA, then join into a single dataset for forecast review.
**When to use:** When user says "run forecast setup", "forecast setup", or asks to prepare forecast data for review.
**Output:** `joined.csv` with site list + VP + VOVI outer join, plus pipeline artifacts and PBA data saved to context directory.

---

## Required Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `site_list_path` | **Yes** | Path to the site list Excel file |
| `ct_file_path` | **Yes** | Path to the VP/CT CSV file (UTC timestamps) |

**If either parameter is not provided, ask the user for the file path.**

---

## Step 1: Execute Notebook

Use the `run_notebook` tool:

```python
run_notebook("forecast_setup.ipynb", variables={"site_list_path": "{path}", "ct_file_path": "{path}"})
```

The notebook will:
1. Validate input files exist
2. Auto-calculate target date (tomorrow Pacific)
3. Load site list (AMZL stations) and VP raw data
4. Fetch VOVI forecasts (US + CA, AMZL, premium)
5. Download latest pipeline artifacts from S3
6. Download latest intraday PBA data from S3
7. Pivot VP data (long to wide with util columns and grid keys)
8. Join site list + VP + VOVI with `available_inputs` flag
9. Save `joined.csv` to context directory

## Step 2: Display Results

After notebook execution, report:

1. **Target date** used
2. **Station breakdown** by `available_inputs` (vp_list, vp, list)
3. **VOVI match count** - how many stations matched VOVI data
4. **Output location** - path to context directory with `joined.csv`

## Step 3: Offer Next Steps

Ask if the user wants to:
- View a sample of the joined data
- Check specific stations
- Run the PBA query

---

## Results Report

| Step | Status | Details |
|------|--------|---------|
| Input Validation | PASS/FAIL | site_list and ct_file exist |
| VOVI Fetch | PASS/FAIL | US + CA row counts |
| Pipeline Artifacts | PASS/FAIL | artifact count |
| PBA Download | PASS/FAIL | row count |
| VP Pivot | PASS/FAIL | station count |
| Joined Output | PASS/FAIL | total rows, station count |

---

## Example Usage

User: "run forecast setup"
-> Ask for site_list_path and ct_file_path, then execute

User: "forecast setup with this site list and CT file" (with file paths)
-> Execute directly with provided paths
