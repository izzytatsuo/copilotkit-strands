"""
Control Tower Station Metadata UDF

Fetches CT station metadata from static URL, saves to context directory,
and registers as DataFrame for DuckDB queries.

URL: https://na.prod.control-tower.last-mile.amazon.dev/api/rap-dal/artifacts/NA/pf_common/intraweek/station_metadata/1?scenario=POR&status=PUBLISHED&space=space%3Dww&time=time%3Dlatest&business=AMZL

Output directory: agent/data/contexts/{date_pacific}/{context_id}/ct_metadata/

Usage:
    # In ETL python() cell or standalone
    SELECT fetch_ct_metadata('ctx_abc123')

    # Then query the registered table
    SELECT * FROM ct_metadata WHERE country = 'US'
"""
import json
import subprocess
from pathlib import Path

# Module-level connection reference
_conn = None
COOKIE_PATH = str(Path.home() / ".midway" / "cookie")

CT_METADATA_URL = (
    "https://na.prod.control-tower.last-mile.amazon.dev/api/rap-dal/artifacts/"
    "NA/pf_common/intraweek/station_metadata/1"
    "?scenario=POR&status=PUBLISHED&space=space%3Dww&time=time%3Dlatest&business=AMZL"
)


def set_connection(conn):
    """Set the DuckDB connection for DataFrame registration."""
    global _conn
    _conn = conn


def set_cookie_path(path: str):
    """Set the cookie path for authentication."""
    global COOKIE_PATH
    COOKIE_PATH = path


def fetch_ct_metadata(context_id: str) -> str:
    """
    Fetch Control Tower station metadata and register as ct_metadata table.

    Args:
        context_id: Execution context ID (e.g., 20260109_143245_fcstsetup)

    Returns:
        JSON: {success, row_count, output_dir, output_file, error}

    Directory structure:
        agent/data/contexts/{context_id}/ct_metadata/
            +-- response.json      # Raw API response
            +-- stations.csv       # Parsed DataFrame
    """
    import pandas as pd
    global _conn

    # Build output directory (date is embedded in context_id)
    base_dir = Path(__file__).parent.parent.parent.parent.parent / "data" / "contexts"
    output_dir = base_dir / context_id / "ct_metadata"
    output_dir.mkdir(parents=True, exist_ok=True)

    meta = {
        "success": False,
        "row_count": 0,
        "output_dir": str(output_dir),
        "output_file": None,
        "error": None
    }

    try:
        # Fetch using curl with cookie
        result = subprocess.run(
            ['curl.exe', '--location-trusted', '-b', COOKIE_PATH, '-k', CT_METADATA_URL],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            meta["error"] = f"curl failed: {result.stderr}"
            return json.dumps(meta)

        # Save raw response
        raw_file = output_dir / "response.json"
        with open(raw_file, 'w') as f:
            f.write(result.stdout)

        # Parse response
        response = json.loads(result.stdout)

        # Extract nested data (artifact.data is a JSON string)
        artifact_data = response.get('artifact', {}).get('data', '[]')
        if isinstance(artifact_data, str):
            stations = json.loads(artifact_data)
        else:
            stations = artifact_data

        # Create DataFrame
        df = pd.DataFrame(stations)

        # Save as CSV
        csv_file = output_dir / "stations.csv"
        df.to_csv(csv_file, index=False)

        # Try to register with DuckDB (may fail if called from SQL context)
        if _conn is not None:
            try:
                _conn.register('ct_metadata', df)
                meta["registered"] = True
            except Exception:
                # "device or resource busy" - can't register while in SQL context
                # User can load CSV manually after UDF completes
                meta["registered"] = False
                meta["load_sql"] = f"CREATE TABLE ct_metadata AS SELECT * FROM '{csv_file}'"

        meta["success"] = True
        meta["row_count"] = len(df)
        meta["output_file"] = str(csv_file)
        meta["columns"] = list(df.columns)

    except Exception as e:
        meta["error"] = str(e)

    return json.dumps(meta)


# DuckDB registration metadata
name = "fetch_ct_metadata"
func = fetch_ct_metadata
parameters = [str]  # context_id
return_type = str
