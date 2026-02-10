"""
Volume Planner Pipeline UDF - Fetch and transform VP data to long format

Fetches VP pipeline data from Control Tower API, transforms to long format,
and registers as DuckDB table.

Supports both PUBLISHED (volume_plan) and UNPUBLISHED (pipeline_volume_plan) data.

Usage:
    # Fetch unpublished (default)
    SELECT fetch_vp_pipeline('ctx_id', 'url1,url2', 'unpublished')

    # Fetch published
    SELECT fetch_vp_pipeline('ctx_id', 'url1,url2', 'published')

Output structure:
    contexts/{ctx_id}/vp_unpublished/    # For status=unpublished
    contexts/{ctx_id}/vp_published/      # For status=published
    Each contains:
    +-- success/          # Raw JSON responses (separate per status!)
    +-- errors/
    +-- metadata/
    +-- batch/
    |   +-- vp_long_{status}.csv   # Primary - transformed data
    +-- wildcard/
        +-- vp_meta_{status}.csv   # Secondary - metadata

CPT format handling:
    - PUBLISHED uses time-only: "12:30:00"
    - UNPUBLISHED uses ISO timestamp: "2026-01-11T19:30:00.000Z"
    - Both are normalized to time-only in output
"""
import json
import csv
from pathlib import Path
from datetime import datetime, timezone

# Module-level connection reference
_conn = None
COOKIE_PATH = str(Path.home() / ".midway" / "cookie")


def set_connection(conn):
    """Set the DuckDB connection for DataFrame registration."""
    global _conn
    _conn = conn


def set_cookie_path(path: str):
    """Set the cookie path for authentication."""
    global COOKIE_PATH
    COOKIE_PATH = path


def _normalize_cpt(cpt_key: str) -> str:
    """
    Normalize CPT key to time-only format (HH:MM:SS).

    Handles both:
        - PUBLISHED: "12:30:00" -> "12:30:00"
        - UNPUBLISHED: "2026-01-11T19:30:00.000Z" -> "19:30:00"
    """
    if 'T' in cpt_key:
        # ISO format: extract time part
        try:
            time_part = cpt_key.split('T')[1].split('.')[0]  # "19:30:00"
            return time_part
        except (IndexError, ValueError):
            return cpt_key
    return cpt_key  # Already time-only


def _transform_vp_response(response_data: dict, source_file: str) -> tuple:
    """
    Transform a single VP JSON response to long format rows.

    Returns:
        (data_rows, meta_row) - data rows for vp_long, meta row for vp_meta
    """
    data_rows = []
    meta_row = {'source_file': source_file, 'recorded_at': '', 'created_at': '', 'status': ''}

    try:
        # Extract metadata from response
        artifact = response_data.get('data', {}).get('artifact', {})
        version = artifact.get('version', {})
        key = version.get('key', {})
        identifier = key.get('identifier', {})

        station = identifier.get('space', {}).get('value', '')
        plan_start_date = identifier.get('time', {}).get('value', '')
        status = identifier.get('status', '')  # PUBLISHED or UNPUBLISHED

        meta_row['status'] = status

        # Get recorded_at from version.key.recordedAt
        recorded_at = key.get('recordedAt', '')
        meta_row['recorded_at'] = recorded_at

        # Get created_at from version.metadata.authoring.createdAt
        version_metadata = version.get('metadata', {})
        authoring = version_metadata.get('authoring', {})
        created_at = authoring.get('createdAt', '')
        meta_row['created_at'] = created_at

        # Parse nested artifact data (may be string or dict)
        artifact_data_str = artifact.get('data', '{}')
        if isinstance(artifact_data_str, str):
            artifact_data = json.loads(artifact_data_str)
        else:
            artifact_data = artifact_data_str

        # Transform to long format
        ofd_dates = artifact_data.get('ofd_dates', {})
        for ofd_date, date_data in ofd_dates.items():
            demand_types = date_data.get('demand_types', {})
            for demand_type, demand_data in demand_types.items():
                cpts = demand_data.get('cpts', {})
                for cpt_key, cpt_data in cpts.items():
                    # Normalize CPT to time-only, then combine with ofd_date
                    # to produce full ISO timestamp matching file-loaded format
                    cpt_time = _normalize_cpt(cpt_key)
                    cpt_full = f"{ofd_date}T{cpt_time}.000Z"

                    for metric_name, metric_value in cpt_data.items():
                        row = {
                            'metric_name': metric_name,
                            'metric_value': metric_value,
                            'node': station,
                            'plan_start_date': plan_start_date,
                            'ofd_dates': ofd_date,
                            'demand_types': demand_type,
                            'cpts': cpt_full
                        }
                        data_rows.append(row)

    except Exception as e:
        # Return what we have on error, don't fail completely
        pass

    return data_rows, meta_row


def fetch_vp_pipeline(ctx_id: str, urls: str, status: str = "unpublished", max_workers: int = 2) -> str:
    """
    Fetch VP pipeline data and transform to long format.

    Args:
        ctx_id: Context ID (e.g., 20260109_143245_fcstsetup)
        urls: Comma-separated URLs to fetch
        status: Data status - "published" or "unpublished" (default: unpublished)
                Used in output filenames: vp_long_{status}.csv
        max_workers: Maximum concurrent requests (default: 2)

    Returns:
        JSON: {success, fetched, failed, rows_transformed, output_dir, csv_file, ...}
    """
    from tools.batch_http import batch_http_request
    import pandas as pd
    global _conn

    # Normalize status for filenames
    status = status.lower().strip()
    if status not in ('published', 'unpublished'):
        status = 'unpublished'

    # Record unload timestamp (UTC)
    unloaded_at = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # Build output directory - separate paths for published vs unpublished
    # This prevents raw JSON responses from overwriting each other
    base_dir = Path(__file__).parent.parent.parent.parent.parent / "data" / "contexts"
    output_dir = base_dir / ctx_id / f"vp_{status}"  # vp_published or vp_unpublished
    batch_dir = output_dir / "batch"
    wildcard_dir = output_dir / "wildcard"

    batch_dir.mkdir(parents=True, exist_ok=True)
    wildcard_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "success": False,
        "status": status,
        "fetched": 0,
        "failed": 0,
        "rows_transformed": 0,
        "output_dir": str(output_dir),
        "csv_file": None,
        "meta_file": None,
        "error": None
    }

    try:
        # Parse URLs
        url_list = [u.strip() for u in urls.split(',') if u.strip()]

        if not url_list:
            result["error"] = "No URLs provided"
            return json.dumps(result)

        # Fetch using batch_http
        fetch_result = batch_http_request(
            urls=url_list,
            cookie=COOKIE_PATH,
            output_dir=str(output_dir),
            session_name=f"vp_pipeline_{ctx_id}",
            verify_ssl=False,
            save_responses=True,
            max_workers=max_workers
        )

        result["fetched"] = fetch_result.get("successful", 0)
        result["failed"] = fetch_result.get("failed", 0)

        success_dir = Path(fetch_result.get("success_dir", output_dir / "success"))

        if not success_dir.exists():
            result["error"] = "No success directory created"
            return json.dumps(result)

        # Transform JSON responses to long format
        all_data_rows = []
        all_meta_rows = []

        response_files = sorted(success_dir.glob("response_*.json"))

        for json_file in response_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    response_data = json.load(f)

                data_rows, meta_row = _transform_vp_response(response_data, json_file.name)
                all_data_rows.extend(data_rows)

                if meta_row:
                    meta_row['unloaded_at'] = unloaded_at
                    all_meta_rows.append(meta_row)

            except Exception:
                continue  # Skip files that fail to parse

        # Write primary file (vp_long_{status}.csv) - REQUIRED
        if all_data_rows:
            csv_file = batch_dir / f"vp_long_{status}.csv"
            fieldnames = ['metric_name', 'metric_value', 'node', 'plan_start_date',
                          'ofd_dates', 'demand_types', 'cpts']

            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_data_rows)

            result["csv_file"] = str(csv_file)
            result["rows_transformed"] = len(all_data_rows)
            result["success"] = True

            # Try to register with DuckDB
            if _conn is not None:
                try:
                    df = pd.DataFrame(all_data_rows)
                    _conn.register('vp_pipeline', df)
                    result["registered"] = True
                except Exception:
                    result["registered"] = False
                    result["load_sql"] = f"CREATE TABLE vp_pipeline AS SELECT * FROM '{csv_file}'"

        # Write secondary file (vp_meta_{status}.csv) - OPTIONAL, best-effort
        try:
            if all_meta_rows:
                meta_file = wildcard_dir / f"vp_meta_{status}.csv"
                meta_fieldnames = ['source_file', 'recorded_at', 'created_at', 'status', 'unloaded_at']

                with open(meta_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=meta_fieldnames)
                    writer.writeheader()
                    writer.writerows(all_meta_rows)

                result["meta_file"] = str(meta_file)

                # Try to register meta table
                if _conn is not None:
                    try:
                        meta_df = pd.DataFrame(all_meta_rows)
                        _conn.register('vp_pipeline_meta', meta_df)
                        result["meta_registered"] = True
                    except Exception:
                        result["meta_registered"] = False
        except Exception:
            pass  # Don't fail if metadata fails

    except Exception as e:
        result["error"] = str(e)

    return json.dumps(result)


# DuckDB registration metadata
name = "fetch_vp_pipeline"
func = fetch_vp_pipeline
parameters = [str, str, str, int]  # ctx_id, urls, status, max_workers
return_type = str
