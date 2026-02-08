"""
VOVI Batch Forecast UDF - Fetch multiple VOVI URLs and save as separate CSVs

Fetches VOVI forecast data from multiple URLs, transforms each response to CSV
with params in filename for schema-safe loading.

Usage:
    SELECT fetch_vovi_batch('20260110_143245_vovi', 'url1,url2,url3')

Output structure:
    contexts/{ctx_id}/vovi_forecast/
    +-- success/                    # Raw JSON responses
    +-- errors/
    +-- metadata/
    +-- batch/                      # Transformed CSVs (one per URL)
    |   +-- 20260111_us_amzl_premium.csv
    |   +-- 20260111_us_amxl_premium.csv
    |   +-- 20260111_ca_amzl_premium.csv
    |   +-- ...
    +-- wildcard/
        +-- vovi_meta.csv           # Metadata for all responses

Filename format: {cptDateKey}_{country}_{businessType}_{shippingType}.csv
    - cptDateKey: date without dashes (20260111)
    - country: lowercase (us, ca)
    - businessType: lowercase (amzl, amxl)
    - shippingType: lowercase (premium)
"""
import json
import csv
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

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


def _parse_vovi_url_params(url: str) -> dict:
    """
    Extract params from VOVI URL for filename generation.

    Returns:
        {cptDateKey, country, businessType, shippingType} or empty dict on error
    """
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        return {
            'cptDateKey': params.get('cptDateKey', [''])[0],
            'country': params.get('country', [''])[0],
            'businessType': params.get('businessType', [''])[0],
            'shippingType': params.get('shippingType', [''])[0],
        }
    except Exception:
        return {}


def _generate_vovi_filename(params: dict) -> str:
    """
    Generate filename from URL params.

    Format: {cptDateKey}_{country}_{businessType}_{shippingType}.csv
    Example: 20260111_us_amzl_premium.csv
    """
    cpt_date = params.get('cptDateKey', '').replace('-', '')
    country = params.get('country', '').lower()
    business = params.get('businessType', '').lower()
    shipping = params.get('shippingType', '').lower()

    if all([cpt_date, country, business, shipping]):
        return f"{cpt_date}_{country}_{business}_{shipping}.csv"
    else:
        # Fallback to timestamp if params missing
        return f"vovi_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"


def _transform_vovi_response(response_data: dict, source_file: str) -> tuple:
    """
    Transform a single VOVI JSON response.

    Returns:
        (data_rows, meta_row, url_params)
    """
    data_rows = []
    meta_row = {
        'source_file': source_file,
        'url': '',
        'cpt_date_key': '',
        'country': '',
        'business_type': '',
        'shipping_type': '',
        'row_count': 0,
        'success': False
    }
    url_params = {}

    try:
        # Get URL and parse params
        url = response_data.get('url', '')
        meta_row['url'] = url
        url_params = _parse_vovi_url_params(url)

        meta_row['cpt_date_key'] = url_params.get('cptDateKey', '')
        meta_row['country'] = url_params.get('country', '')
        meta_row['business_type'] = url_params.get('businessType', '')
        meta_row['shipping_type'] = url_params.get('shippingType', '')

        # Get data array from response
        data = response_data.get('data', [])

        if isinstance(data, list):
            data_rows = data
            meta_row['row_count'] = len(data_rows)
            meta_row['success'] = True

    except Exception as e:
        meta_row['error'] = str(e)

    return data_rows, meta_row, url_params


def fetch_vovi_batch(ctx_id: str, urls: str, max_workers: int = 2) -> str:
    """
    Fetch VOVI forecast data from multiple URLs.

    Each response is saved as a separate CSV with params in filename.

    Args:
        ctx_id: Context ID (e.g., 20260110_143245_vovi)
        urls: Comma-separated URLs to fetch
        max_workers: Maximum concurrent requests (default: 2)

    Returns:
        JSON: {success, fetched, failed, files_created, output_dir, ...}
    """
    from tools.batch_http import batch_http_request
    global _conn

    # Record unload timestamp (UTC)
    unloaded_at = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # Build output directory
    base_dir = Path(__file__).parent.parent.parent / "data" / "contexts"
    output_dir = base_dir / ctx_id / "vovi_forecast"
    batch_dir = output_dir / "batch"
    wildcard_dir = output_dir / "wildcard"

    batch_dir.mkdir(parents=True, exist_ok=True)
    wildcard_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "success": False,
        "fetched": 0,
        "failed": 0,
        "files_created": [],
        "total_rows": 0,
        "output_dir": str(output_dir),
        "batch_dir": str(batch_dir),
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
            session_name=f"vovi_{ctx_id}",
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

        # Process each response file
        all_meta_rows = []
        response_files = sorted(success_dir.glob("response_*.json"))

        for json_file in response_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    response_data = json.load(f)

                data_rows, meta_row, url_params = _transform_vovi_response(
                    response_data, json_file.name
                )

                if data_rows:
                    # Generate filename from params
                    csv_filename = _generate_vovi_filename(url_params)
                    csv_file = batch_dir / csv_filename

                    # Get ALL unique fieldnames from all rows (schema may vary)
                    fieldnames = []
                    for row in data_rows:
                        for key in row.keys():
                            if key not in fieldnames:
                                fieldnames.append(key)

                    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                        writer.writeheader()
                        writer.writerows(data_rows)

                    result["files_created"].append(csv_filename)
                    result["total_rows"] += len(data_rows)
                    meta_row['csv_file'] = csv_filename

                meta_row['unloaded_at'] = unloaded_at
                all_meta_rows.append(meta_row)

            except Exception:
                continue  # Skip files that fail to parse

        # Write metadata file
        if all_meta_rows:
            meta_file = wildcard_dir / "vovi_meta.csv"
            meta_fieldnames = [
                'source_file', 'csv_file', 'url', 'cpt_date_key', 'country',
                'business_type', 'shipping_type', 'row_count', 'success', 'unloaded_at'
            ]

            with open(meta_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=meta_fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(all_meta_rows)

            result["meta_file"] = str(meta_file)

        result["success"] = len(result["files_created"]) > 0

    except Exception as e:
        result["error"] = str(e)

    return json.dumps(result)


# DuckDB registration metadata
name = "fetch_vovi_batch"
func = fetch_vovi_batch
parameters = [str, str, int]  # ctx_id, urls, max_workers
return_type = str
