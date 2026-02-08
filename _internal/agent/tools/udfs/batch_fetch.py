"""
Batch HTTP Fetch UDF - call batch_http from SQL

Maps to batch_http_request params:
    - url -> urls (comma-separated string -> List)
    - cookie -> cookie
    - output_dir -> output_dir
    - session_name -> session_name
    - method -> method

Usage:
    -- Basic fetch with defaults
    SELECT batch_fetch('https://api.example.com/data', '~/.midway/cookie', 'output/folder')

    -- With session name and method
    SELECT batch_fetch(
        'https://api1.com,https://api2.com',  -- comma-separated URLs
        '~/.midway/cookie',
        'agent/data/my_session',
        'My Analysis',  -- session_name
        'GET'           -- method
    )

    -- With execution context
    SELECT batch_fetch(
        url_list,
        '~/.midway/cookie',
        'agent/data/contexts/' || epoch_ts || '_' || ctx_id,
        'ETL Context ' || ctx_id,
        'GET'
    ) FROM exec_ctx
"""
import json
from pathlib import Path

# Module-level connection reference
_conn = None
COOKIE_PATH = str(Path.home() / ".midway" / "cookie")


def set_connection(conn):
    """Set the DuckDB connection for DataFrame registration."""
    global _conn
    _conn = conn


def set_cookie_path(path: str):
    """Set the default cookie path."""
    global COOKIE_PATH
    COOKIE_PATH = path


def batch_fetch(
    url: str,
    cookie: str,
    output_dir: str,
    session_name: str = "",
    method: str = "GET"
) -> str:
    """
    Fetch URL(s) using batch_http and save to output_dir.

    Maps to batch_http_request params:
        url -> urls (split by comma)
        cookie -> cookie
        output_dir -> output_dir
        session_name -> session_name
        method -> method

    Args:
        url: URL(s) to fetch - comma-separated for multiple
        cookie: Path to cookie file
        output_dir: Directory to save responses
        session_name: Optional session name for tracking
        method: HTTP method (default: GET)

    Returns:
        JSON: {success, session_id, output_dir, success_dir, files_count, failed}
    """
    from tools.batch_http import batch_http_request

    result = {
        "success": False,
        "session_id": None,
        "output_dir": output_dir,
        "success_dir": None,
        "files_count": 0,
        "failed": 0,
        "error": None
    }

    try:
        # Split comma-separated URLs
        urls = [u.strip() for u in url.split(',') if u.strip()]

        # Expand cookie path
        cookie_path = str(Path(cookie).expanduser()) if cookie else COOKIE_PATH

        # Call batch_http_request with mapped params
        response = batch_http_request(
            urls=urls,
            method=method,
            cookie=cookie_path,
            output_dir=output_dir,
            session_name=session_name if session_name else None,
            save_responses=True
        )

        # Map response to simple result
        result["success"] = response.get("successful", 0) > 0
        result["session_id"] = response.get("session_id")
        result["output_dir"] = response.get("session_dir", output_dir)
        result["success_dir"] = response.get("success_dir")
        result["files_count"] = response.get("successful", 0)
        result["failed"] = response.get("failed", 0)

    except Exception as e:
        result["error"] = str(e)

    return json.dumps(result)


# DuckDB registration metadata
name = "batch_fetch"
func = batch_fetch
parameters = [str, str, str, str, str]  # url, cookie, output_dir, session_name, method
return_type = str
