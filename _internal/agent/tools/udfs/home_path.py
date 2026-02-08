"""
Home Path UDF for DuckDB

Returns the user's home directory path.

Usage in SQL:
    SELECT home_path() as result;

Returns JSON: {"success": true/false, "result": "<path or error>"}
"""
from pathlib import Path
import json


def home_path() -> str:
    """
    Get the user's home directory path.

    Returns:
        JSON string: {"success": bool, "result": "<path or error>"}
    """
    try:
        path = str(Path.home())
        return json.dumps({"success": True, "result": path})
    except Exception as e:
        return json.dumps({"success": False, "result": str(e)})


# DuckDB registration metadata
name = "home_path"
func = home_path
parameters = []
return_type = str
