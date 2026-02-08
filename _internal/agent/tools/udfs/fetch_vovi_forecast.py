"""
VOVI Forecast UDF - creates vovi_df and vovi_meta DataFrames

Usage:
    # Setup
    from udfs import register_all
    conn = duckdb.connect()
    register_all(conn)

    # Call UDF - creates 'vovi' and 'vovi_meta' tables
    conn.sql("SELECT create_vovi('2026-01-09', 'US', 'amzl', 'premium')").fetchdf()

    # Check metadata
    conn.sql("SELECT * FROM vovi_meta").fetchdf()

    # Query data (if success)
    conn.sql("SELECT * FROM vovi WHERE station LIKE 'D%'").fetchdf()
"""
import subprocess
import json
import pandas as pd
from pathlib import Path

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


def _fetch_vovi(cpt_date: str, country: str, business_type: str, shipping_type: str):
    """Internal fetch - returns (data_df, meta_df)"""
    meta = {"success": False, "error": None, "row_count": 0}

    try:
        url = (
            f"https://prod.vovi.last-mile.amazon.dev/api/forecast/list_approved"
            f"?country={country}&cptDateKey={cpt_date}"
            f"&shippingType={shipping_type}&businessType={business_type}"
        )

        result = subprocess.run(
            ['curl.exe', '--location-trusted', '-b', COOKIE_PATH, url],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            meta["error"] = f"curl failed: {result.stderr}"
            return pd.DataFrame(), pd.DataFrame([meta])

        data = json.loads(result.stdout)
        df = pd.DataFrame(data)

        meta["success"] = True
        meta["row_count"] = len(df)
        return df, pd.DataFrame([meta])

    except Exception as e:
        meta["error"] = str(e)
        return pd.DataFrame(), pd.DataFrame([meta])


def create_vovi(cpt_date: str, country: str, business_type: str, shipping_type: str) -> str:
    """
    Fetch VOVI data and register as 'vovi' and 'vovi_meta' tables.

    Args:
        cpt_date: CPT date (YYYY-MM-DD)
        country: Country code (US)
        business_type: Business type (amzl)
        shipping_type: Shipping type (premium)

    Returns:
        JSON metadata: {success, error, row_count}
    """
    global _conn

    vovi_df, vovi_meta = _fetch_vovi(cpt_date, country, business_type, shipping_type)

    if _conn is not None:
        _conn.register('vovi', vovi_df)
        _conn.register('vovi_meta', vovi_meta)

    # Return metadata as JSON
    return vovi_meta.to_json(orient='records')


# DuckDB registration metadata
name = "create_vovi"
func = create_vovi
parameters = [str, str, str, str]  # cpt_date, country, business_type, shipping_type
return_type = str
