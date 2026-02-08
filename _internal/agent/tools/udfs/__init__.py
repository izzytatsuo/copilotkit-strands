"""
DuckDB UDF Collection for Agent ETL Tool

Each UDF file exports:
- name: str - function name for DuckDB
- func: callable - the Python function
- parameters: list - parameter types
- return_type: type - return type

Usage:
    from tools.udfs import register_all, set_cookie_path

    set_cookie_path("~/.midway/cookie")  # Optional: override default
    register_all(conn)  # Register all UDFs with DuckDB connection
"""

from . import fetch_vovi_forecast
from . import home_path
from . import batch_fetch
from . import fetch_ct_metadata
from . import generate_ctx_id
from . import check_mw_cookie
from . import fetch_vp_pipeline
from . import fetch_vovi_batch

# Collect all UDFs
ALL_UDFS = [
    {
        'name': fetch_vovi_forecast.name,  # 'create_vovi'
        'func': fetch_vovi_forecast.func,
        'parameters': fetch_vovi_forecast.parameters,
        'return_type': fetch_vovi_forecast.return_type,
        'module': fetch_vovi_forecast,
    },
    {
        'name': home_path.name,
        'func': home_path.func,
        'parameters': home_path.parameters,
        'return_type': home_path.return_type,
        'module': home_path,
    },
    {
        'name': batch_fetch.name,  # 'batch_fetch'
        'func': batch_fetch.func,
        'parameters': batch_fetch.parameters,
        'return_type': batch_fetch.return_type,
        'module': batch_fetch,
    },
    {
        'name': fetch_ct_metadata.name,  # 'fetch_ct_metadata'
        'func': fetch_ct_metadata.func,
        'parameters': fetch_ct_metadata.parameters,
        'return_type': fetch_ct_metadata.return_type,
        'module': fetch_ct_metadata,
    },
    {
        'name': generate_ctx_id.name,  # 'generate_ctx_id'
        'func': generate_ctx_id.func,
        'parameters': generate_ctx_id.parameters,
        'return_type': generate_ctx_id.return_type,
        'module': generate_ctx_id,
    },
    {
        'name': check_mw_cookie.name,  # 'check_mw_cookie'
        'func': check_mw_cookie.func,
        'parameters': check_mw_cookie.parameters,
        'return_type': check_mw_cookie.return_type,
        'module': check_mw_cookie,
    },
    {
        'name': fetch_vp_pipeline.name,  # 'fetch_vp_pipeline'
        'func': fetch_vp_pipeline.func,
        'parameters': fetch_vp_pipeline.parameters,
        'return_type': fetch_vp_pipeline.return_type,
        'module': fetch_vp_pipeline,
    },
    {
        'name': fetch_vovi_batch.name,  # 'fetch_vovi_batch'
        'func': fetch_vovi_batch.func,
        'parameters': fetch_vovi_batch.parameters,
        'return_type': fetch_vovi_batch.return_type,
        'module': fetch_vovi_batch,
    },
]


def set_cookie_path(path: str):
    """Set cookie path for all UDFs that need authentication."""
    for udf in ALL_UDFS:
        module = udf.get('module')
        if module and hasattr(module, 'set_cookie_path'):
            module.set_cookie_path(path)


def set_connection(conn):
    """Set DuckDB connection for all UDFs that register DataFrames."""
    for udf in ALL_UDFS:
        module = udf.get('module')
        if module and hasattr(module, 'set_connection'):
            module.set_connection(conn)


def register_all(conn, debug: bool = False):
    """
    Register all UDFs with a DuckDB connection.

    Args:
        conn: DuckDB connection
        debug: Print registration info

    Returns:
        List of registered function names
    """
    # Set connection for UDFs that need to register DataFrames
    set_connection(conn)

    registered = []
    for udf in ALL_UDFS:
        conn.create_function(
            udf['name'],
            udf['func'],
            udf['parameters'],
            udf['return_type']
        )
        registered.append(udf['name'])
        if debug:
            print(f"Registered UDF: {udf['name']}")

    return registered


def get_udf_info():
    """Get info about all available UDFs for documentation."""
    return [
        {
            'name': udf['name'],
            'parameters': [p.__name__ for p in udf['parameters']],
            'return_type': udf['return_type'].__name__,
        }
        for udf in ALL_UDFS
    ]
