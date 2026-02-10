"""
Context ID Generator UDF

Generates ctx_id in format: YYYYMMDD_HHMMSS_taskname or YYYYMMDD_HHMMSS_uuid8
Timestamps are UTC so context directories sort chronologically and epoch parsing is straightforward.

Usage:
    SELECT generate_ctx_id('')              -- 20260109_214532_a3f2b1c9
    SELECT generate_ctx_id('fcstsetup')     -- 20260109_214532_fcstsetup
    SELECT generate_ctx_id('vovi_load')     -- 20260109_214532_vovi_load
"""
import uuid
from datetime import datetime
import pytz


def generate_ctx_id(task_name: str = "") -> str:
    """
    Generate a context ID for folder organization.

    Args:
        task_name: Optional task name suffix. If empty, uses 8-char uuid.

    Returns:
        ctx_id: Format YYYYMMDD_HHMMSS_suffix
            - With task_name: 20260109_143245_fcstsetup
            - Without: 20260109_143245_a3f2b1c9
    """
    # Get current time in UTC
    now = datetime.now(pytz.utc)
    timestamp = now.strftime('%Y%m%d_%H%M%S')

    # Use task_name or generate uuid suffix
    if task_name and task_name.strip():
        suffix = task_name.strip()
    else:
        suffix = str(uuid.uuid4())[:8]

    return f"{timestamp}_{suffix}"


# DuckDB registration metadata
name = "generate_ctx_id"
func = generate_ctx_id
parameters = [str]  # task_name (empty string for uuid)
return_type = str
