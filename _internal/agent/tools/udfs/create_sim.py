"""
Create SIM Ticket UDF

Wraps SIMData.create_sim() for use in ETL notebook Python cells.
Handles SSO authentication and custom fields formatting.

Usage in notebook Python cell:
    result = create_sim(
        title="My Ticket Title",
        description="Ticket description",
        folder_id="uuid-of-folder",
        tags="TAG1,TAG2",
        custom_fields='[{"id": "field_name", "value": "field_value"}]'
    )

    # result is JSON string with {status, sim_id, uuid, ...} or {status, error}
"""
import json
from typing import Optional

# Module-level SIMData instance (lazily initialized)
_sim_data = None


def _get_sim_data():
    """Get or create SIMData instance with connection pooling."""
    global _sim_data
    if _sim_data is None:
        from tools.sim_data import SIMData
        _sim_data = SIMData(debug=False)
    return _sim_data


def create_sim(
    title: str,
    description: str,
    folder_id: str,
    tags: Optional[str] = None,
    custom_fields: Optional[str] = None,
    requester: Optional[str] = None
) -> str:
    """
    Create a new SIM ticket in the specified folder.

    Args:
        title: Ticket title
        description: Ticket description
        folder_id: Target folder UUID
        tags: Optional comma-separated tag names (e.g., "TAG1,TAG2,TAG3")
        custom_fields: Optional JSON array of custom fields
                       Format: '[{"id": "field_name", "value": "field_value"}, ...]'
        requester: Optional requester kerberos username (defaults to current user)

    Returns:
        JSON string with result:
        - Success: {"status": "success", "sim_id": "V2047...", "uuid": "...", "title": "...", "folder_id": "..."}
        - Error: {"status": "error", "error": "Error message"}

    Example:
        # Simple ticket
        result = create_sim(
            title="PHX7 Manual Forecast",
            description="Forecast for 2026-01-15",
            folder_id="a4bf22db-9a8c-4e17-8780-92a7c68fb865"
        )

        # With tags and custom fields
        result = create_sim(
            title="PHX7 Manual Forecast | 19:30:00",
            description="AMZL forecast ticket",
            folder_id="a4bf22db-9a8c-4e17-8780-92a7c68fb865",
            tags="AMZL,MOUNTAIN,Manual-Forecast",
            custom_fields='[{"id": "station", "value": "PHX7"}, {"id": "cpt", "value": "19:30"}]'
        )
    """
    try:
        sim_data = _get_sim_data()

        # Call the SIMData.create_sim method
        # Note: SIMData.create_sim expects folder_id first, then title, description
        result = sim_data.create_sim(
            folder_id=folder_id,
            title=title,
            description=description,
            custom_fields=custom_fields,
            tags=tags,
            requester=requester
        )

        # SIMData.create_sim returns a Dict, convert to JSON string for UDF consistency
        return json.dumps(result)

    except Exception as e:
        return json.dumps({
            "status": "error",
            "error": str(e)
        })


# DuckDB registration metadata (for potential future SQL function registration)
name = "create_sim"
func = create_sim
parameters = [str, str, str, str, str, str]  # title, description, folder_id, tags, custom_fields, requester
return_type = str
