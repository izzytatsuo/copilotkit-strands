"""Strands AG-UI Integration Example - Proverbs Agent.

This example demonstrates a Strands agent integrated with AG-UI, featuring:
- Shared state management between agent and UI
- Backend tool execution (get_weather, update_proverbs)
- Frontend tools (set_theme_color)
- Generative UI rendering
"""

import json
import os
import sys
from pathlib import Path
from typing import List

# Fix Windows console encoding for Unicode characters (emojis like ✅)
# Use errors='replace' to substitute unsupported characters instead of crashing
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, 'reconfigure'):
    try:
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

import boto3
from ag_ui_strands import (
    StrandsAgent,
    StrandsAgentConfig,
    ToolBehavior,
    create_strands_app,
)
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from strands import Agent, tool
from strands.models.openai import OpenAIModel
from strands.models import BedrockModel

# ETL tool imports
from tools.duckdb_etl import DuckDBETL, _make_boto3_session
from tools.batch_http import batch_http_request, list_sessions, get_session_info
from tools.run_process import run_process
from tools.sim_data import SIMData

# Strands AWS tool — patch to use project-local credentials
import strands_tools.use_aws as _use_aws_mod
_orig_get_boto3_client = _use_aws_mod.get_boto3_client
def _patched_get_boto3_client(service_name, region_name, profile_name=None):
    from botocore.config import Config as BotocoreConfig
    session = _make_boto3_session(profile_name=profile_name)
    config = BotocoreConfig(user_agent_extra="strands-agents-use-aws")
    return session.client(service_name=service_name, region_name=region_name, config=config)
_use_aws_mod.get_boto3_client = _patched_get_boto3_client
from strands_tools import use_aws

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', 'env', '.env'))

# Model configuration - supports both OpenAI and Bedrock via environment variables
# Set MODEL_PROVIDER=bedrock to use AWS Bedrock, otherwise defaults to OpenAI
MODEL_PROVIDER = os.getenv("MODEL_PROVIDER", "openai").lower()


class ProverbsList(BaseModel):
    """A list of proverbs."""

    proverbs: List[str] = Field(description="The complete list of proverbs")


@tool
def get_weather(location: str):
    """Get the weather for a location.

    Args:
        location: The location to get weather for

    Returns:
        Weather information as JSON string
    """
    return json.dumps({"location": "70 degrees"})


@tool
def set_theme_color(theme_color: str):
    """Change the theme color of the UI.

    This is a frontend tool - it returns None as the actual
    execution happens on the frontend via useFrontendTool.

    Args:
        theme_color: The color to set as theme
    """
    return None


@tool
def update_layout(split: str = "horizontal", chart_pct: int = 45, grid_position: str = "bottom"):
    """Update the dashboard layout.

    This is a frontend tool - it returns None as the actual
    execution happens on the frontend via useFrontendTool.

    Args:
        split: Layout direction - 'horizontal' (chart top, grid bottom) or 'vertical' (chart right, grid left)
        chart_pct: Chart panel size as percentage (10-90), remainder goes to grid
        grid_position: Grid position - 'top' or 'bottom' (default bottom). Only applies to horizontal split.
    """
    return None


@tool
def refresh_dashboard():
    """Refresh the dashboard grid and chart data after a notebook run completes.

    This is a frontend tool - it returns None as the actual
    execution happens on the frontend via useFrontendTool.
    Call this after run_notebook or run_process completes to
    update the dashboard with the latest data.
    """
    return None


@tool
def update_proverbs(proverbs_list: ProverbsList):
    """Update the complete list of proverbs.

    IMPORTANT: Always provide the entire list, not just new proverbs.

    Args:
        proverbs_list: The complete updated proverbs list

    Returns:
        Success message
    """
    return "Proverbs updated successfully"


def build_proverbs_prompt(input_data, user_message: str) -> str:
    """Inject the current proverbs state into the prompt."""
    state_dict = getattr(input_data, "state", None)
    if isinstance(state_dict, dict) and "proverbs" in state_dict:
        proverbs_json = json.dumps(state_dict["proverbs"], indent=2)
        return (
            f"Current proverbs list:\n{proverbs_json}\n\nUser request: {user_message}"
        )
    return user_message


async def proverbs_state_from_args(context):
    """Extract proverbs state from tool arguments.

    This function is called when update_proverbs tool is executed
    to emit a state snapshot to the UI.

    Args:
        context: ToolResultContext containing tool execution details

    Returns:
        dict: State snapshot with proverbs array, or None on error
    """
    try:
        tool_input = context.tool_input
        if isinstance(tool_input, str):
            tool_input = json.loads(tool_input)

        proverbs_data = tool_input.get("proverbs_list", tool_input)

        # Extract proverbs array
        if isinstance(proverbs_data, dict):
            proverbs_array = proverbs_data.get("proverbs", [])
        else:
            proverbs_array = []

        return {"proverbs": proverbs_array}
    except Exception:
        return None


# Configure agent behavior
shared_state_config = StrandsAgentConfig(
    state_context_builder=build_proverbs_prompt,
    tool_behaviors={
        "update_proverbs": ToolBehavior(
            skip_messages_snapshot=True,
            state_from_args=proverbs_state_from_args,
        )
    },
)

# Initialize model based on MODEL_PROVIDER environment variable
if MODEL_PROVIDER == "bedrock":
    # Build boto3 session for Bedrock auth
    # Priority: env vars AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY > [bedrock] profile in project credentials > default chain
    _bedrock_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    _bedrock_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
    _bedrock_region = os.getenv("AWS_REGION", "us-east-1")

    if _bedrock_key and _bedrock_secret:
        _bedrock_session = boto3.Session(
            aws_access_key_id=_bedrock_key,
            aws_secret_access_key=_bedrock_secret,
            region_name=_bedrock_region,
        )
    else:
        # Try [bedrock] profile from project-local credentials
        from tools.duckdb_etl import _make_boto3_session
        try:
            _bedrock_session = _make_boto3_session(profile_name="bedrock")
        except Exception:
            # Fall back to default credential chain
            _bedrock_session = boto3.Session(region_name=_bedrock_region)

    model = BedrockModel(
        boto_session=_bedrock_session,
        model_id=os.getenv("BEDROCK_MODEL_ID", "us.anthropic.claude-haiku-4-5-20251001-v1:0"),
        max_tokens=int(os.getenv("BEDROCK_MAX_TOKENS", "8192")),
    )
else:
    # OpenAI - requires OPENAI_API_KEY
    api_key = os.getenv("OPENAI_API_KEY", "")
    model = OpenAIModel(
        client_args={"api_key": api_key},
        model_id=os.getenv("OPENAI_MODEL_ID", "gpt-4o"),
    )

def load_knowledge() -> str:
    """Load domain knowledge from markdown files in the knowledge folder."""
    knowledge_dir = Path(__file__).parent / "knowledge"
    if not knowledge_dir.exists():
        return ""
    knowledge_parts = []
    for md_file in sorted(knowledge_dir.glob("*.md")):
        try:
            content = md_file.read_text(encoding="utf-8")
            knowledge_parts.append(content)
        except Exception as e:
            print(f"Warning: Could not load {md_file}: {e}")
    return ("\n\n---\n\n").join(knowledge_parts)


DOMAIN_KNOWLEDGE = load_knowledge()

system_prompt = "You are a helpful and wise assistant that helps manage a collection of proverbs."
if DOMAIN_KNOWLEDGE:
    system_prompt += "\n\n" + "=" * 50 + "\nDOMAIN KNOWLEDGE\n" + "=" * 50 + "\n\n" + DOMAIN_KNOWLEDGE

# Initialize ETL tool providers
etl_provider = DuckDBETL(enable_s3=True, debug=False)
sim_provider = SIMData(debug=False)

# Create Strands agent with tools
# Note: Frontend tools (set_theme_color, refresh_dashboard) return None - actual execution happens in the UI
strands_agent = Agent(
    model=model,
    system_prompt=system_prompt,
    tools=[
        # Existing tools
        update_proverbs, get_weather, set_theme_color, update_layout, refresh_dashboard,
        # DuckDB ETL tools
        etl_provider.etl,
        etl_provider.run_notebook,
        etl_provider.list_notebooks,
        etl_provider.sql,
        etl_provider.python,
        etl_provider.connection_status,
        etl_provider.restart_connection,
        etl_provider.close_connection,
        # Batch HTTP tools
        batch_http_request, list_sessions, get_session_info,
        # Process runner
        run_process,
        # SIM tools
        sim_provider.search_sim,
        sim_provider.fetch_sim_by_ids,
        sim_provider.create_sim,
        sim_provider.check_sim_status,
        # AWS tool
        use_aws,
    ],
)

# Wrap with AG-UI integration
agui_agent = StrandsAgent(
    agent=strands_agent,
    name="strands_agent",
    description="A proverbs assistant that collaborates with you to manage proverbs",
    config=shared_state_config,
)

# Create the FastAPI app
agent_path = os.getenv("AGENT_PATH", "/")
app = create_strands_app(agui_agent, agent_path)

if __name__ == "__main__":
    import uvicorn

    port  = int(os.getenv("AGENT_PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
