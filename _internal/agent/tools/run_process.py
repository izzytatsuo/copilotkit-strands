"""Run Process Tool - Execute predefined process workflows."""

from pathlib import Path
from strands import tool


@tool
def run_process(process_name: str):
    """Load and execute a predefined process from the processes folder.

    Processes are markdown files containing step-by-step instructions that
    the agent should follow. Use this when you need to run a standard
    workflow like connection_validator, forecast_review, etc.

    Args:
        process_name: Name of the process to run (without .md extension)

    Returns:
        Process instructions to follow, or error if process not found
    """
    # Get the agent directory (parent of tools)
    agent_dir = Path(__file__).parent.parent
    processes_dir = agent_dir / "processes"
    process_file = processes_dir / f"{process_name}.md"

    if not processes_dir.exists():
        return f"Error: Processes folder not found at {processes_dir}"

    if not process_file.exists():
        available = [f.stem for f in processes_dir.glob("*.md")]
        return f"Error: Process '{process_name}' not found. Available processes: {available}"

    try:
        instructions = process_file.read_text(encoding="utf-8")
        nl = chr(10)
        return f"=== PROCESS: {process_name} ==={nl}{nl}{instructions}{nl}{nl}=== Follow these instructions now. Report results when complete. ==="
    except Exception as e:
        return f"Error loading process '{process_name}': {e}"
