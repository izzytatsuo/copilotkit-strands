"""Batch HTTP request tool with session tracking and JSON storage.

This tool fetches data from multiple URLs and saves results as JSON files
organized by session ID. Works seamlessly with the DuckDB ETL tool.
"""

from strands import tool
from typing import List, Dict, Any, Optional
import uuid
import json
from pathlib import Path
from datetime import datetime
import requests
import http.cookiejar
import os
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


# Session cache for connection pooling
SESSION_CACHE = {}


def create_session(config: Dict[str, Any]) -> requests.Session:
    """Create and configure a requests Session with connection pooling."""
    session = requests.Session()

    if config.get("keep_alive", True):
        adapter = HTTPAdapter(
            pool_connections=config.get("pool_size", 10),
            pool_maxsize=config.get("pool_size", 10),
            max_retries=Retry(
                total=config.get("max_retries", 3),
                backoff_factor=0.5,
                status_forcelist=[500, 502, 503, 504],
            ),
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

    return session


def get_cached_session(url: str, config: Dict[str, Any]) -> requests.Session:
    """Get or create a cached session for the domain."""
    domain = urlparse(url).netloc
    if domain not in SESSION_CACHE:
        SESSION_CACHE[domain] = create_session(config)
    return SESSION_CACHE[domain]


def load_cookies(session: requests.Session, cookie_path: str) -> bool:
    """Load cookies from file into session. Returns True if successful."""
    cookie_path = os.path.expanduser(cookie_path)

    if not os.path.exists(cookie_path):
        print(f"  [WARN] Cookie file not found: {cookie_path}")
        return False

    try:
        # Try Mozilla format first
        cookies = http.cookiejar.MozillaCookieJar()
        cookies.load(cookie_path, ignore_discard=True, ignore_expires=True)
        session.cookies.update(cookies)
        return True
    except Exception:
        try:
            # Try Netscape format (curl/Midway style)
            with open(cookie_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        parts = line.split("\t")
                        if len(parts) >= 7:
                            domain, flag, path, secure, expires, name, value = parts
                            session.cookies.set(name, value, domain=domain, path=path)
            return True
        except Exception as e:
            print(f"  [WARN] Failed to load cookies: {str(e)}")
            return False


@tool
def batch_http_request(
    urls: List[str],
    method: str = "GET",
    auth_type: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    cookie: Optional[str] = None,
    cookie_jar: Optional[str] = None,
    verify_ssl: bool = True,
    auth_env_var: Optional[str] = None,
    session_config: Optional[Dict[str, Any]] = None,
    metrics: bool = False,
    convert_to_markdown: bool = False,
    allow_redirects: bool = True,
    max_redirects: int = 30,
    session_id: Optional[str] = None,
    session_name: Optional[str] = None,
    save_responses: bool = True,
    output_dir: Optional[str] = None,
    max_workers: int = 2
) -> Dict[str, Any]:
    """Fetch data from multiple URLs with session tracking.

    Fetches URLs concurrently using ThreadPoolExecutor for faster execution.
    Saves each response as a separate JSON file for easy ETL processing.

    Args:
        urls: List of URLs to fetch
        method: HTTP method (default: GET)
        auth_type: Authentication type (negotiate, bearer, etc.)
        headers: Optional HTTP headers
        cookie: Path to cookie file (e.g., for Midway authentication)
        cookie_jar: Path to save cookies to
        verify_ssl: Whether to verify SSL certificates (default: True)
        auth_env_var: Environment variable name containing auth token
        session_config: Session configuration (keep_alive, max_retries, pool_size, etc.)
        metrics: Whether to collect request timing metrics
        convert_to_markdown: Convert HTML responses to markdown
        allow_redirects: Whether to follow redirects (default: True)
        max_redirects: Maximum number of redirects to follow (default: 30)
        session_id: Optional session ID (generates UUID if not provided)
        session_name: Optional friendly name for the session
        save_responses: If True, save responses as JSON files (default: True)
        output_dir: Custom output directory (default: agent/data/sessions/{session_id})
        max_workers: Maximum concurrent requests (default: 2)

    Returns:
        Dictionary with:
        - session_id: Session identifier
        - total_urls: Number of URLs fetched
        - successful: Number of successful fetches
        - failed: Number of failed fetches
        - output_files: List of JSON file paths (if saved)
        - session_dir: Root directory containing all session files
        - success_dir: Directory containing successful responses
        - errors_dir: Directory containing error responses
        - metadata_dir: Directory containing session metadata
        - batch_dir: Directory for batch processing outputs

    Examples:
        # Basic usage - auto-generates session
        result = batch_http_request(
            urls=["https://api.example.com/data1", "https://api.example.com/data2"],
            auth_type="negotiate"
        )

        # With Midway cookie (for Control Tower API)
        result = batch_http_request(
            urls=urls,
            cookie="C:/Users/admsia/.midway/cookie",
            session_name="Control Tower Analysis"
        )

        # Then use with ETL tool (reads from success/ directory):
        etl(
            sources=[
                {"name": "api_data", "path": result["success_dir"] + "/*.json", "format": "json"}
            ],
            transformations=[...],
            outputs=[...]
        )

        # Session directory structure:
        # agent/data/sessions/{session_id}/
        #   +-- success/          # Successful responses
        #   +-- errors/           # Error responses
        #   +-- metadata/         # Session metadata
        #   +-- batch/            # Batch processing outputs
    """
    # Generate session ID if not provided
    if not session_id:
        session_id = str(uuid.uuid4())

    # Setup output directory structure
    if output_dir:
        session_dir = Path(output_dir)
    else:
        base_dir = Path(__file__).parent.parent.parent.parent / "data" / "sessions"
        session_dir = base_dir / session_id

    # Create organized subdirectories
    session_dir.mkdir(parents=True, exist_ok=True)
    success_dir = session_dir / "success"
    errors_dir = session_dir / "errors"
    metadata_dir = session_dir / "metadata"
    batch_dir = session_dir / "batch"

    success_dir.mkdir(exist_ok=True)
    errors_dir.mkdir(exist_ok=True)
    metadata_dir.mkdir(exist_ok=True)
    batch_dir.mkdir(exist_ok=True)

    # Track results
    results = []
    successful = 0
    failed = 0
    output_files = []

    print(f"\n{'='*60}")
    print(f"Session ID: {session_id}")
    if session_name:
        print(f"Session Name: {session_name}")
    print(f"Fetching {len(urls)} URLs...")
    print(f"Output: {session_dir}")
    print(f"{'='*60}\n")

    # Setup session configuration
    if not session_config:
        session_config = {}

    # Get first URL to initialize session (assumes all URLs from same domain)
    first_url = urls[0] if urls else None
    if first_url:
        session = get_cached_session(first_url, session_config)

        # Load cookies if provided
        if cookie:
            load_cookies(session, cookie)

    # Thread-safe counters and lists
    lock = threading.Lock()
    successful_count = [0]  # Use list for mutable int in closure
    failed_count = [0]

    def fetch_single_url(i: int, url: str) -> Dict[str, Any]:
        """Fetch a single URL and return result data."""
        response_num = f"{i:04d}"
        result_data = None

        try:
            print(f"[{i}/{len(urls)}] Fetching: {url[:70]}...", flush=True)

            # Build request parameters
            request_kwargs = {
                "method": method,
                "url": url,
                "headers": headers or {},
                "verify": verify_ssl,
                "allow_redirects": allow_redirects,
            }

            # Make HTTP request
            response = session.request(**request_kwargs)
            response.raise_for_status()

            # Parse response data
            response_data = None
            try:
                response_data = response.json()
            except Exception:
                response_data = response.text

            result_data = {
                "url": url,
                "response_number": i,
                "success": True,
                "status_code": response.status_code,
                "fetched_at": datetime.now().isoformat(),
                "session_id": session_id,
                "content_type": response.headers.get("Content-Type", "unknown"),
                "data": response_data
            }

            with lock:
                successful_count[0] += 1
            print(f"  [{i}] OK (Status: {response.status_code})")

            # Save to file if requested
            if save_responses:
                output_file = success_dir / f"response_{response_num}.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result_data, f, indent=2)
                with lock:
                    output_files.append(str(output_file))

        except Exception as e:
            error_msg = str(e)
            with lock:
                failed_count[0] += 1
            print(f"  [{i}] ERR: {error_msg[:50]}")

            result_data = {
                "url": url,
                "response_number": i,
                "success": False,
                "fetched_at": datetime.now().isoformat(),
                "session_id": session_id,
                "error": error_msg
            }

            # Save error to file if requested
            if save_responses:
                output_file = errors_dir / f"response_{response_num}_error.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result_data, f, indent=2)
                with lock:
                    output_files.append(str(output_file))

        return result_data

    # Fetch URLs concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_single_url, i, url): i for i, url in enumerate(urls, 1)}

        for future in as_completed(futures):
            result_data = future.result()
            if result_data:
                with lock:
                    results.append(result_data)

    # Get final counts
    successful = successful_count[0]
    failed = failed_count[0]

    # Save session metadata
    metadata = {
        "session_id": session_id,
        "session_name": session_name,
        "created_at": datetime.now().isoformat(),
        "total_urls": len(urls),
        "successful": successful,
        "failed": failed,
        "urls": urls,
        "output_files": output_files
    }

    if save_responses:
        metadata_file = metadata_dir / "session_metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Complete: {successful} successful, {failed} failed")
    if save_responses:
        print(f"Saved to: {session_dir}")
        print(f"Total files: {len(output_files)}")
    print(f"{'='*60}\n")

    return {
        "session_id": session_id,
        "session_name": session_name,
        "total_urls": len(urls),
        "successful": successful,
        "failed": failed,
        "output_files": output_files if save_responses else [],
        "session_dir": str(session_dir),
        "success_dir": str(success_dir),
        "errors_dir": str(errors_dir),
        "metadata_dir": str(metadata_dir),
        "batch_dir": str(batch_dir),
        "results": results if not save_responses else None,  # Only return in-memory if not saving
        "etl_ready": save_responses  # Indicates data is ready for ETL tool
    }


@tool
def list_sessions(limit: int = 20) -> List[Dict[str, Any]]:
    """List all saved batch HTTP sessions.

    Args:
        limit: Maximum number of sessions to return (default: 20)

    Returns:
        List of session metadata dictionaries
    """
    base_dir = Path(__file__).parent.parent.parent.parent / "data" / "sessions"

    if not base_dir.exists():
        return []

    sessions = []

    for session_dir in sorted(base_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]:
        if not session_dir.is_dir():
            continue

        # Check new location first, then fall back to old location
        metadata_file = session_dir / "metadata" / "session_metadata.json"
        if not metadata_file.exists():
            metadata_file = session_dir / "session_metadata.json"

        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
                sessions.append(metadata)
        else:
            # Session exists but no metadata
            sessions.append({
                "session_id": session_dir.name,
                "session_dir": str(session_dir),
                "note": "No metadata file found"
            })

    return sessions


@tool
def get_session_info(session_id: str) -> Dict[str, Any]:
    """Get detailed info about a specific session.

    Args:
        session_id: Session ID (full UUID or first 8 characters)

    Returns:
        Session metadata and file list
    """
    base_dir = Path(__file__).parent.parent.parent.parent / "data" / "sessions"

    # Find session directory (support short IDs)
    session_dir = None
    if len(session_id) == 8:
        # Short ID - find matching session
        for d in base_dir.iterdir():
            if d.is_dir() and d.name.startswith(session_id):
                session_dir = d
                break
    else:
        # Full ID
        session_dir = base_dir / session_id

    if not session_dir or not session_dir.exists():
        return {"error": f"Session not found: {session_id}"}

    # Load metadata (check new location first, fall back to old)
    metadata_file = session_dir / "metadata" / "session_metadata.json"
    if not metadata_file.exists():
        metadata_file = session_dir / "session_metadata.json"

    if metadata_file.exists():
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
    else:
        metadata = {
            "session_id": session_dir.name,
            "note": "No metadata file found"
        }

    # List all files in session (from organized subdirectories)
    files = []

    # Check success/ directory
    success_dir = session_dir / "success"
    if success_dir.exists():
        for file_path in sorted(success_dir.glob("*.json")):
            files.append({
                "type": "success",
                "filename": file_path.name,
                "path": str(file_path),
                "size_kb": round(file_path.stat().st_size / 1024, 2)
            })

    # Check errors/ directory
    errors_dir = session_dir / "errors"
    if errors_dir.exists():
        for file_path in sorted(errors_dir.glob("*.json")):
            files.append({
                "type": "error",
                "filename": file_path.name,
                "path": str(file_path),
                "size_kb": round(file_path.stat().st_size / 1024, 2)
            })

    # Fall back to old flat structure if no organized directories
    if not files:
        for file_path in sorted(session_dir.glob("*.json")):
            if file_path.name != "session_metadata.json":
                files.append({
                    "type": "unknown",
                    "filename": file_path.name,
                    "path": str(file_path),
                    "size_kb": round(file_path.stat().st_size / 1024, 2)
                })

    metadata["files"] = files
    metadata["file_count"] = len(files)
    metadata["session_dir"] = str(session_dir)

    return metadata
