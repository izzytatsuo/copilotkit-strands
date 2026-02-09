from strands import tool, Agent
from typing import List, Dict, Any, Optional
import duckdb
import os
import subprocess
import boto3
from pathlib import Path

from botocore.session import Session as BotocoreSession

from tools.udfs import register_all as register_udfs, set_cookie_path as set_udf_cookie_path


def _get_aws_credentials_file() -> Optional[str]:
    """Resolve AWS credentials file path: env var > project-local > None (default chain)."""
    env_path = os.environ.get("AWS_CREDENTIALS_PATH")
    if env_path:
        p = Path(env_path).expanduser()
        if p.exists():
            return str(p)
    project_path = Path(__file__).parent.parent / "aws" / "credentials"
    if project_path.exists():
        return str(project_path)
    return None


def _make_boto3_session(profile_name: Optional[str] = None) -> boto3.Session:
    """Create a boto3 session using project-local credentials without polluting global env."""
    creds_file = _get_aws_credentials_file()
    if creds_file:
        # Use botocore to set credentials file on this session only
        botocore_session = BotocoreSession()
        botocore_session.set_config_variable("credentials_file", creds_file)
        if profile_name:
            botocore_session.set_config_variable("profile", profile_name)
        return boto3.Session(botocore_session=botocore_session)
    return boto3.Session(profile_name=profile_name)


class DuckDBETL:
    """
    ETL tool provider using DuckDB for data transformations.

    Provides tools for loading data from multiple sources (local/S3, various formats,
    in-memory DataFrames), running SQL transformations, and outputting results to
    multiple destinations.

    Features:
    - Connection pooling and management
    - S3 and local file support
    - Multiple file formats (CSV, Parquet, Excel, TXT, JSON)
    - In-memory DataFrame sources (zero-copy via Apache Arrow)
    - SQL transformations with DuckDB
    - Automatic type inference
    - Preview results
    - Connection health monitoring
    """

    def __init__(
        self,
        enable_s3: bool = True,
        debug: bool = False,
        default_preview_rows: int = 5
    ):
        """
        Initialize the DuckDB ETL tool provider.

        Args:
            enable_s3: Whether to enable S3 support with AWS extensions
            debug: Enable debug logging for troubleshooting
            default_preview_rows: Default number of rows to preview in results
        """
        self.enable_s3 = enable_s3
        self.debug = debug
        self.default_preview_rows = default_preview_rows
        self._conn = None
        self.cookie_path = os.path.expanduser(r"~\.midway\cookie")

    def is_connected(self) -> bool:
        """
        Check if the DuckDB connection is active and healthy.

        Returns:
            True if connection is active, False otherwise
        """
        if self._conn is None:
            return False

        try:
            # Try a simple query to verify connection is alive
            self._conn.execute("SELECT 1").fetchone()
            return True
        except Exception:
            return False

    def close(self):
        """Close the DuckDB connection and release resources."""
        if self._conn:
            try:
                self._conn.close()
                if self.debug:
                    print("DuckDB connection closed")
            except Exception as e:
                if self.debug:
                    print(f"Error closing connection: {e}")
            finally:
                self._conn = None

    def restart(self) -> bool:
        """
        Restart the DuckDB connection (close and reinitialize).

        Returns:
            True if restart successful, False otherwise
        """
        if self.debug:
            print("Restarting DuckDB connection...")

        self.close()
        self._conn = None

        # Trigger reconnection
        try:
            conn = self._get_connection()
            if self.debug:
                print("DuckDB connection restarted")
            return self.is_connected()
        except Exception as e:
            if self.debug:
                print(f"Failed to restart connection: {e}")
            return False

    def _register_udf_functions(self, conn: duckdb.DuckDBPyConnection):
        """
        Register Python UDF functions with DuckDB connection.

        UDFs are defined in tools/udfs/ directory. Each UDF is in its own file
        with metadata (name, func, parameters, return_type) for registration.
        """
        # Set cookie path for UDFs that need authentication
        set_udf_cookie_path(self.cookie_path)

        # Register all UDFs from the udfs package
        registered = register_udfs(conn, debug=self.debug)

        if self.debug:
            print(f"Registered {len(registered)} UDFs: {registered}")

    def _get_connection(self, aws_profile: Optional[str] = None) -> duckdb.DuckDBPyConnection:
        """
        Get or create DuckDB connection with extensions loaded.

        Automatically handles dead connections and recreates them.

        Args:
            aws_profile: Optional AWS profile name from ~/.aws/credentials to use for S3 operations

        Returns:
            Active DuckDB connection
        """
        # Check if existing connection is still valid
        if self._conn is not None:
            if not self.is_connected():
                if self.debug:
                    print("Existing connection is dead, creating new one...")
                self._conn = None

        if self._conn is None:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self._conn = duckdb.connect()

                    self._conn.execute("INSTALL excel; LOAD excel;")

                    if self.enable_s3:
                        self._conn.execute("INSTALL httpfs; LOAD httpfs;")
                        self._conn.execute("INSTALL aws; LOAD aws;")

                        # Configure S3 credentials based on profile
                        if aws_profile:
                            # Use specific AWS profile credentials
                            try:
                                session = _make_boto3_session(profile_name=aws_profile)
                                credentials = session.get_credentials()

                                if credentials:
                                    # Create secret with explicit credentials
                                    self._conn.execute(f"""
                                        CREATE OR REPLACE SECRET (
                                            TYPE s3,
                                            KEY_ID '{credentials.access_key}',
                                            SECRET '{credentials.secret_key}',
                                            REGION '{session.region_name or 'us-east-1'}'
                                        )
                                    """)

                                    if self.debug:
                                        print(f"DuckDB initialized with S3 support using profile '{aws_profile}' (attempt {attempt + 1})")
                                else:
                                    raise ValueError(f"No credentials found for profile '{aws_profile}'")
                            except Exception as e:
                                raise Exception(f"Failed to load AWS profile '{aws_profile}': {e}")
                        else:
                            # Use default credential chain (best-effort — may fail if no default AWS config)
                            try:
                                self._conn.execute("CREATE OR REPLACE SECRET (TYPE s3, PROVIDER credential_chain);")
                                if self.debug:
                                    print(f"DuckDB initialized with S3 support using credential chain (attempt {attempt + 1})")
                            except Exception:
                                # No default credentials available — S3 will work when aws_profile is specified per-query
                                if self.debug:
                                    print(f"DuckDB initialized without default S3 credentials (attempt {attempt + 1})")
                    else:
                        if self.debug:
                            print(f"DuckDB initialized (attempt {attempt + 1})")

                    # Register Python UDF functions
                    self._register_udf_functions(self._conn)
                    break

                except Exception as e:
                    if attempt == max_retries - 1:
                        raise Exception(f"Failed to initialize DuckDB after {max_retries} attempts: {e}")
                    if self.debug:
                        print(f"Connection attempt {attempt + 1} failed, retrying...")
                    import time
                    time.sleep(1)

        return self._conn

    def _set_s3_credentials(self, conn: duckdb.DuckDBPyConnection, aws_profile: Optional[str] = None):
        """
        Set or update S3 credentials in DuckDB connection.

        Args:
            conn: DuckDB connection
            aws_profile: Optional AWS profile name. If None, uses credential chain.
        """
        if not self.enable_s3:
            return

        if aws_profile:
            # Use specific AWS profile credentials
            try:
                session = _make_boto3_session(profile_name=aws_profile)
                credentials = session.get_credentials()

                if credentials:
                    # Recreate secret with explicit credentials
                    conn.execute(f"""
                        CREATE OR REPLACE SECRET (
                            TYPE s3,
                            KEY_ID '{credentials.access_key}',
                            SECRET '{credentials.secret_key}',
                            REGION '{session.region_name or 'us-east-1'}'
                        )
                    """)

                    if self.debug:
                        print(f"S3 credentials updated to profile '{aws_profile}'")
                else:
                    raise ValueError(f"No credentials found for profile '{aws_profile}'")
            except Exception as e:
                raise Exception(f"Failed to load AWS profile '{aws_profile}': {e}")
        else:
            # Use default credential chain
            conn.execute("CREATE OR REPLACE SECRET (TYPE s3, PROVIDER credential_chain);")

            if self.debug:
                print("S3 credentials updated to credential chain")

    def _check_file_exists(self, path: str, conn: duckdb.DuckDBPyConnection) -> bool:
        """
        Check if a file exists (local or S3).

        Args:
            path: File path (local or s3://)
            conn: DuckDB connection

        Returns:
            True if file exists, False otherwise
        """
        if path.startswith("s3://"):
            # For S3, try to read metadata
            try:
                conn.execute(f"SELECT * FROM '{path}' LIMIT 0")
                return True
            except:
                return False
        else:
            # For local files
            return os.path.exists(path)

    def _build_copy_command(
        self,
        source_name: str,
        output_path: str,
        output_format: str,
        output_opts: Dict[str, Any]
    ) -> str:
        """
        Build the COPY command based on format.

        Args:
            source_name: Name of the source table/view
            output_path: Destination path
            output_format: Output format (csv, parquet, txt, json)
            output_opts: Format-specific options

        Returns:
            SQL COPY command string
        """
        if output_format == "csv":
            delimiter = output_opts.get("delimiter", ",")
            header = output_opts.get("header", True)
            return f"""
                COPY (SELECT * FROM {source_name})
                TO '{output_path}'
                (HEADER {header}, DELIMITER '{delimiter}')
            """
        elif output_format == "parquet":
            return f"""
                COPY (SELECT * FROM {source_name})
                TO '{output_path}'
                (FORMAT PARQUET)
            """
        elif output_format == "txt":
            delimiter = output_opts.get("delimiter", "\t")
            header = output_opts.get("header", True)
            return f"""
                COPY (SELECT * FROM {source_name})
                TO '{output_path}'
                (HEADER {header}, DELIMITER '{delimiter}')
            """
        elif output_format == "json":
            return f"""
                COPY (SELECT * FROM {source_name})
                TO '{output_path}'
                (FORMAT JSON)
            """
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

    @tool
    def etl(
        self,
        sources: List[Dict[str, Any]],
        transformations: List[Dict[str, str]],
        outputs: List[Dict[str, Any]],
        options: Optional[Dict[str, Any]] = None,
        aws_profile: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute an ETL pipeline using DuckDB.

        Loads data from multiple sources (local/S3, CSV/Parquet/Excel/TXT),
        runs SQL transformations, and outputs results to local or S3 destinations.
        Files are overwritten by default.

        Args:
            sources: List of input data sources. Each source requires:
                - type: Source type - "file" (default), "dataframe", or "http"
                - name: Table name to reference in SQL queries

                For file sources (type="file" or omitted):
                    - path: File location (local path or s3://bucket/key)
                    - format: File format (csv, parquet, xlsx, txt)
                    - options: Optional format-specific settings
                        - delimiter: For CSV/TXT files (default: ',' for CSV, tab for TXT)
                        - header: Whether file has header row (default: True)
                        - sheet_name: For Excel files (default: 0)
                    - aws_profile: Optional AWS profile for this specific source (overrides etl-level profile)

                For DataFrame sources (type="dataframe"):
                    - dataframe: pandas DataFrame object to use as source
                    - Note: Uses zero-copy registration via Apache Arrow for efficiency

                For HTTP sources (type="http"):
                    - url: Full URL to fetch data from
                    - format: Response format - "json" (default) or "csv"
                    - cookie_file: Path to cookie file for auth (default: ~/.midway/cookie)

            transformations: List of cells to run sequentially. Each requires:
                - name: Output table name (can be referenced by later queries)
                - query: SQL query or Python code
                - type: Cell type - "sql" (default) or "python"

                For SQL cells (default):
                    - query: SQL query (can reference sources and previous transformations)
                    - Creates a view with the given name

                For Python cells (type="python"):
                    - query: Python code with access to conn, pd, json, Path
                    - Use conn.register('name', df) to create queryable tables
                    - Set result variable to return data in response
                    - UDFs available (call directly, no import needed):
                        * generate_ctx_id(prefix) - Generate unique context ID
                        * fetch_ct_metadata(ctx_id) - Fetch Control Tower station metadata
                        * fetch_vp_pipeline(ctx_id, urls, label, max_workers) - Batch fetch VP data
                        * fetch_vovi_batch(ctx_id, urls, max_workers) - Batch fetch VOVI data
                        * check_mw_cookie() - Check Midway cookie validity
                    - IMPORTANT: Do NOT create conn = duckdb.connect() - use the provided conn

            outputs: List of output destinations. Each requires:
                - source: Which transformation table to output
                - path: Output location (local path or s3://bucket/key)
                - format: Output format (csv, parquet, txt, json)
                - options: Optional format-specific settings
                    - delimiter: For CSV/TXT files
                    - header: Include header row (default: True)
                - overwrite: Allow overwriting existing files (default: True)
                - aws_profile: Optional AWS profile for this specific output (overrides etl-level profile)

            options: Optional global settings:
                - debug: Print debug information (overrides instance setting)
                - return_preview: Include data previews in results (default: True)
                - preview_rows: Number of rows to preview (default: 5)
                - error_on_empty: Fail if transformation returns no results (default: False)
                - allow_overwrite: Global overwrite setting (default: True)

            aws_profile: Optional AWS profile name from ~/.aws/credentials for S3 operations.
                This sets the default profile for all sources/outputs that don't specify their own.
                Precedence order: source/output-level > etl-level > credential chain (AWS_PROFILE/default).

        Returns:
            Dictionary with:
                - status: "success", "partial_success", or "failed"
                - sources_loaded: List of successfully loaded sources
                - transformations_run: List of completed transformations
                - outputs_created: List of files written
                - previews: Preview data for each transformation (if enabled)
                - errors: List of any errors encountered

        Examples:
            Basic usage:
                result = agent.tool.etl(
                    sources=[
                        {
                            "path": "s3://bucket/sales.csv",
                            "name": "sales",
                            "format": "csv"
                        }
                    ],
                    transformations=[
                        {
                            "name": "filtered",
                            "query": "SELECT * FROM sales WHERE amount > 100"
                        }
                    ],
                    outputs=[
                        {
                            "source": "filtered",
                            "path": "s3://bucket/output.csv",
                            "format": "csv"
                        }
                    ]
                )

            Multi-source join:
                result = agent.tool.etl(
                    sources=[
                        {"path": "s3://bucket/sales.csv", "name": "sales", "format": "csv"},
                        {"path": "local/customers.parquet", "name": "customers", "format": "parquet"}
                    ],
                    transformations=[
                        {
                            "name": "enriched",
                            "query": '''
                                SELECT s.*, c.customer_name
                                FROM sales s
                                JOIN customers c ON s.customer_id = c.id
                            '''
                        }
                    ],
                    outputs=[
                        {"source": "enriched", "path": "s3://bucket/enriched.parquet", "format": "parquet"}
                    ]
                )

            Using specific AWS profile (etl-level):
                result = agent.tool.etl(
                    sources=[
                        {"path": "s3://my-bucket/data.csv", "name": "data", "format": "csv"}
                    ],
                    transformations=[
                        {"name": "processed", "query": "SELECT * FROM data WHERE status = 'active'"}
                    ],
                    outputs=[
                        {"source": "processed", "path": "s3://output-bucket/result.csv", "format": "csv"}
                    ],
                    aws_profile="athena_vs_code"  # Use this profile for all S3 operations
                )

            In-memory DataFrame sources:
                result = agent.tool.etl(
                    sources=[
                        {
                            "type": "dataframe",
                            "name": "insite",
                            "dataframe": insite_result["dataframe"]
                        }
                    ],
                    transformations=[
                        {
                            "name": "merged",
                            "query": "SELECT * FROM insite"
                        }
                    ],
                    outputs=[
                        {"source": "merged", "path": "output/result.csv", "format": "csv"}
                    ]
                )

            HTTP source (fetch API data directly):
                result = agent.tool.etl(
                    sources=[
                        {
                            "type": "http",
                            "name": "forecast",
                            "url": "https://prod.vovi.last-mile.amazon.dev/api/forecast/list_approved?country=US&cptDateKey=2026-01-05&shippingType=premium&businessType=amzl",
                            "format": "json"
                        }
                    ],
                    transformations=[
                        {
                            "name": "filtered",
                            "query": "SELECT * FROM forecast WHERE station_code LIKE 'D%'"
                        }
                    ],
                    outputs=[
                        {"source": "filtered", "path": "forecast_filtered.csv", "format": "csv"}
                    ]
                )

            Using UDFs in Python cells (recommended for complex workflows):
                result = agent.tool.etl(
                    sources=[],
                    transformations=[
                        {
                            "name": "ctx_setup",
                            "type": "python",
                            "query": '''
ctx_id = generate_ctx_id('my_etl')
result = {'ctx_id': ctx_id}
'''
                        }
                    ],
                    outputs=[]
                )
        """
        # Merge options with defaults
        opts = {
            "debug": self.debug,
            "return_preview": True,
            "preview_rows": self.default_preview_rows,
            "error_on_empty": False,
            "allow_overwrite": True
        }
        if options:
            opts.update(options)

        result = {
            "status": "success",
            "sources_loaded": [],
            "transformations_run": [],
            "outputs_created": [],
            "previews": {},
            "errors": []
        }

        try:
            conn = self._get_connection(aws_profile=aws_profile)

            # EXTRACT: Load all data sources
            for source in sources:
                try:
                    name = source["name"]
                    source_type = source.get("type", "file")

                    # Handle DataFrame sources (in-memory)
                    if source_type == "dataframe":
                        df = source.get("dataframe")
                        if df is None:
                            raise ValueError(f"Source '{name}' has type='dataframe' but no 'dataframe' field provided")

                        # Register DataFrame with DuckDB (zero-copy via Apache Arrow)
                        conn.register(name, df)

                        # Get row count
                        row_count = len(df)

                        result["sources_loaded"].append({
                            "name": name,
                            "type": "dataframe",
                            "rows": row_count,
                            "columns": len(df.columns)
                        })

                        if opts["debug"]:
                            print(f"Registered DataFrame '{name}': {row_count:,} rows, {len(df.columns)} columns")

                    # Handle HTTP sources (fetch via curl)
                    elif source_type == "http":
                        url = source.get("url")
                        if url is None:
                            raise ValueError(f"Source '{name}' has type='http' but no 'url' field provided")

                        cookie_file = source.get("cookie_file", self.cookie_path)
                        cookie_file = os.path.expanduser(cookie_file)
                        fmt = source.get("format", "json").lower()

                        # Fetch data via curl
                        curl_result = subprocess.run(
                            ['curl.exe', '--location-trusted', '-b', cookie_file, url],
                            capture_output=True,
                            text=True
                        )

                        if curl_result.returncode != 0:
                            raise Exception(f"curl failed for '{name}': {curl_result.stderr}")

                        # Parse based on format and create view
                        if fmt == "json":
                            # Use read_json_auto with the fetched data
                            conn.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_json_auto('{curl_result.stdout}')")
                        elif fmt == "csv":
                            conn.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_csv_auto('{curl_result.stdout}')")
                        else:
                            raise ValueError(f"Unsupported HTTP format: {fmt}. Use 'json' or 'csv'")

                        # Get row count
                        row_count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]

                        result["sources_loaded"].append({
                            "name": name,
                            "type": "http",
                            "url": url,
                            "format": fmt,
                            "rows": row_count
                        })

                        if opts["debug"]:
                            print(f"Fetched HTTP '{name}': {row_count:,} rows from {url}")

                    # Handle file sources (local or S3)
                    elif source_type == "file":
                        path = source["path"]
                        fmt = source.get("format", "csv").lower()
                        source_opts = source.get("options", {})
                        source_aws_profile = source.get("aws_profile")

                        # Set S3 credentials if source specifies a profile (precedence: source > etl > default)
                        if path.startswith("s3://"):
                            effective_profile = source_aws_profile if source_aws_profile is not None else aws_profile
                            if effective_profile is not None or source_aws_profile is not None:
                                self._set_s3_credentials(conn, effective_profile)

                        # Build read command based on format
                        if fmt == "csv":
                            read_cmd = f"read_csv_auto('{path}')"
                        elif fmt == "parquet":
                            read_cmd = f"read_parquet('{path}')"
                        elif fmt in ["xlsx", "excel"]:
                            sheet = source_opts.get("sheet_name", 0)
                            read_cmd = f"read_excel('{path}', sheet_name='{sheet}')"
                        elif fmt == "txt":
                            delimiter = source_opts.get("delimiter", "\t")
                            read_cmd = f"read_csv_auto('{path}', delim='{delimiter}')"
                        else:
                            raise ValueError(f"Unsupported format: {fmt}")

                        # Create view from source
                        conn.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM {read_cmd}")

                        # Get row count
                        row_count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]

                        result["sources_loaded"].append({
                            "name": name,
                            "type": "file",
                            "path": path,
                            "format": fmt,
                            "rows": row_count
                        })

                        if opts["debug"]:
                            print(f"Loaded {name}: {row_count:,} rows from {path}")

                    else:
                        raise ValueError(f"Unknown source type: {source_type}. Use 'file', 'dataframe', or 'http'")

                except Exception as e:
                    error_msg = f"Failed to load source '{source.get('name', 'unknown')}': {str(e)}"
                    result["errors"].append(error_msg)
                    if opts["debug"]:
                        print(f"ERROR: {error_msg}")

            # TRANSFORM: Run all queries (SQL or Python cells)
            # Shared namespace for Python cells (persists across cells like a notebook)
            import pandas as _pd
            import json as _json
            from pathlib import Path as _Path

            # Import UDFs for direct Python access
            from tools.udfs.generate_ctx_id import generate_ctx_id
            from tools.udfs.fetch_ct_metadata import fetch_ct_metadata
            from tools.udfs.fetch_vp_pipeline import fetch_vp_pipeline
            from tools.udfs.fetch_vovi_batch import fetch_vovi_batch
            from tools.udfs.check_mw_cookie import check_mw_cookie
            from tools.udfs.create_sim import create_sim

            # Base contexts directory for notebook outputs
            _contexts_dir = _Path(__file__).parent.parent.parent.parent / "data" / "contexts"

            _py_namespace = {
                'conn': conn,
                'pd': _pd,
                'json': _json,
                'Path': _Path,
                'result': None,
                'contexts_dir': _contexts_dir,  # Base path for notebook outputs
                'make_boto3_session': _make_boto3_session,  # Project-aware boto3 session factory
                # UDFs available as direct Python calls
                'generate_ctx_id': generate_ctx_id,
                'fetch_ct_metadata': fetch_ct_metadata,
                'fetch_vp_pipeline': fetch_vp_pipeline,
                'fetch_vovi_batch': fetch_vovi_batch,
                'check_mw_cookie': check_mw_cookie,
                'create_sim': create_sim,
            }

            for i, transform in enumerate(transformations):
                try:
                    name = transform["name"]
                    query = transform["query"]
                    cell_type = transform.get("type", "sql").lower()

                    if cell_type == "python":
                        # Execute Python code - namespace persists across cells
                        _py_namespace['result'] = None  # Reset result for each cell
                        exec(query, _py_namespace)
                        namespace = _py_namespace  # For result access below

                        # Check if the named table was registered
                        try:
                            row_count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                        except:
                            row_count = 0  # Table may not have been created

                        result["transformations_run"].append({
                            "name": name,
                            "type": "python",
                            "rows": row_count,
                            "order": i,
                            "result": namespace.get('result')
                        })

                        # Get preview if table exists and requested
                        if opts["return_preview"] and row_count > 0:
                            preview_df = conn.execute(
                                f"SELECT * FROM {name} LIMIT {opts['preview_rows']}"
                            ).fetchdf()
                            result["previews"][name] = preview_df.to_dict('records')

                        if opts["debug"]:
                            print(f"Python cell '{name}': {row_count:,} rows")

                    else:
                        # SQL cell (default)
                        # Strip leading comment line (-- name: ... | type: sql)
                        import re as _re
                        clean_query = _re.sub(r'^--\s*name:.*\n?', '', query, count=1).strip()

                        # Substitute Python variables into SQL (for _vars-style references)
                        # Replace bare identifiers that match Python namespace string vars
                        for _var_name, _var_val in _py_namespace.items():
                            if isinstance(_var_val, str) and _var_name in clean_query:
                                # Normalize backslashes to forward slashes for DuckDB SQL on Windows
                                _safe_val = _var_val.replace('\\', '/')
                                # Only replace if it appears as a bare identifier (not inside quotes)
                                clean_query = _re.sub(
                                    r'\b' + _re.escape(_var_name) + r'\b',
                                    f"'{_safe_val}'",
                                    clean_query
                                )

                        # If cell already has CREATE/INSERT/etc, execute directly
                        _table_name = name
                        if _re.match(r'(?i)^(SET|CREATE|INSERT|DROP|ALTER|UPDATE|DELETE)', clean_query):
                            conn.execute(clean_query)
                            # Extract actual table/view name from CREATE statement
                            _create_match = _re.search(
                                r'(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+(\w+)',
                                clean_query
                            )
                            if _create_match:
                                _table_name = _create_match.group(1)
                        else:
                            conn.execute(f"CREATE OR REPLACE VIEW {name} AS {clean_query}")

                        # Get row count
                        row_count = conn.execute(f"SELECT COUNT(*) FROM {_table_name}").fetchone()[0]

                        result["transformations_run"].append({
                            "name": name,
                            "type": "sql",
                            "rows": row_count,
                            "order": i
                        })

                        # Get preview if requested
                        if opts["return_preview"]:
                            preview_df = conn.execute(
                                f"SELECT * FROM {name} LIMIT {opts['preview_rows']}"
                            ).fetchdf()
                            result["previews"][name] = preview_df.to_dict('records')

                        if opts["debug"]:
                            print(f"SQL cell '{name}': {row_count:,} rows")

                    # Check for empty results
                    if opts["error_on_empty"] and row_count == 0:
                        raise ValueError(f"Transformation '{name}' produced no results")

                except Exception as e:
                    error_msg = f"Failed transformation '{transform.get('name', f'step_{i}')}': {str(e)}"
                    result["errors"].append(error_msg)
                    if opts["debug"]:
                        print(f"ERROR: {error_msg}")

            # LOAD: Write outputs
            for output in outputs:
                try:
                    source_name = output["source"]
                    output_path = output["path"]
                    output_format = output.get("format", "csv").lower()
                    output_opts = output.get("options", {})
                    output_aws_profile = output.get("aws_profile")

                    # Set S3 credentials if output specifies a profile (precedence: output > etl > default)
                    if output_path.startswith("s3://"):
                        effective_profile = output_aws_profile if output_aws_profile is not None else aws_profile
                        if effective_profile is not None or output_aws_profile is not None:
                            self._set_s3_credentials(conn, effective_profile)

                    # Check overwrite settings
                    allow_overwrite = output.get("overwrite", opts["allow_overwrite"])

                    # Handle overwrite protection
                    if not allow_overwrite:
                        file_exists = self._check_file_exists(output_path, conn)
                        if file_exists:
                            raise ValueError(
                                f"File already exists at {output_path} and overwrite=False"
                            )

                    # Build and execute COPY command
                    copy_cmd = self._build_copy_command(
                        source_name, output_path, output_format, output_opts
                    )
                    conn.execute(copy_cmd)

                    # Get row count
                    row_count = conn.execute(f"SELECT COUNT(*) FROM {source_name}").fetchone()[0]

                    result["outputs_created"].append({
                        "source": source_name,
                        "path": output_path,
                        "format": output_format,
                        "rows": row_count
                    })

                    if opts["debug"]:
                        print(f"Output '{source_name}' -> {output_path} ({row_count:,} rows)")

                except Exception as e:
                    error_msg = f"Failed output '{output.get('source', 'unknown')}': {str(e)}"
                    result["errors"].append(error_msg)
                    if opts["debug"]:
                        print(f"ERROR: {error_msg}")

            # Set final status
            if result["errors"]:
                result["status"] = "partial_success" if result["outputs_created"] else "failed"

        except Exception as e:
            result["status"] = "failed"
            result["errors"].append(f"Fatal error: {str(e)}")
            if opts.get("debug"):
                import traceback
                traceback.print_exc()

        return result

    @tool
    def connection_status(self) -> Dict[str, Any]:
        """
        Check the status of the DuckDB connection.

        Returns connection state, version, and configuration information.
        Useful for troubleshooting connection issues.

        Returns:
            Dictionary with:
                - connected: Whether connection is active
                - s3_enabled: Whether S3 support is enabled
                - debug_mode: Whether debug logging is enabled
                - version: DuckDB version (if connected)
                - message: Human-readable status message
        """
        status = {
            "connected": self.is_connected(),
            "s3_enabled": self.enable_s3,
            "debug_mode": self.debug
        }

        if status["connected"]:
            try:
                conn = self._get_connection()
                version = conn.execute("SELECT version()").fetchone()[0]
                status["version"] = version
                status["message"] = "Connection is active and healthy"
            except Exception as e:
                status["connected"] = False
                status["error"] = str(e)
                status["message"] = f"Connection error: {str(e)}"
        else:
            status["message"] = "No active connection"

        return status

    @tool
    def restart_connection(self) -> Dict[str, Any]:
        """
        Restart the DuckDB connection.

        Closes the existing connection and creates a new one.
        Useful after credential changes or if connection becomes stale.

        Returns:
            Dictionary with:
                - status: "success" or "failed"
                - message: Result description
                - connected: Current connection state after restart
        """
        try:
            success = self.restart()
            return {
                "status": "success" if success else "failed",
                "message": "Connection restarted successfully" if success else "Failed to restart connection",
                "connected": self.is_connected()
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error restarting connection: {str(e)}",
                "connected": False
            }

    @tool
    def close_connection(self) -> Dict[str, Any]:
        """
        Close the DuckDB connection and release resources.

        A new connection will be created automatically on the next ETL operation.
        Useful for cleaning up resources or forcing credential refresh.

        Returns:
            Dictionary with:
                - status: "success" or "error"
                - message: Result description
                - was_connected: Whether a connection existed before closing
        """
        try:
            was_connected = self.is_connected()
            self.close()
            return {
                "status": "success",
                "message": "Connection closed successfully" if was_connected else "No connection to close",
                "was_connected": was_connected
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error closing connection: {str(e)}"
            }

    @tool
    def sql(self, query: str, preview_rows: int = 10) -> Dict[str, Any]:
        """
        Execute a SQL query and return results.

        Use this for quick queries on registered tables or loaded data.

        Args:
            query: SQL query to execute
            preview_rows: Max rows to return (default 10)

        Returns:
            Dictionary with status, row_count, columns, and data preview

        Examples:
            sql("SELECT * FROM my_table WHERE x > 10")
            sql("SELECT COUNT(*) FROM vovi")
        """
        try:
            conn = self._get_connection()
            result = conn.execute(query).fetchdf()

            return {
                "status": "success",
                "row_count": len(result),
                "columns": list(result.columns),
                "data": result.head(preview_rows).to_dict('records')
            }

        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }

    def _parse_cell_metadata(self, source: str, cell_meta: dict) -> tuple:
        """
        Extract name and type from a notebook cell.

        Looks for metadata in:
        1. Cell metadata dict (cell_meta.name, cell_meta.type)
        2. First line comment patterns:
           - Python: # name: foo | type: python
           - SQL: -- name: bar | type: sql

        Args:
            source: Cell source code
            cell_meta: Cell metadata dict from notebook

        Returns:
            Tuple of (name, type) or (None, None) if not found
        """
        import re

        # Check cell metadata first
        if 'name' in cell_meta:
            return cell_meta.get('name'), cell_meta.get('type', 'python')

        # Parse from first line
        lines = source.split('\n')
        if not lines:
            return None, None

        first_line = lines[0].strip()

        # Python comment: # name: xxx | type: python
        py_match = re.match(r'^#\s*name:\s*(\w+)\s*\|\s*type:\s*(\w+)', first_line)
        if py_match:
            return py_match.group(1), py_match.group(2)

        # SQL comment: -- name: xxx | type: sql
        sql_match = re.match(r'^--\s*name:\s*(\w+)\s*\|\s*type:\s*(\w+)', first_line)
        if sql_match:
            return sql_match.group(1), sql_match.group(2)

        return None, None

    def _parse_notebook(self, notebook_path: str) -> Dict[str, Any]:
        """
        Parse a Jupyter notebook and extract ETL configuration.

        Reads notebook JSON, extracts ETL metadata from notebook.metadata.etl,
        and parses code cells to build transformation list.

        Args:
            notebook_path: Path to .ipynb file

        Returns:
            Dictionary with:
                - metadata: ETL metadata from notebook (sources, outputs, options, etc.)
                - transformations: List of {name, query, type} for each code cell
                - path: Notebook path
                - cell_count: Total code cells processed
        """
        import json

        with open(notebook_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)

        # Get ETL metadata from notebook
        etl_meta = nb.get('metadata', {}).get('etl', {})
        transformations = []

        for cell in nb.get('cells', []):
            # Skip non-code cells (markdown, raw)
            if cell.get('cell_type') != 'code':
                continue

            # Get cell source (may be list or string)
            source = cell.get('source', [])
            if isinstance(source, list):
                source = ''.join(source)

            if not source.strip():
                continue

            # Skip setup cells that create new connections (shadows ETL conn)
            if 'duckdb.connect()' in source:
                continue

            # Extract name and type from cell
            name, cell_type = self._parse_cell_metadata(source, cell.get('metadata', {}))

            # Only include cells with names (these are ETL steps)
            if name:
                transformations.append({
                    'name': name,
                    'query': source,
                    'type': cell_type or 'python'
                })

        return {
            'metadata': etl_meta,
            'transformations': transformations,
            'path': notebook_path,
            'cell_count': len(transformations)
        }

    @tool
    def list_notebooks(self, folder_path: str = None) -> Dict[str, Any]:
        """
        List available ETL notebooks with their metadata.

        Scans a folder for .ipynb files and extracts ETL configuration from each.
        Use this to discover available notebooks before running them.

        Args:
            folder_path: Path to notebooks folder. Defaults to agent/notebooks

        Returns:
            Dictionary with:
                - status: "success" or "error"
                - notebooks: List of notebook info with name, description, path, etl config
                - count: Number of notebooks found

        Example:
            list_notebooks()  # Uses default folder
            list_notebooks("C:/my/notebooks")
        """
        import os
        import json
        from pathlib import Path

        try:
            # Default to agent/notebooks folder
            if folder_path is None:
                # Find agent/notebooks relative to this file
                this_file = Path(__file__).resolve()
                folder_path = this_file.parent.parent / 'notebooks'
            else:
                folder_path = Path(folder_path)

            if not folder_path.exists():
                return {
                    "status": "error",
                    "error": f"Folder not found: {folder_path}",
                    "notebooks": [],
                    "count": 0
                }

            notebooks = []
            for nb_file in folder_path.glob("*.ipynb"):
                try:
                    with open(nb_file, 'r', encoding='utf-8') as f:
                        nb_data = json.load(f)

                    etl_meta = nb_data.get('metadata', {}).get('etl', {})

                    # Parse typed variable schema and extract default values
                    variables_schema = etl_meta.get('variables', {})
                    variable_values = {}

                    for var_name, var_def in variables_schema.items():
                        # Handle both new typed format (dict with 'type') and legacy format (direct value)
                        if isinstance(var_def, dict) and 'type' in var_def:
                            # New typed format: { "type": "select", "default": "Eastern", ... }
                            variable_values[var_name] = var_def.get('default', '')
                        else:
                            # Legacy format: direct value (backward compat, will be removed after migration)
                            variable_values[var_name] = var_def
                            # Convert to typed schema format
                            variables_schema[var_name] = {
                                "type": "text",
                                "label": var_name,
                                "default": var_def
                            }

                    notebooks.append({
                        "path": str(nb_file),
                        "filename": nb_file.name,
                        "name": etl_meta.get('name', nb_file.stem),
                        "description": etl_meta.get('description', ''),
                        "etl": {
                            "sources": etl_meta.get('sources', []),
                            "outputs": etl_meta.get('outputs', []),
                            "options": etl_meta.get('options', {}),
                            "aws_profile": etl_meta.get('aws_profile'),
                            "variables": variables_schema,        # Full schema with type info
                            "variable_values": variable_values    # Default values extracted
                        }
                    })
                except Exception as e:
                    # Include notebook with error info
                    notebooks.append({
                        "path": str(nb_file),
                        "filename": nb_file.name,
                        "name": nb_file.stem,
                        "error": str(e)
                    })

            return {
                "status": "success",
                "notebooks": notebooks,
                "count": len(notebooks),
                "folder": str(folder_path)
            }

        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "notebooks": [],
                "count": 0
            }

    @tool
    def run_notebook(
        self,
        notebook_path: str,
        sources: List[Dict[str, Any]] = None,
        outputs: List[Dict[str, Any]] = None,
        options: Dict[str, Any] = None,
        aws_profile: str = None,
        variables: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Execute a Jupyter notebook as an ETL pipeline.

        Loads the notebook, extracts transformations from code cells,
        and runs them through the ETL engine. Parameters from notebook
        metadata can be overridden.

        Args:
            notebook_path: Path to .ipynb file
            sources: Override sources from notebook metadata (default: notebook's sources)
            outputs: Override outputs from notebook metadata (default: notebook's outputs)
            options: Override options from notebook metadata (default: notebook's options)
            aws_profile: Override AWS profile (default: notebook's aws_profile)
            variables: Override variables to inject into Python namespace
                       (default: notebook's variables)

        Returns:
            ETL result with:
                - status: "success", "partial_success", or "failed"
                - notebook: Source notebook path
                - transformations_run: List of completed transformations
                - outputs_created: List of files written
                - previews: Preview data for each transformation
                - errors: List of any errors encountered
                - metadata: Notebook ETL metadata used

        Example:
            # Run with notebook defaults
            run_notebook("agent/notebooks/etl_eastern.ipynb")

            # Override outputs
            run_notebook(
                "agent/notebooks/etl_eastern.ipynb",
                outputs=[{"source": "summary", "path": "my_output.csv", "format": "csv"}]
            )

            # Override variables
            run_notebook(
                "agent/notebooks/etl_eastern.ipynb",
                variables={"tz_bucket": "Pacific", "business_line": "AMXL"}
            )
        """
        import os
        from pathlib import Path

        try:
            # Resolve notebook path
            nb_path = Path(notebook_path)
            if not nb_path.is_absolute():
                # Try relative to agent/notebooks
                this_file = Path(__file__).resolve()
                nb_folder = this_file.parent.parent / 'notebooks'
                # Strip 'agent/notebooks/' prefix if present to avoid duplication
                clean_path = str(notebook_path).replace('agent/notebooks/', '').replace('agent\\notebooks\\', '')
                nb_path = nb_folder / clean_path

            if not nb_path.exists():
                return {
                    "status": "error",
                    "error": f"Notebook not found: {nb_path}",
                    "notebook": str(notebook_path)
                }

            # Parse notebook
            parsed = self._parse_notebook(str(nb_path))
            nb_meta = parsed['metadata']

            # Merge parameters: provided overrides > notebook defaults
            final_sources = sources if sources is not None else nb_meta.get('sources', [])
            final_outputs = outputs if outputs is not None else nb_meta.get('outputs', [])
            final_aws_profile = aws_profile if aws_profile is not None else nb_meta.get('aws_profile')

            # Merge options (provided options override notebook defaults)
            final_options = nb_meta.get('options', {}).copy()
            if options:
                final_options.update(options)

            # Merge variables (provided variables override notebook defaults)
            # Extract default values from typed schema format
            raw_variables = nb_meta.get('variables', {})
            final_variables = {}
            for var_name, var_def in raw_variables.items():
                if isinstance(var_def, dict) and 'type' in var_def:
                    # New typed format: extract default value
                    final_variables[var_name] = var_def.get('default', '')
                else:
                    # Legacy format: direct value
                    final_variables[var_name] = var_def
            # Override with provided variables
            if variables:
                final_variables.update(variables)

            # Get transformations from notebook cells
            transformations = parsed['transformations']

            if not transformations:
                return {
                    "status": "error",
                    "error": "No transformations found in notebook. Cells must have name/type metadata.",
                    "notebook": str(nb_path)
                }

            # Inject variables into both Python namespace AND DuckDB _vars table
            # This allows both Python cells (via variables) and SQL cells (via _vars table) to use them
            if final_variables:
                # Python variable assignments
                var_code = '\n'.join([
                    f"{k} = {repr(v)}" for k, v in final_variables.items()
                ])

                # SQL to create _vars table (for SQL cells to reference)
                # Format: CREATE OR REPLACE TABLE _vars AS SELECT 'value1' as key1, 'value2' as key2
                sql_columns = ', '.join([
                    f"{repr(v)} as {k}" for k, v in final_variables.items()
                ])
                vars_table_sql = f"conn.execute(\"CREATE OR REPLACE TABLE _vars AS SELECT {sql_columns}\")"

                # Combined injection code
                injection_code = var_code + '\n' + vars_table_sql

                # Find first Python cell and inject variables
                for i, t in enumerate(transformations):
                    if t['type'] == 'python':
                        transformations[i] = {
                            **t,
                            'query': injection_code + '\n' + t['query']
                        }
                        break
                else:
                    # No Python cell found, prepend a setup cell
                    transformations.insert(0, {
                        'name': '_variables',
                        'type': 'python',
                        'query': injection_code + '\nresult = "variables set"'
                    })

            # Run ETL pipeline
            result = self.etl(
                sources=final_sources,
                transformations=transformations,
                outputs=final_outputs,
                options=final_options,
                aws_profile=final_aws_profile
            )

            # Add notebook-specific info to result
            result['notebook'] = str(nb_path)
            result['metadata'] = {
                'name': nb_meta.get('name', nb_path.stem),
                'description': nb_meta.get('description', ''),
                'variables_used': final_variables
            }

            return result

        except Exception as e:
            import traceback
            return {
                "status": "error",
                "error": str(e),
                "traceback": traceback.format_exc() if self.debug else None,
                "notebook": str(notebook_path)
            }

    @tool
    def python(self, code: str) -> Dict[str, Any]:
        """
        Execute Python code with access to the DuckDB connection.

        Use this to load data from any source and register DataFrames for SQL queries.
        The code has access to:
            - conn: DuckDB connection (use conn.register('name', df) to make DataFrames queryable)
            - pd: pandas
            - json: json module
            - Path: pathlib.Path
            - result: set this variable to return data

        Args:
            code: Python code to execute

        Returns:
            Dictionary with status, any result set, and registered tables

        Examples:
            # Load VOVI data
            python('''
                from tools.vovi_fetch import fetch_vovi
                vovi_df, meta_df = fetch_vovi('2026-01-09')
                conn.register('vovi', vovi_df)
                conn.register('vovi_meta', meta_df)
                result = f"Loaded {len(vovi_df)} rows"
            ''')

            # Then query with SQL
            sql("SELECT * FROM vovi WHERE station LIKE 'D%'")
        """
        import pandas as pd
        import json
        from pathlib import Path

        try:
            conn = self._get_connection()

            # Namespace for code execution
            namespace = {
                'conn': conn,
                'pd': pd,
                'json': json,
                'Path': Path,
                'result': None
            }

            exec(code, namespace)

            # Get list of registered tables
            tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
            table_names = [t[0] for t in tables]

            return {
                "status": "success",
                "result": namespace.get('result'),
                "tables": table_names
            }

        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
