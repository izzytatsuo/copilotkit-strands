"""
SIM Data Tool - Strands tool for fetching and creating SIM tickets via Maxis API.

Provides three main capabilities:
1. search_sim - Query by folder ID with optional filters (status, assignee, date range, etc.)
2. fetch_sim_by_ids - Fetch specific SIM tickets by V-number or UUID
3. create_sim - Create new SIM tickets in specified folders

Authentication uses:
- Browser cookies (Firefox/Chrome) via browser_cookie3
- Windows SSPI/Kerberos auth via requests_negotiate_sspi
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import pandas as pd
import requests
import urllib3

from strands import tool

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Try to import auth dependencies
try:
    from requests_negotiate_sspi import HttpNegotiateAuth
    SSPI_AVAILABLE = True
except ImportError:
    SSPI_AVAILABLE = False

try:
    import browser_cookie3
    COOKIES_AVAILABLE = True
except ImportError:
    COOKIES_AVAILABLE = False


class SIMData:
    """
    SIM Data tool provider for Strands agents.

    Fetches SIM tickets from the Maxis API with Windows authentication.
    Uses Firefox/Chrome cookies + SSPI/Kerberos auth.

    Example usage:
        sim_data = SIMData(debug=True)
        # Then add sim_data.search_sim and sim_data.fetch_sim_by_ids to agent tools
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        browser: str = "firefox",
        debug: bool = False,
        default_preview_rows: int = 3
    ):
        """
        Initialize SIMData tool provider.

        Args:
            base_url: Maxis API base URL. Defaults to env SIM_BASE_URL or PDX endpoint.
            browser: Browser to load cookies from ('firefox', 'chrome', 'edge')
            debug: Enable debug logging
            default_preview_rows: Number of preview rows in response summary
        """
        self.base_url = base_url or os.getenv(
            "SIM_BASE_URL",
            "https://maxis-service-prod-pdx.amazon.com"
        )
        self.write_base_url = os.getenv(
            "SIM_WRITE_URL",
            "https://maxis-service-prod-iad.amazon.com"
        )
        self.browser = browser
        self.debug = debug
        self.default_preview_rows = default_preview_rows
        self.logger = logging.getLogger(__name__)

        if debug:
            logging.basicConfig(level=logging.DEBUG)

        self._session = None
        self._auth = None
        self._sso_cookies = None

    def _get_session(self) -> requests.Session:
        """Get or create authenticated session with cookies and SSPI auth."""
        if self._session is None:
            self._session = requests.Session()
            self._session.verify = False

            # Setup SSPI auth if available
            if SSPI_AVAILABLE:
                self._auth = HttpNegotiateAuth()
                if self.debug:
                    self.logger.info("SSPI/Kerberos auth initialized")
            else:
                self._auth = None
                self.logger.warning("SSPI auth not available - install requests_negotiate_sspi")

            # Load browser cookies
            self._load_browser_cookies()

        return self._session

    def _load_browser_cookies(self):
        """Load cookies from browser for authentication."""
        if not COOKIES_AVAILABLE:
            self.logger.warning("browser_cookie3 not available - install browser_cookie3")
            return

        cookie_count = 0
        browsers = {
            'firefox': browser_cookie3.firefox,
            'chrome': browser_cookie3.chrome,
            'edge': browser_cookie3.edge,
        }

        browser_func = browsers.get(self.browser)
        if not browser_func:
            self.logger.warning(f"Unknown browser: {self.browser}")
            return

        try:
            cookies = browser_func(domain_name='.amazon.com')
            for cookie in cookies:
                self._session.cookies.set(cookie.name, cookie.value, domain=cookie.domain)
                cookie_count += 1
            if self.debug:
                self.logger.info(f"Loaded {cookie_count} cookies from {self.browser}")
        except Exception as e:
            self.logger.warning(f"Could not load {self.browser} cookies: {e}")

        if cookie_count == 0:
            self.logger.warning(
                f"No cookies loaded. Make sure you're logged into Midway in {self.browser}."
            )

    def _get_sso_auth_cookies(self):
        """Perform full SSO authentication for write operations.

        Uses a FRESH session to avoid browser cookies overwriting the rfp cookie.
        """
        if self._sso_cookies is not None:
            return self._sso_cookies

        # Use a fresh session for SSO - don't pre-load browser cookies
        sso_session = requests.Session()
        sso_session.verify = False
        auth = HttpNegotiateAuth() if SSPI_AVAILABLE else None

        try:
            # Step 1: Get authn_endpoint (fresh session gets proper rfp cookie)
            sso_login_url = f"{self.write_base_url}/sso/login"
            headers = {'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'}
            response = sso_session.get(sso_login_url, headers=headers, verify=False)
            if response.status_code != 200:
                return None
            data = response.json()
            authn_endpoint = data.get('authn_endpoint')
            if not authn_endpoint:
                return None

            # Get rfp cookie from session (set by the API response)
            amzn_sso_rfp = None
            for cookie in sso_session.cookies:
                if cookie.name == 'amzn_sso_rfp':
                    amzn_sso_rfp = cookie.value
                    break
            if not amzn_sso_rfp:
                return None

            # Step 2: Get Midway URL (with SSPI auth)
            response = sso_session.get(authn_endpoint, auth=auth, allow_redirects=False, verify=False)
            if response.status_code != 302:
                return None
            midway_url = response.headers.get('Location')
            if not midway_url:
                return None

            # Step 3: Get token (load midway cookies from browser)
            midway_cookies = {}
            if COOKIES_AVAILABLE:
                browser_func = {'firefox': browser_cookie3.firefox, 'chrome': browser_cookie3.chrome, 'edge': browser_cookie3.edge}.get(self.browser)
                if browser_func:
                    try:
                        all_cookies = browser_func(domain_name='.amazon.com')
                        for cookie in all_cookies:
                            if 'midway' in cookie.domain or cookie.name in ['session', 'user_name', 'amazon_enterprise_access']:
                                midway_cookies[cookie.name] = cookie.value
                    except:
                        pass

            response = sso_session.get(midway_url, cookies=midway_cookies, auth=auth, allow_redirects=False, verify=False)
            if response.status_code != 200:
                return None
            token = response.text
            if not token or len(token) < 10:
                return None

            self._sso_cookies = {'amzn_sso_rfp': amzn_sso_rfp, 'amzn_sso_token': token}
            return self._sso_cookies
        except:
            return None


    def _make_request(self, url: str, method: str = 'GET', **kwargs) -> requests.Response:
        """Make an authenticated request to the Maxis API."""
        session = self._get_session()

        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        headers.update(kwargs.get('headers', {}))
        kwargs['headers'] = headers

        if self._auth:
            kwargs['auth'] = self._auth

        # Explicitly set verify=False to ensure SSL verification is disabled
        kwargs.setdefault('verify', False)

        # Set default timeout if not provided
        kwargs.setdefault('timeout', 30)

        response = session.request(method, url, **kwargs)

        # Handle redirects to sentry/midway
        if response.status_code in (302, 307):
            redirect_url = response.headers.get('Location')
            if redirect_url:
                if self.debug:
                    self.logger.info(f"Following redirect to: {redirect_url[:80]}...")
                response = session.get(redirect_url, auth=self._auth, headers=headers)

        return response

    def _extract_issue_details(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Extract important fields from a SIM issue document."""
        # Get SIM ID from aliases (first alias, not necessarily V-prefixed)
        sim_id = ''
        aliases = doc.get('aliases', [])
        if aliases:
            sim_id = aliases[0].get('id', '')

        # Extract tags (skip River- workflow tags)
        tags = doc.get('tags', [])
        clean_tags = [t.get('id', '') for t in tags if not t.get('id', '').startswith('River-')]

        return {
            'sim_id': sim_id,
            'UUID': doc.get('id', ''),
            'Title': doc.get('title', ''),
            'Status': doc.get('status', ''),
            'TT_Status': doc.get('extensions', {}).get('tt', {}).get('status', ''),
            'Requester': self._clean_identity(doc.get('requesterIdentity', '')),
            'Assignee': self._clean_identity(doc.get('assigneeIdentity', '')),
            'Resolved_By': self._clean_identity(doc.get('lastResolvedByIdentity', '')),
            'Last_Updated_By': self._clean_identity(doc.get('lastUpdatedIdentity', '')),
            'Created': self._format_date(doc.get('createDate')),
            'Updated': self._format_date(doc.get('lastUpdatedDate')),
            'Resolved': self._format_date(doc.get('lastResolvedDate')),
            'Tags': ', '.join(clean_tags),
            'Containing_Folder': doc.get('containingFolder', ''),
            'Assigned_Folder': doc.get('assignedFolder', ''),
            'Description_Preview': (doc.get('description', '') or '')[:200],
        }

    def _clean_identity(self, identity: str) -> str:
        """Clean up identity strings."""
        if not identity:
            return ''
        if identity.startswith('kerberos:'):
            identity = identity[9:]
        if '@ANT.AMAZON.COM' in identity:
            identity = identity.split('@')[0]
        if identity.startswith('arn:aws:'):
            parts = identity.split('/')
            if len(parts) > 1:
                return parts[-1]
        return identity

    def _format_date(self, date_str: str) -> str:
        """Format date string for display."""
        if not date_str:
            return ''
        return date_str[:19].replace('T', ' ') if len(date_str) >= 19 else date_str

    def _save_output(
        self,
        df: pd.DataFrame,
        output_path: str,
        output_format: str,
        raw_issues: Optional[List[Dict]] = None
    ) -> Dict[str, Any]:
        """Save DataFrame or raw JSON to output path."""
        try:
            # Handle S3 paths
            if output_path.startswith('s3://'):
                import boto3
                from io import StringIO, BytesIO

                # Parse S3 path
                path_parts = output_path[5:].split('/', 1)
                bucket = path_parts[0]
                key = path_parts[1] if len(path_parts) > 1 else ''

                s3_client = boto3.client('s3')

                if output_format.lower() == 'json' and raw_issues:
                    # Save raw JSON
                    json_content = json.dumps(raw_issues, indent=2, default=str)
                    s3_client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=json_content.encode('utf-8'),
                        ContentType='application/json'
                    )
                else:
                    # Save CSV
                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer, index=False)
                    s3_client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=csv_buffer.getvalue().encode('utf-8'),
                        ContentType='text/csv'
                    )

                return {"path": output_path, "location": "s3"}

            else:
                # Local file
                os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)

                if output_format.lower() == 'json' and raw_issues:
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(raw_issues, f, indent=2, default=str)
                else:
                    df.to_csv(output_path, index=False)

                return {"path": os.path.abspath(output_path), "location": "local"}

        except Exception as e:
            return {"path": output_path, "location": "error", "error": str(e)}


    @tool
    def create_sim(
        self,
        folder_id: str,
        title: str,
        description: str,
        custom_fields: Optional[str] = None,
        tags: Optional[str] = None,
        requester: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new SIM ticket in the specified folder.

        Args:
            folder_id: Target folder UUID
            title: Ticket title
            description: Ticket description
            custom_fields: Optional JSON array of custom fields
            tags: Optional comma-separated tag names
            requester: Optional requester kerberos username

        Returns:
            Dict with status, created sim_id/UUID, or error.
        """
        try:
            sso_cookies = self._get_sso_auth_cookies()
            if not sso_cookies:
                return {'status': 'error', 'error': 'SSO authentication failed. Please ensure you are logged into Midway.'}

            user = requester or os.getlogin()
            payload = {
                'title': title,
                'description': description,
                'assignedFolder': folder_id,
                'requesterIdentity': f'kerberos:{user}@ANT.AMAZON.COM'
            }

            if custom_fields:
                try:
                    fields = json.loads(custom_fields)
                    if isinstance(fields, list):
                        payload['customFields'] = {'string': fields}
                except json.JSONDecodeError:
                    return {'status': 'error', 'error': 'Invalid custom_fields JSON'}

            if tags:
                tag_list = [{'id': t.strip()} for t in tags.split(',') if t.strip()]
                if tag_list:
                    payload['tags'] = tag_list

            url = f'{self.write_base_url}/issues'
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Origin': 'https://sim.amazon.com',
                'Referer': 'https://sim.amazon.com/',
                'amzn-version': '1.0'
            }

            session = self._get_session()
            response = session.post(url, headers=headers, cookies=sso_cookies, json=payload, verify=False, timeout=30)

            if response.status_code == 201:
                try:
                    result = response.json()
                    aliases = result.get('aliases', [])
                    sim_id = aliases[0].get('id', '') if aliases else ''
                    return {'status': 'success', 'sim_id': sim_id, 'uuid': result.get('id', ''), 'title': title, 'folder_id': folder_id}
                except json.JSONDecodeError:
                    return {'status': 'success', 'message': 'Created but could not parse response'}
            else:
                if 'midway' in response.text.lower():
                    self._sso_cookies = None
                return {'status': 'error', 'error': response.text[:500], 'http_status': response.status_code}

        except Exception as e:
            return {'status': 'error', 'error': str(e)}

    @tool
    def search_sim(
        self,
        output_path: Optional[str] = None,
        return_dataframe: bool = False,
        folder_id: Optional[str] = None,
        status: Optional[str] = None,
        tt_status: Optional[str] = None,
        assigned_group: Optional[str] = None,
        assignee: Optional[str] = None,
        date_range: Optional[str] = None,
        sort_by: str = "createDate",
        sort_order: str = "desc",
        limit: int = 50,
        output_format: str = "csv",
        custom_query: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Search SIM tickets by folder ID and optional filters.

        Queries the Maxis API using Lucene query syntax with flexible filters.
        Can save results to file or return DataFrame for in-memory processing.

        Args:
            output_path: Optional where to save results (S3 path like s3://bucket/key.csv or local path)
            return_dataframe: If True, include the pandas DataFrame in the result (default: False)
            folder_id: SIM folder UUID to search in (e.g., "40c4bd10-f3e8-473d-8fe4-c75ff5d81cfc")
            status: Issue status filter ("Open" or "Closed")
            tt_status: TT status filter ("Assigned", "Researching", "Work In Progress", "Pending", "Resolved")
            assigned_group: Resolver group filter (e.g., "STCO Forecast Support")
            assignee: Assignee kerberos username filter
            date_range: Date range filter ("today", "last_7_days", "last_30_days", or None for all)
            sort_by: Field to sort by ("createDate", "lastUpdatedDate", "title")
            sort_order: Sort order ("desc" or "asc")
            limit: Maximum number of results (default 50, 0 for no limit)
            output_format: Output format ("csv" for structured data, "json" for raw API response)
            custom_query: Custom Lucene query string (overrides other filters if provided)

        Returns:
            Dict with status, row count, columns, preview rows, and optionally:
            - output: Save result info (if output_path provided)
            - dataframe: pandas DataFrame (if return_dataframe=True)
        """
        try:
            # Build query
            if custom_query:
                query = custom_query
            else:
                query_parts = []

                if folder_id:
                    query_parts.append(f"containingFolder:{folder_id}")

                if status and status.lower() != 'all':
                    query_parts.append(f"status:{status}")

                if tt_status and tt_status.lower() != 'all':
                    tt_val = f'"{tt_status}"' if ' ' in tt_status else tt_status
                    query_parts.append(f"extensions.tt.status:{tt_val}")

                if assigned_group:
                    query_parts.append(f'extensions.tt.assignedGroup:"{assigned_group}"')

                if assignee:
                    query_parts.append(f"assigneeIdentity:kerberos:{assignee}@ANT.AMAZON.COM")

                # Date range
                if date_range:
                    date_map = {
                        'today': ('NOW-1DAYS', 'NOW'),
                        'last_7_days': ('NOW-7DAYS', 'NOW'),
                        'last_30_days': ('NOW-30DAYS', 'NOW'),
                    }
                    if date_range in date_map:
                        start, end = date_map[date_range]
                        query_parts.append(f"createDate:[{start} TO {end}]")

                query = " AND ".join(query_parts) if query_parts else "*"

            # Build URL
            encoded_query = quote(query, safe='')
            url = f"{self.base_url}/issues/?q={encoded_query}"

            if sort_by:
                sort_param = f"{sort_by} {sort_order}"
                url += f"&sort={quote(sort_param, safe='')}"

            if limit and limit > 0:
                url += f"&limit={limit}"

            if self.debug:
                self.logger.info(f"SIM search URL: {url}")

            # Make request
            response = self._make_request(url)

            if response.status_code != 200:
                # Check for auth redirect
                if 'midway' in response.text.lower() or 'sentry' in response.text.lower():
                    return {
                        "status": "error",
                        "error": "Authentication required. Please log into sim.amazon.com in your browser.",
                        "http_status": response.status_code
                    }
                return {
                    "status": "error",
                    "error": f"HTTP {response.status_code}: {response.text[:500]}",
                    "http_status": response.status_code
                }

            try:
                result = response.json()
            except json.JSONDecodeError:
                return {
                    "status": "error",
                    "error": "Invalid JSON response from API"
                }

            if 'error' in result:
                return {
                    "status": "error",
                    "error": result['error']
                }

            documents = result.get('documents', [])

            if not documents:
                return {
                    "status": "success",
                    "message": "No tickets found matching criteria",
                    "rows": 0,
                    "query": query
                }

            # Extract fields
            rows = [self._extract_issue_details(doc) for doc in documents]
            df = pd.DataFrame(rows)

            result = {
                "status": "success",
                "rows": len(df),
                "columns": list(df.columns),
                "query": query,
            }

            # Include DataFrame if requested
            if return_dataframe:
                result["dataframe"] = df

            # Save output (if output_path provided)
            if output_path:
                save_result = self._save_output(
                    df,
                    output_path,
                    output_format,
                    raw_issues=documents if output_format.lower() == 'json' else None
                )
                result["output"] = save_result

            # Build preview (limited rows, key columns)
            preview_cols = ['sim_id', 'Title', 'Status', 'TT_Status', 'Assignee', 'Created']
            preview_cols = [c for c in preview_cols if c in df.columns]
            preview_df = df[preview_cols].head(self.default_preview_rows)
            result["preview"] = preview_df.to_dict(orient='records')

            return result

        except Exception as e:
            self.logger.exception("Error in search_sim")
            return {
                "status": "error",
                "error": str(e)
            }

    @tool
    def fetch_sim_by_ids(
        self,
        issue_ids: str,
        output_path: Optional[str] = None,
        return_dataframe: bool = False,
        output_format: str = "csv"
    ) -> Dict[str, Any]:
        """
        Fetch specific SIM tickets by V-number or UUID.

        Fetches one or more SIM tickets by their IDs. Supports both V-numbers
        (e.g., V2028249521) and UUIDs. Can save to file or return DataFrame.

        Args:
            issue_ids: Comma-separated or newline-separated list of issue IDs
                       (e.g., "V2028249521,V2021488750" or "V2028249521\\nV2021488750")
            output_path: Optional where to save results (S3 path like s3://bucket/key.csv or local path)
            return_dataframe: If True, include the pandas DataFrame in the result (default: False)
            output_format: Output format ("csv" for structured data, "json" for raw API response)

        Returns:
            Dict with status, row count, successful/failed counts, and optionally:
            - output: Save result info (if output_path provided)
            - dataframe: pandas DataFrame (if return_dataframe=True)
        """
        try:
            # Parse IDs
            ids = []
            for line in issue_ids.replace(',', '\n').split('\n'):
                id_clean = line.strip()
                if id_clean:
                    ids.append(id_clean)

            if not ids:
                return {
                    "status": "error",
                    "error": "No valid issue IDs provided"
                }

            if self.debug:
                self.logger.info(f"Fetching {len(ids)} SIM issues by ID")

            # Fetch each issue
            results = []
            failed = []

            for issue_id in ids:
                url = f"{self.base_url}/issues/{issue_id}"
                response = self._make_request(url)

                if response.status_code == 200:
                    try:
                        doc = response.json()
                        if 'error' not in doc:
                            results.append(doc)
                        else:
                            failed.append({"id": issue_id, "error": doc.get('error')})
                    except json.JSONDecodeError:
                        failed.append({"id": issue_id, "error": "Invalid JSON"})
                else:
                    failed.append({"id": issue_id, "error": f"HTTP {response.status_code}"})

            if not results:
                return {
                    "status": "error",
                    "error": "Failed to fetch any issues",
                    "failed": failed
                }

            # Extract fields
            rows = [self._extract_issue_details(doc) for doc in results]
            df = pd.DataFrame(rows)

            result = {
                "status": "success",
                "rows": len(df),
                "fetched": len(results),
                "failed_count": len(failed),
                "failed": failed[:5] if failed else [],  # Limit failed list in response
                "columns": list(df.columns),
            }

            # Include DataFrame if requested
            if return_dataframe:
                result["dataframe"] = df

            # Save output (if output_path provided)
            if output_path:
                save_result = self._save_output(
                    df,
                    output_path,
                    output_format,
                    raw_issues=results if output_format.lower() == 'json' else None
                )
                result["output"] = save_result

            # Build preview
            preview_cols = ['sim_id', 'Title', 'Status', 'TT_Status', 'Assignee', 'Created']
            preview_cols = [c for c in preview_cols if c in df.columns]
            preview_df = df[preview_cols].head(self.default_preview_rows)
            result["preview"] = preview_df.to_dict(orient='records')

            return result

        except Exception as e:
            self.logger.exception("Error in fetch_sim_by_ids")
            return {
                "status": "error",
                "error": str(e)
            }

    @tool
    def check_sim_status(self) -> Dict[str, Any]:
        """
        Check SIM API connection and authentication status.

        Verifies that authentication dependencies are available and tests
        connectivity to the Maxis API.

        Returns:
            Dict with authentication status and connectivity info.
        """
        status = {
            "sspi_available": SSPI_AVAILABLE,
            "cookies_available": COOKIES_AVAILABLE,
            "base_url": self.base_url,
            "browser": self.browser
        }

        # Check if cookies are loaded
        session = self._get_session()
        cookie_count = len(session.cookies)
        status["cookies_loaded"] = cookie_count

        if not SSPI_AVAILABLE:
            status["warning"] = "SSPI auth not available - install: pip install requests_negotiate_sspi"

        if not COOKIES_AVAILABLE:
            status["warning"] = "browser_cookie3 not available - install: pip install browser_cookie3"

        if cookie_count == 0:
            status["warning"] = f"No cookies loaded. Please log into sim.amazon.com in {self.browser}."

        # Test connectivity with a simple query
        try:
            url = f"{self.base_url}/issues/?q=*&limit=1"
            response = self._make_request(url, timeout=10)
            status["http_status"] = response.status_code

            if response.status_code == 200:
                status["connection"] = "ok"
                try:
                    data = response.json()
                    status["api_response"] = "valid JSON"
                    if 'documents' in data:
                        status["api_response"] = f"valid - {len(data.get('documents', []))} docs in test query"
                except json.JSONDecodeError:
                    status["api_response"] = "invalid JSON"
            elif 'midway' in response.text.lower() or 'sentry' in response.text.lower():
                status["connection"] = "auth_required"
                status["message"] = "Please log into sim.amazon.com in your browser"
            else:
                status["connection"] = "error"
                status["message"] = response.text[:200]
        except requests.exceptions.Timeout:
            status["connection"] = "timeout"
        except requests.exceptions.ConnectionError as e:
            status["connection"] = "connection_error"
            status["message"] = str(e)[:200]
        except Exception as e:
            status["connection"] = "error"
            status["message"] = str(e)[:200]

        return status
