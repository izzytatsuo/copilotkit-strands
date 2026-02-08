"""
Midway Cookie Expiration Check UDF

Returns minutes until Midway cookie expiration (negative if expired, null if error).

Usage:
    SELECT check_mw_cookie()  -- Returns: 458 (minutes until expiration)

    -- Check if valid (more than 5 min remaining)
    SELECT check_mw_cookie() > 5 AS cookie_valid
"""
from pathlib import Path
import time

COOKIE_PATH = str(Path.home() / ".midway" / "cookie")


def set_cookie_path(path: str):
    """Set the cookie path."""
    global COOKIE_PATH
    COOKIE_PATH = path


def check_mw_cookie() -> int:
    """
    Check Midway cookie expiration for amazon.dev domain.

    Returns:
        Minutes until expiration (negative if expired, None if error/not found)
    """
    try:
        cookie_path = Path(COOKIE_PATH).expanduser()

        if not cookie_path.exists():
            return None

        with open(cookie_path, 'r') as f:
            lines = f.readlines()

        # Parse cookie file (skip comment lines starting with #)
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split('\t')
            if len(parts) >= 7:
                domain, flag, path, secure, expiration, name, value = parts[:7]

                # Look for amazon.dev domain cookie
                if 'amazon.dev' in domain.lower():
                    try:
                        exp_epoch = int(expiration)
                        if exp_epoch == 0:
                            # Session cookie - no expiration
                            return None

                        current_epoch = int(time.time())
                        minutes_remaining = (exp_epoch - current_epoch) // 60
                        return minutes_remaining
                    except ValueError:
                        continue

        # No amazon.dev cookie found
        return None

    except Exception:
        return None


# DuckDB registration metadata
name = "check_mw_cookie"
func = check_mw_cookie
parameters = []  # No parameters
return_type = int
