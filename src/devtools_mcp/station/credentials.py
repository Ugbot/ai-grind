"""Local credential store for the station platform API key.

The browser auth flow lands here: the dashboard's /station/auth page sends
the user to the platform's OAuth login with a loopback callback; the
platform mints an lls_ key and redirects it back to the dashboard, which
calls save_credentials(). Tools then resolve the key via
StationConfig.api_key(): env var first, this store second.

One credential per machine (~/.devtools-mcp/station-auth.json, 0600
best-effort). Env always wins so CI/agents can override without touching
the file.
"""

from __future__ import annotations

import contextlib
import json
import os
import stat
from pathlib import Path

from devtools_mcp.tracker.db import TrackerError, utc_now_iso

ENV_AUTH_PATH: str = "DEVTOOLS_MCP_STATION_AUTH"
KEY_PREFIX: str = "lls_"
KEY_MAX_LEN: int = 200
DASHBOARD_AUTH_PATH: str = "/station/auth"
DEFAULT_DASHBOARD: str = "http://127.0.0.1:8765"


def auth_instructions(platform_url: str = "") -> str:
    """The canonical how-to-authenticate text surfaced by tools and errors.

    Written for the LLM to relay to the human verbatim.
    """
    target = f" against {platform_url}" if platform_url else ""
    return (
        f"Not authenticated with the station platform{target}. Tell the user to:\n"
        f"1. Start the dashboard if it isn't running (devtools_dashboard tool), then\n"
        f"2. Open {DEFAULT_DASHBOARD}{DASHBOARD_AUTH_PATH} in a browser and sign in with "
        f"GitHub or Google, the key is stored locally and automatically.\n"
        f"Alternatively they can paste an existing lls_ key on that page, or "
        f"export LLM_STATION_API_KEY=lls_... in the environment (env always wins)."
    )


def credentials_path() -> Path:
    """Where the credential file lives (env override for tests)."""
    override = os.environ.get(ENV_AUTH_PATH, "").strip()
    path = Path(override) if override else Path.home() / ".devtools-mcp" / "station-auth.json"
    assert path.name, "credentials path has no filename"
    return path


def save_credentials(url: str, api_key: str, org_id: str, member: str = "") -> Path:
    """Persist the platform credential (single slot). Returns the file path."""
    if not url.startswith(("http://", "https://")):
        raise TrackerError(f"bad platform url {url!r}")
    if not api_key.startswith(KEY_PREFIX) or len(api_key) > KEY_MAX_LEN:
        raise TrackerError("that does not look like a platform API key (expected lls_...)")
    path = credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "url": url.rstrip("/"),
        "api_key": api_key,
        "org_id": org_id,
        "member": member,
        "saved_at": utc_now_iso(),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    with contextlib.suppress(OSError):
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0600; advisory on Windows
    assert path.is_file(), "credential write did not land"
    return path


def load_credentials() -> dict | None:
    """The stored credential, or None. Malformed files read as None (re-auth)."""
    path = credentials_path()
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    key = str(payload.get("api_key", ""))
    if not key.startswith(KEY_PREFIX) or len(key) > KEY_MAX_LEN:
        return None
    assert "url" in payload, "credential file missing url"
    return payload


def clear_credentials() -> bool:
    """Delete the stored credential. Returns True when something was removed."""
    path = credentials_path()
    if not path.is_file():
        return False
    path.unlink()
    assert not path.is_file(), "credential file survived unlink"
    return True
