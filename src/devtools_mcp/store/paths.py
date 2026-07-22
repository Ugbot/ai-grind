"""Persistent data directory paths for devtools-mcp."""

from __future__ import annotations

import os
from pathlib import Path


def data_root() -> Path:
    """Root data directory (~/.devtools-mcp or DEVTOOLS_MCP_DATA)."""
    raw = os.environ.get("DEVTOOLS_MCP_DATA", "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".devtools-mcp"


def runs_dir() -> Path:
    return data_root() / "runs"
