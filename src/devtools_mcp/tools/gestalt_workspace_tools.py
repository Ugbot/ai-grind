"""Maintained local portable Gestalt workspace reconstruction entry point."""

from __future__ import annotations

from typing import Any, Literal

from devtools_mcp.gestalt_workspace import reconstruct
from devtools_mcp.server import mcp


@mcp.tool()
async def devtools_gestalt_workspace(
    action: Literal["inspect", "install", "verify"],
    bundle_dir: str,
    base_url: str,
    limits: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Inspect/install/verify portable CME research v1 examples, without executing cells.

    Supply an absolute bundle_dir and explicit HTTP loopback base_url with port.
    Reads all bounded notebook/dashboard pages and full matching bodies. Installs
    only missing resources after conflict preflight; never overwrites or deletes.
    Returns bounded ID/hash/state/URL receipts, including partial/uncertain outcomes.
    On uncertainty inspect/resume; never blindly retry a create. Single-writer
    ownership across MCP processes is required (server has no atomic name create).
    Optional limits: max_resources (per kind), max_pages (per kind), page_size,
    max_file_bytes, max_body_bytes, max_total_bytes, max_response_bytes, timeout_ms.
    No auth/config changes, remote hosts, redirects, old IDs, or code execution.
    """
    return await reconstruct(action, bundle_dir, base_url, limits)
