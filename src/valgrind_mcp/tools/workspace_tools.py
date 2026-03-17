"""Workspace management and utility tools."""

from __future__ import annotations

from mcp.server.fastmcp import Context

from valgrind_mcp.runner import check_valgrind
from valgrind_mcp.server import get_app_ctx, mcp


@mcp.tool()
async def valgrind_check(ctx: Context, valgrind_path: str = "valgrind") -> str:
    """Check if valgrind is installed and return version info.

    Run this first to verify valgrind is available before using other tools.
    """
    info = await check_valgrind(valgrind_path)
    if info.get("installed") == "true":
        return f"Valgrind is installed.\n**Version:** {info['version']}\n**Path:** {info['path']}"
    return f"Valgrind is NOT installed.\n**Error:** {info.get('error', 'unknown')}\n**Tried path:** {info['path']}"


@mcp.tool()
async def valgrind_raw_output(ctx: Context, run_id: str, workspace_id: str | None = None) -> str:
    """Get the raw valgrind output file content for a run.

    Returns truncated output if the file is very large (>50KB).
    """
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    raw_path = ws.get_raw_path(run_id)

    try:
        with open(raw_path, errors="replace") as f:
            content = f.read()
    except FileNotFoundError:
        return f"Raw output file not found: {raw_path}"

    max_len = 50_000
    if len(content) > max_len:
        return content[:max_len] + f"\n\n... truncated ({len(content):,} total bytes)"
    return content


@mcp.tool()
async def valgrind_create_workspace(ctx: Context, name: str = "default") -> str:
    """Create a new workspace for organizing valgrind runs.

    Returns the workspace_id to use with other tools.
    """
    app = get_app_ctx(ctx)
    ws = app.create_workspace(name)
    return f"Created workspace `{name}` with ID: `{ws.workspace_id}`"


@mcp.tool()
async def valgrind_list_runs(ctx: Context, workspace_id: str | None = None) -> str:
    """List all valgrind runs in a workspace with summary info."""
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    runs = ws.list_runs()

    if not runs:
        return f"No runs in workspace `{ws.name}` (`{ws.workspace_id}`)."

    parts = [f"**Workspace:** `{ws.name}` ({len(runs)} run(s))", ""]
    for run in runs:
        parts.append(
            f"- `{run['run_id']}` | {run['tool']} | {run['binary']} | {run['duration']} | exit {run['exit_code']}"
        )
    return "\n".join(parts)
