"""Batch tools: devtools_check, devtools_run, devtools_list, devtools_raw."""

from __future__ import annotations

from mcp.server.fastmcp import Context

from devtools_mcp.registry import get_backend
from devtools_mcp.server import get_app_ctx, mcp


@mcp.tool()
async def devtools_check(ctx: Context) -> str:
    """Detect all installed development tools and their versions.

    Probes the system for valgrind, lldb, dtrace, and perf.
    Run this first to see what's available.
    """
    app = get_app_ctx(ctx)
    return app.registry.format_check()


@mcp.tool()
async def devtools_run(
    ctx: Context,
    suite: str,
    tool: str,
    binary: str,
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    workspace_id: str | None = None,
) -> str:
    """Run any development tool against a binary.

    Dispatches to the correct backend (valgrind, dtrace, perf) based on suite.
    Returns a concise summary with run_id for deeper analysis via devtools_analyze
    or devtools_search.

    Args:
        suite: Tool suite — "valgrind", "dtrace", "perf"
        tool: Specific tool — e.g. "memcheck", "callgrind", "massif", "stat", "trace"
        binary: Path to the executable to analyze
        args: Arguments to pass to the binary
        extra_args: Extra flags for the tool (e.g. valgrind suppression files)
        timeout: Max seconds to wait (default 300)
        workspace_id: Workspace to store results
    """
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    if not app.registry.is_available(suite, tool):
        available = [f"{t.suite}:{t.name}" for t in app.registry.list_available()]
        return f"Tool {suite}:{tool} is not available.\n\nInstalled tools: {', '.join(available) or 'none'}"

    try:
        backend = get_backend(suite)
    except KeyError as e:
        return str(e)

    err, parsed, raw_path = await backend.run(
        tool=tool,
        binary=binary,
        args=args,
        extra_args=extra_args,
        timeout=timeout,
    )

    if err:
        return err

    ws.store_run(parsed, raw_path)
    summary = backend.format_summary(parsed)
    return summary


@mcp.tool()
async def devtools_list(ctx: Context, workspace_id: str | None = None) -> str:
    """List all stored runs in the workspace.

    Shows run_id, suite, tool, binary, duration, and exit code for each run.
    """
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    runs = ws.list_runs()

    if not runs:
        return f"No runs in workspace `{ws.name}`."

    parts = [f"**Workspace:** `{ws.name}` ({len(runs)} run(s))", ""]
    for run in runs:
        parts.append(
            f"- `{run['run_id']}` | {run['suite']}:{run['tool']} | {run['binary']} | "
            f"{run['duration']} | exit {run['exit_code']}"
        )
    return "\n".join(parts)


@mcp.tool()
async def devtools_raw(ctx: Context, run_id: str, workspace_id: str | None = None) -> str:
    """Get the raw tool output for a run.

    Returns truncated output if the file exceeds 50KB.
    """
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    try:
        raw_path = ws.get_raw_path(run_id)
    except KeyError:
        return f"No raw output stored for run `{run_id}`."

    try:
        with open(raw_path, errors="replace") as f:
            content = f.read()
    except FileNotFoundError:
        return f"Raw output file not found: {raw_path}"

    max_len = 50_000
    if len(content) > max_len:
        return content[:max_len] + f"\n\n... truncated ({len(content):,} total bytes)"
    return content
