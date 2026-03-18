"""Search and correlation tools: devtools_search, devtools_correlate."""

from __future__ import annotations

from mcp.server.fastmcp import Context

from devtools_mcp.formatters import format_dataframe
from devtools_mcp.index import build_index, correlate_runs, search_index
from devtools_mcp.server import get_app_ctx, mcp


@mcp.tool()
async def devtools_search(
    ctx: Context,
    query: str | None = None,
    suite: str | None = None,
    run_ids: list[str] | None = None,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    kind_pattern: str | None = None,
    min_value: float | None = None,
    limit: int = 20,
    workspace_id: str | None = None,
) -> str:
    """Search across ALL stored results in the workspace.

    Queries a unified index built from all runs. Finds patterns across
    valgrind errors, profiling hotspots, debug snapshots, and trace data.

    Args:
        query: Text search across function names, files, error messages, kinds
        suite: Limit to a specific suite ("valgrind", "lldb", "dtrace", "perf")
        run_ids: Limit to specific runs (None = search all)
        file_pattern: Regex on source files
        function_pattern: Regex on function names
        kind_pattern: Regex on error/event kinds
        min_value: Minimum numeric value (bytes leaked, instruction count, etc.)
        limit: Max results (default 20)
        workspace_id: Workspace to search
    """
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    if not ws.runs:
        return "No runs stored. Use `devtools_run()` first."

    index = build_index(ws)

    if index.is_empty():
        return "No searchable data in stored runs."

    results = search_index(
        index,
        query=query,
        suite=suite,
        run_ids=run_ids,
        file_pattern=file_pattern,
        function_pattern=function_pattern,
        kind_pattern=kind_pattern,
        min_value=min_value,
        limit=limit,
    )

    if results.is_empty():
        desc = f"query='{query}'" if query else "filters"
        return f"No results matching {desc} across {len(ws.runs)} run(s)."

    total_in_index = len(index)
    header = f"**Search results** ({len(results)} of {total_in_index} indexed items"
    if query:
        header += f", query: `{query}`"
    header += ")\n"

    return header + "\n" + format_dataframe(results, max_rows=limit)


@mcp.tool()
async def devtools_correlate(
    ctx: Context,
    run_id_a: str,
    run_id_b: str,
    join_on: str = "function",
    limit: int = 50,
    workspace_id: str | None = None,
) -> str:
    """Correlate two runs by joining on a shared column.

    Example: join a memcheck run with a callgrind run on "function" to find
    "leaky functions that are also hot". Returns a merged table.

    Args:
        run_id_a: First run
        run_id_b: Second run
        join_on: Column to join on (default: "function")
        limit: Max rows in result
        workspace_id: Workspace containing both runs
    """
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    try:
        run_a = ws.get_run(run_id_a)
        run_b = ws.get_run(run_id_b)
    except KeyError as e:
        return str(e)

    result = correlate_runs(ws, run_id_a, run_id_b, join_on=join_on, limit=limit)

    if result.is_empty():
        return f"No correlation found joining {run_a.suite}:{run_a.tool} with {run_b.suite}:{run_b.tool} on `{join_on}`"

    header = (
        f"**Correlation:** {run_a.suite}:{run_a.tool} ↔ {run_b.suite}:{run_b.tool}\n"
        f"Join on: `{join_on}` | {len(result)} matching rows\n"
    )
    return header + "\n" + format_dataframe(result, max_rows=limit)
