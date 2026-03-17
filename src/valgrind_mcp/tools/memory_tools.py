"""Massif memory analysis tools."""

from __future__ import annotations

import polars as pl
from mcp.server.fastmcp import Context

from valgrind_mcp import analysis
from valgrind_mcp.filters import apply_filters, build_filter_spec
from valgrind_mcp.formatters import format_filtered
from valgrind_mcp.models import MassifResult
from valgrind_mcp.server import get_run, mcp


@mcp.tool()
async def valgrind_analyze_memory(
    ctx: Context,
    run_id: str,
    time_min: int | None = None,
    time_max: int | None = None,
    min_bytes: int | None = None,
    max_bytes: int | None = None,
    detailed_only: bool = False,
    sample_every: int | None = None,
    offset: int = 0,
    limit: int | None = None,
    workspace_id: str | None = None,
) -> str:
    """Analyze massif results with time range filtering and sampling.

    Args:
        run_id: The run_id from a massif run
        time_min: Only snapshots at time >= this
        time_max: Only snapshots at time <= this
        min_bytes: Only snapshots with total_bytes >= this
        max_bytes: Only snapshots with total_bytes <= this
        detailed_only: Only show detailed/peak snapshots
        sample_every: Take every Nth snapshot
        offset: Skip first N snapshots
        limit: Max snapshots to return
        workspace_id: Workspace containing the run
    """
    _, run = get_run(ctx, run_id, workspace_id)
    if not isinstance(run, MassifResult):
        return f"Run {run_id} is not a massif run (tool={run.tool})"

    spec = build_filter_spec(
        time_min=time_min,
        time_max=time_max,
        min_bytes=min_bytes,
        max_bytes=max_bytes,
        sample_every=sample_every,
        offset=offset,
        limit=limit,
    )

    parts: list[str] = []

    df = analysis.massif_timeline_df(run)
    if detailed_only:
        df = df.filter(pl.col("is_detailed"))
    df = apply_filters(df, spec)
    parts.append(format_filtered(df, "Memory timeline", spec))

    peak_allocs = analysis.peak_allocations(run)
    if peak_allocs:
        parts.append("")
        parts.append("**Peak allocation tree:**")
        for alloc in peak_allocs[:30]:
            indent = "  " * alloc["depth"]
            fn = alloc["function"] or "???"
            loc = ""
            if alloc["file"] and alloc["line"]:
                loc = f" ({alloc['file']}:{alloc['line']})"
            parts.append(f"{indent}- `{fn}`{loc}: {alloc['bytes']:,} bytes")

    return "\n".join(parts)
