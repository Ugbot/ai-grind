"""Run comparison tools."""

from __future__ import annotations

import polars as pl
from mcp.server.fastmcp import Context

from valgrind_mcp import analysis, formatters
from valgrind_mcp.filters import apply_filters, build_filter_spec
from valgrind_mcp.models import CallgrindResult, MassifResult, MemcheckResult
from valgrind_mcp.server import get_app_ctx, mcp


@mcp.tool()
async def valgrind_compare_runs(
    ctx: Context,
    run_id_a: str,
    run_id_b: str,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    exclude_files: str | None = None,
    exclude_functions: str | None = None,
    min_delta: int | None = None,
    sort_by: str | None = None,
    sort_descending: bool = True,
    offset: int = 0,
    limit: int | None = None,
    workspace_id: str | None = None,
) -> str:
    """Compare two valgrind runs of the same tool type with filtering.

    Args:
        run_id_a: First run (baseline)
        run_id_b: Second run (comparison)
        file_pattern: Regex to include only matching files
        function_pattern: Regex to include only matching functions
        exclude_files: Regex to exclude files
        exclude_functions: Regex to exclude functions
        min_delta: Minimum absolute delta to include
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return
        workspace_id: Workspace containing both runs
    """
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run_a = ws.get_run(run_id_a)
    run_b = ws.get_run(run_id_b)

    if run_a.tool != run_b.tool:
        return f"Cannot compare different tools: {run_a.tool} vs {run_b.tool}"

    if isinstance(run_a, MemcheckResult) and isinstance(run_b, MemcheckResult):
        df = analysis.compare_memcheck(run_a, run_b)
        if min_delta is not None and "count_delta" in df.columns:
            df = df.filter(pl.col("count_delta").abs() >= min_delta)
        spec = build_filter_spec(
            kind_pattern=function_pattern,
            sort_by=sort_by,
            sort_descending=sort_descending,
            offset=offset,
            limit=limit,
        )
        df = apply_filters(df, spec)
        return formatters.format_filtered(df, "Memcheck comparison (A → B)", spec)

    if isinstance(run_a, CallgrindResult) and isinstance(run_b, CallgrindResult):
        df = analysis.compare_callgrind(run_a, run_b)
        if min_delta is not None and "cost_delta" in df.columns:
            df = df.filter(pl.col("cost_delta").abs() >= min_delta)
        spec = build_filter_spec(
            function_pattern=function_pattern,
            exclude_functions=exclude_functions,
            sort_by=sort_by,
            sort_descending=sort_descending,
            offset=offset,
            limit=limit,
        )
        df = apply_filters(df, spec)
        return formatters.format_filtered(df, "Callgrind comparison (A → B)", spec)

    if isinstance(run_a, MassifResult) and isinstance(run_b, MassifResult):
        info = analysis.compare_massif(run_a, run_b)
        parts = ["**Massif comparison (A → B):**", ""]
        parts.append(f"- Peak A: {info['peak_a_bytes']:,} bytes")
        parts.append(f"- Peak B: {info['peak_b_bytes']:,} bytes")
        parts.append(f"- Delta: {info['peak_delta_bytes']:+,} bytes ({info['peak_delta_pct']:+.1f}%)")
        parts.append(f"- Snapshots: {info['snapshots_a']} → {info['snapshots_b']}")
        return "\n".join(parts)

    return f"Comparison not implemented for tool: {run_a.tool}"
