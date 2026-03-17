"""Profiling analysis tools — callgrind hotspots, cachegrind miss rates, call graphs."""

from __future__ import annotations

import polars as pl
from mcp.server.fastmcp import Context

from valgrind_mcp import analysis, formatters
from valgrind_mcp.filters import apply_filters, build_filter_spec
from valgrind_mcp.models import CachegrindResult, CallgrindResult
from valgrind_mcp.server import get_run, mcp


@mcp.tool()
async def valgrind_analyze_hotspots(
    ctx: Context,
    run_id: str,
    event: str = "Ir",
    top_n: int = 20,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    exclude_files: str | None = None,
    exclude_functions: str | None = None,
    min_cost: int | None = None,
    min_pct: float | None = None,
    sort_by: str | None = None,
    sort_descending: bool = True,
    offset: int = 0,
    limit: int | None = None,
    sample_n: int | None = None,
    workspace_id: str | None = None,
) -> str:
    """Analyze callgrind hotspots with rich filtering.

    Args:
        run_id: The run_id from a callgrind run
        event: Event to sort by (Ir, Dr, Dw, etc.)
        top_n: Number of top functions (before pagination)
        file_pattern: Regex to include only matching files
        function_pattern: Regex to include only matching functions
        exclude_files: Regex to exclude files
        exclude_functions: Regex to exclude functions
        min_cost: Minimum self cost to include
        min_pct: Minimum self percentage to include
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return
        sample_n: Random sample of N rows
        workspace_id: Workspace containing the run
    """
    _, run = get_run(ctx, run_id, workspace_id)
    if not isinstance(run, CallgrindResult):
        return f"Run {run_id} is not a callgrind run (tool={run.tool})"

    thresholds: dict[str, tuple[float | None, float | None]] = {}
    if min_cost is not None:
        thresholds[f"self_{event}"] = (min_cost, None)
    if min_pct is not None:
        thresholds["self_pct"] = (min_pct, None)

    spec = build_filter_spec(
        file_pattern=file_pattern,
        function_pattern=function_pattern,
        exclude_files=exclude_files,
        exclude_functions=exclude_functions,
        sort_by=sort_by,
        sort_descending=sort_descending,
        offset=offset,
        limit=limit,
        sample_n=sample_n,
        thresholds=thresholds,
    )

    df = analysis.hotspots(run, event=event, top_n=top_n)
    df = apply_filters(df, spec)
    return formatters.format_filtered(df, f"Callgrind hotspots by {event}", spec)


@mcp.tool()
async def valgrind_analyze_cache(
    ctx: Context,
    run_id: str,
    top_n: int = 20,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    exclude_files: str | None = None,
    exclude_functions: str | None = None,
    min_miss_pct: float | None = None,
    min_ir: int | None = None,
    sort_by: str | None = None,
    sort_descending: bool = True,
    offset: int = 0,
    limit: int | None = None,
    sample_n: int | None = None,
    workspace_id: str | None = None,
) -> str:
    """Analyze cachegrind — functions with worst cache miss rates.

    Args:
        run_id: The run_id from a cachegrind run
        top_n: Number of top functions
        file_pattern: Regex to include only matching files
        function_pattern: Regex to include only matching functions
        exclude_files: Regex to exclude files
        exclude_functions: Regex to exclude functions
        min_miss_pct: Minimum I1 miss percentage
        min_ir: Minimum instruction references
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return
        sample_n: Random sample of N rows
        workspace_id: Workspace containing the run
    """
    _, run = get_run(ctx, run_id, workspace_id)
    if not isinstance(run, CachegrindResult):
        return f"Run {run_id} is not a cachegrind run (tool={run.tool})"

    thresholds: dict[str, tuple[float | None, float | None]] = {}
    if min_miss_pct is not None:
        thresholds["i1_miss_pct"] = (min_miss_pct, None)
    if min_ir is not None:
        thresholds["total_ir"] = (min_ir, None)

    spec = build_filter_spec(
        file_pattern=file_pattern,
        function_pattern=function_pattern,
        exclude_files=exclude_files,
        exclude_functions=exclude_functions,
        sort_by=sort_by,
        sort_descending=sort_descending,
        offset=offset,
        limit=limit,
        sample_n=sample_n,
        thresholds=thresholds,
    )

    df = analysis.cache_miss_rates(run, top_n=top_n)
    df = apply_filters(df, spec)
    return formatters.format_filtered(df, "Cachegrind miss rates by function", spec)


@mcp.tool()
async def valgrind_analyze_cache_lines(
    ctx: Context,
    run_id: str,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    exclude_files: str | None = None,
    exclude_functions: str | None = None,
    min_ir: int | None = None,
    min_miss_rate: float | None = None,
    sort_by: str = "ir",
    sort_descending: bool = True,
    offset: int = 0,
    limit: int = 50,
    sample_n: int | None = None,
    workspace_id: str | None = None,
) -> str:
    """Analyze cachegrind at source line granularity with filtering.

    Args:
        run_id: The run_id from a cachegrind run
        file_pattern: Regex to include only matching files
        function_pattern: Regex to include only matching functions
        exclude_files: Regex to exclude files
        exclude_functions: Regex to exclude functions
        min_ir: Minimum instruction references per line
        min_miss_rate: Minimum L1 instruction miss rate percentage
        sort_by: Column to sort by (default: ir)
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return (default 50)
        sample_n: Random sample of N rows
        workspace_id: Workspace containing the run
    """
    _, run = get_run(ctx, run_id, workspace_id)
    if not isinstance(run, CachegrindResult):
        return f"Run {run_id} is not a cachegrind run (tool={run.tool})"

    thresholds: dict[str, tuple[float | None, float | None]] = {}
    if min_ir is not None:
        thresholds["ir"] = (min_ir, None)
    if min_miss_rate is not None:
        thresholds["i1_miss_rate"] = (min_miss_rate, None)

    spec = build_filter_spec(
        file_pattern=file_pattern,
        function_pattern=function_pattern,
        exclude_files=exclude_files,
        exclude_functions=exclude_functions,
        sort_by=sort_by,
        sort_descending=sort_descending,
        offset=offset,
        limit=limit,
        sample_n=sample_n,
        thresholds=thresholds,
    )

    df = analysis.cachegrind_df(run)
    df = apply_filters(df, spec)
    return formatters.format_filtered(df, "Cachegrind per-line data", spec)


@mcp.tool()
async def valgrind_analyze_callgraph(
    ctx: Context,
    run_id: str,
    caller_pattern: str | None = None,
    callee_pattern: str | None = None,
    exclude_functions: str | None = None,
    min_calls: int | None = None,
    min_cost: int | None = None,
    event: str = "Ir",
    sort_by: str | None = None,
    sort_descending: bool = True,
    offset: int = 0,
    limit: int = 50,
    workspace_id: str | None = None,
) -> str:
    """Analyze callgrind call graph — caller/callee relationships with costs.

    Args:
        run_id: The run_id from a callgrind run
        caller_pattern: Regex to filter caller functions
        callee_pattern: Regex to filter callee functions
        exclude_functions: Regex to exclude from both caller and callee
        min_calls: Minimum call count
        min_cost: Minimum cost for the event
        event: Cost event (default Ir)
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return
        workspace_id: Workspace containing the run
    """
    _, run = get_run(ctx, run_id, workspace_id)
    if not isinstance(run, CallgrindResult):
        return f"Run {run_id} is not a callgrind run (tool={run.tool})"

    df = analysis.call_graph_summary(run)
    if df.is_empty():
        return "No call graph data available."

    if caller_pattern:
        df = df.filter(pl.col("caller").str.contains(f"(?i){caller_pattern}"))
    if callee_pattern:
        df = df.filter(pl.col("callee").str.contains(f"(?i){callee_pattern}"))
    if exclude_functions:
        df = df.filter(
            ~pl.col("caller").str.contains(f"(?i){exclude_functions}")
            & ~pl.col("callee").str.contains(f"(?i){exclude_functions}")
        )

    thresholds: dict[str, tuple[float | None, float | None]] = {}
    if min_calls is not None:
        thresholds["call_count"] = (min_calls, None)
    cost_col = f"cost_{event}"
    if min_cost is not None and cost_col in df.columns:
        thresholds[cost_col] = (min_cost, None)

    spec = build_filter_spec(
        sort_by=sort_by or "call_count",
        sort_descending=sort_descending,
        offset=offset,
        limit=limit,
        thresholds=thresholds,
    )

    df = apply_filters(df, spec)
    return formatters.format_filtered(df, f"Call graph (event: {event})", spec)
