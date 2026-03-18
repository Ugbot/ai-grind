"""Analysis tools: devtools_analyze, devtools_query, devtools_compare."""

from __future__ import annotations

import polars as pl
from mcp.server.fastmcp import Context

from devtools_mcp.filters import apply_filters, build_filter_spec
from devtools_mcp.formatters import format_filtered
from devtools_mcp.registry import get_backend
from devtools_mcp.server import get_run, mcp


@mcp.tool()
async def devtools_analyze(
    ctx: Context,
    run_id: str,
    group_by: str | None = None,
    top_n: int = 20,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    kind_pattern: str | None = None,
    exclude_files: str | None = None,
    exclude_functions: str | None = None,
    min_bytes: int | None = None,
    sort_by: str | None = None,
    sort_descending: bool = True,
    offset: int = 0,
    limit: int | None = None,
    sample_n: int | None = None,
    sample_every: int | None = None,
    workspace_id: str | None = None,
) -> str:
    """Analyze a run result with rich filtering.

    Builds a DataFrame from the run, applies filters, and returns a focused table.
    Works with any tool suite (valgrind, dtrace, perf, lldb snapshots).

    Args:
        run_id: The run_id from a previous devtools_run or debug_inspect
        group_by: Column to group by (e.g. "kind", "function", "file")
        top_n: Max groups when using group_by
        file_pattern: Regex to include only matching files
        function_pattern: Regex to include only matching functions
        kind_pattern: Regex to include only matching error/event kinds
        exclude_files: Regex to exclude files (e.g. "/usr/lib|vg_replace")
        exclude_functions: Regex to exclude functions (e.g. "^__|^std::")
        min_bytes: Minimum bytes threshold
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return
        sample_n: Random sample of N rows
        sample_every: Take every Nth row
        workspace_id: Workspace containing the run
    """
    ws, run = get_run(ctx, run_id, workspace_id)

    try:
        backend = get_backend(run.suite)
    except KeyError:
        return f"No backend registered for suite '{run.suite}'"

    builder = backend.df_builders.get(run.tool) or backend.df_builders.get("_default")
    if not builder:
        return f"No DataFrame builder for {run.suite}:{run.tool}"

    df = builder(run)
    if df.is_empty():
        return f"No data in run `{run_id}` ({run.suite}:{run.tool})"

    spec = build_filter_spec(
        file_pattern=file_pattern,
        function_pattern=function_pattern,
        kind_pattern=kind_pattern,
        exclude_files=exclude_files,
        exclude_functions=exclude_functions,
        min_bytes=min_bytes,
        sort_by=sort_by,
        sort_descending=sort_descending,
        offset=offset,
        limit=limit,
        sample_n=sample_n,
        sample_every=sample_every,
    )

    df = apply_filters(df, spec)

    if group_by and group_by in df.columns:
        numeric_cols = [c for c in df.columns if c != group_by and df[c].dtype in (pl.Int64, pl.Float64, pl.UInt64)]
        aggs = [pl.len().alias("count")]
        for col in numeric_cols[:5]:
            aggs.append(pl.col(col).sum().alias(f"total_{col}"))
        df = df.group_by(group_by).agg(aggs).sort("count", descending=True).head(top_n)

    return format_filtered(df, f"{run.suite}:{run.tool} analysis", spec)


@mcp.tool()
async def devtools_query(
    ctx: Context,
    run_id: str,
    columns: list[str] | None = None,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    kind_pattern: str | None = None,
    exclude_files: str | None = None,
    exclude_functions: str | None = None,
    sort_by: str | None = None,
    sort_descending: bool = True,
    offset: int = 0,
    limit: int = 50,
    sample_n: int | None = None,
    workspace_id: str | None = None,
) -> str:
    """Ad-hoc DataFrame query on any run. Pass columns=["schema"] to list available columns.

    Args:
        run_id: The run_id from any previous run
        columns: Columns to return (None=all, ["schema"]=list columns)
        file_pattern: Regex include on files
        function_pattern: Regex include on functions
        kind_pattern: Regex include on kinds
        exclude_files: Regex to exclude files
        exclude_functions: Regex to exclude functions
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows (default 50)
        sample_n: Random sample of N rows
        workspace_id: Workspace containing the run
    """
    ws, run = get_run(ctx, run_id, workspace_id)

    try:
        backend = get_backend(run.suite)
    except KeyError:
        return f"No backend for suite '{run.suite}'"

    builder = backend.df_builders.get(run.tool) or backend.df_builders.get("_default")
    if not builder:
        return f"No DataFrame builder for {run.suite}:{run.tool}"

    df = builder(run)

    if columns == ["schema"]:
        schema_info = [f"- `{col}`: {dtype}" for col, dtype in df.schema.items()]
        header = f"**Schema for {run.suite}:{run.tool} `{run_id}`:**\n\n"
        return header + "\n".join(schema_info) + f"\n\n{len(df)} total rows"

    spec = build_filter_spec(
        file_pattern=file_pattern,
        function_pattern=function_pattern,
        kind_pattern=kind_pattern,
        exclude_files=exclude_files,
        exclude_functions=exclude_functions,
        sort_by=sort_by,
        sort_descending=sort_descending,
        offset=offset,
        limit=limit,
        sample_n=sample_n,
    )

    df = apply_filters(df, spec)

    if columns and columns != ["schema"]:
        valid_cols = [c for c in columns if c in df.columns]
        if valid_cols:
            df = df.select(valid_cols)
        else:
            return f"No matching columns. Available: {df.columns}"

    return format_filtered(df, f"Query: {run.suite}:{run.tool}", spec)


@mcp.tool()
async def devtools_compare(
    ctx: Context,
    run_id_a: str,
    run_id_b: str,
    function_pattern: str | None = None,
    exclude_functions: str | None = None,
    min_delta: int | None = None,
    sort_by: str | None = None,
    offset: int = 0,
    limit: int = 50,
    workspace_id: str | None = None,
) -> str:
    """Compare two runs of the same tool type. Shows deltas.

    Args:
        run_id_a: Baseline run
        run_id_b: Comparison run
        function_pattern: Regex to include only matching functions
        exclude_functions: Regex to exclude functions
        min_delta: Minimum absolute delta to include
        sort_by: Column to sort by
        offset: Skip first N rows
        limit: Max rows
        workspace_id: Workspace containing both runs
    """
    ws_a, run_a = get_run(ctx, run_id_a, workspace_id)
    _, run_b = get_run(ctx, run_id_b, workspace_id)

    if run_a.suite != run_b.suite or run_a.tool != run_b.tool:
        return f"Cannot compare {run_a.suite}:{run_a.tool} with {run_b.suite}:{run_b.tool}"

    # Try suite-specific comparison first
    if run_a.suite == "valgrind":
        from devtools_mcp.valgrind import analysis as vg_analysis
        from devtools_mcp.valgrind.models import CallgrindResult, MassifResult, MemcheckResult

        if isinstance(run_a, MemcheckResult) and isinstance(run_b, MemcheckResult):
            df = vg_analysis.compare_memcheck(run_a, run_b)
        elif isinstance(run_a, CallgrindResult) and isinstance(run_b, CallgrindResult):
            df = vg_analysis.compare_callgrind(run_a, run_b)
        elif isinstance(run_a, MassifResult) and isinstance(run_b, MassifResult):
            info = vg_analysis.compare_massif(run_a, run_b)
            parts = ["**Comparison (A → B):**", ""]
            for k, v in info.items():
                parts.append(f"- {k}: {v:,}" if isinstance(v, int) else f"- {k}: {v:.1f}")
            return "\n".join(parts)
        else:
            return f"No comparison for {run_a.tool}"

        if min_delta is not None:
            delta_cols = [c for c in df.columns if "delta" in c]
            for col in delta_cols:
                df = df.filter(pl.col(col).abs() >= min_delta)

        spec = build_filter_spec(
            function_pattern=function_pattern,
            exclude_functions=exclude_functions,
            sort_by=sort_by,
            offset=offset,
            limit=limit,
        )
        df = apply_filters(df, spec)
        return format_filtered(df, f"Comparison: {run_a.suite}:{run_a.tool} (A → B)", spec)

    return f"Comparison not yet implemented for suite '{run_a.suite}'"
