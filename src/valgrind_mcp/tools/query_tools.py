"""General-purpose query tool for ad-hoc analysis."""

from __future__ import annotations

from mcp.server.fastmcp import Context

from valgrind_mcp import analysis, formatters
from valgrind_mcp.filters import apply_filters, build_filter_spec
from valgrind_mcp.models import (
    CachegrindResult,
    CallgrindResult,
    MassifResult,
    MemcheckResult,
    ThreadCheckResult,
)
from valgrind_mcp.server import get_run, mcp


@mcp.tool()
async def valgrind_query(
    ctx: Context,
    run_id: str,
    columns: list[str] | None = None,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    kind_pattern: str | None = None,
    exclude_files: str | None = None,
    exclude_functions: str | None = None,
    min_bytes: int | None = None,
    max_bytes: int | None = None,
    sort_by: str | None = None,
    sort_descending: bool = True,
    offset: int = 0,
    limit: int = 50,
    sample_n: int | None = None,
    sample_fraction: float | None = None,
    sample_every: int | None = None,
    sample_seed: int | None = None,
    stratify_by: str | None = None,
    time_min: int | None = None,
    time_max: int | None = None,
    thresholds: dict[str, list[float | None]] | None = None,
    workspace_id: str | None = None,
) -> str:
    """General-purpose query for any valgrind run data.

    Builds the raw DataFrame and applies arbitrary filters, column selection,
    sorting, sampling, and pagination. Pass columns=["schema"] to list columns.

    Args:
        run_id: The run_id from any previous run
        columns: Columns to return (None=all, ["schema"]=list columns)
        file_pattern: Regex include on files
        function_pattern: Regex include on functions
        kind_pattern: Regex include on kinds
        exclude_files: Regex to exclude files
        exclude_functions: Regex to exclude functions
        min_bytes: Minimum bytes
        max_bytes: Maximum bytes
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows (default 50)
        sample_n: Random sample of N rows
        sample_fraction: Random fraction (0.0-1.0)
        sample_every: Every Nth row
        sample_seed: Seed for reproducible sampling
        stratify_by: Column for stratified sampling
        time_min: Min time (massif)
        time_max: Max time (massif)
        thresholds: Dict of column -> [min, max]
        workspace_id: Workspace containing the run
    """
    _, run = get_run(ctx, run_id, workspace_id)

    if isinstance(run, MemcheckResult):
        df = analysis.memcheck_errors_df(run)
    elif isinstance(run, ThreadCheckResult):
        df = analysis.threadcheck_errors_df(run)
    elif isinstance(run, CallgrindResult):
        df = analysis.callgrind_df(run)
    elif isinstance(run, CachegrindResult):
        df = analysis.cachegrind_df(run)
    elif isinstance(run, MassifResult):
        df = analysis.massif_timeline_df(run)
    else:
        return f"Unknown run type: {type(run)}"

    if columns == ["schema"]:
        schema_info = [f"- `{col}`: {dtype}" for col, dtype in df.schema.items()]
        return f"**Schema for {run.tool} run `{run_id}`:**\n\n" + "\n".join(schema_info) + f"\n\n{len(df)} total rows"

    threshold_tuples: dict[str, tuple[float | None, float | None]] = {}
    if thresholds:
        for col, bounds in thresholds.items():
            if isinstance(bounds, list) and len(bounds) == 2:
                threshold_tuples[col] = (bounds[0], bounds[1])

    spec = build_filter_spec(
        file_pattern=file_pattern,
        function_pattern=function_pattern,
        kind_pattern=kind_pattern,
        exclude_files=exclude_files,
        exclude_functions=exclude_functions,
        min_bytes=min_bytes,
        max_bytes=max_bytes,
        sort_by=sort_by,
        sort_descending=sort_descending,
        offset=offset,
        limit=limit,
        sample_n=sample_n,
        sample_fraction=sample_fraction,
        sample_every=sample_every,
        sample_seed=sample_seed,
        stratify_by=stratify_by,
        time_min=time_min,
        time_max=time_max,
        thresholds=threshold_tuples,
    )

    df = apply_filters(df, spec)

    if columns and columns != ["schema"]:
        valid_cols = [c for c in columns if c in df.columns]
        if valid_cols:
            df = df.select(valid_cols)
        else:
            return f"None of the requested columns exist. Available: {df.columns}"

    return formatters.format_filtered(df, f"Query results ({run.tool})", spec)
