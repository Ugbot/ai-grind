"""Error analysis tools for memcheck, helgrind, and drd results."""

from __future__ import annotations

import re as re_mod

import polars as pl
from mcp.server.fastmcp import Context

from valgrind_mcp import analysis, formatters
from valgrind_mcp.filters import FilterSpec, apply_filters, build_filter_spec
from valgrind_mcp.models import MemcheckResult, ThreadCheckResult
from valgrind_mcp.server import get_run, mcp


@mcp.tool()
async def valgrind_analyze_errors(
    ctx: Context,
    run_id: str,
    group_by: str = "kind",
    top_n: int = 20,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    kind_pattern: str | None = None,
    exclude_files: str | None = None,
    exclude_functions: str | None = None,
    min_bytes: int | None = None,
    min_stack_depth: int | None = None,
    max_stack_depth: int | None = None,
    thread_ids: list[int] | None = None,
    sort_by: str | None = None,
    sort_descending: bool = True,
    offset: int = 0,
    limit: int | None = None,
    sample_n: int | None = None,
    sample_every: int | None = None,
    stratify_by: str | None = None,
    workspace_id: str | None = None,
) -> str:
    """Analyze errors from a memcheck, helgrind, or drd run with rich filtering.

    Groups errors by kind, function, file, or "raw" for ungrouped rows.
    Supports regex filtering, exclusion patterns, thresholds, pagination, and sampling.

    Args:
        run_id: The run_id from a previous run tool call
        group_by: How to group — "kind", "function", "file", or "raw"
        top_n: Max groups to show (ignored with limit/offset)
        file_pattern: Regex to include only matching files (case-insensitive)
        function_pattern: Regex to include only matching functions
        kind_pattern: Regex to include only matching error kinds
        exclude_files: Regex to exclude files
        exclude_functions: Regex to exclude functions
        min_bytes: Minimum bytes leaked (memcheck only)
        min_stack_depth: Only errors with stack depth >= this
        max_stack_depth: Only errors with stack depth <= this
        thread_ids: Only errors from these thread IDs
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return
        sample_n: Random sample of N rows
        sample_every: Take every Nth row
        stratify_by: Stratified sampling column
        workspace_id: Workspace containing the run
    """
    _, run = get_run(ctx, run_id, workspace_id)

    spec = build_filter_spec(
        file_pattern=file_pattern,
        function_pattern=function_pattern,
        kind_pattern=kind_pattern,
        exclude_files=exclude_files,
        exclude_functions=exclude_functions,
        min_bytes=min_bytes,
        min_stack_depth=min_stack_depth,
        max_stack_depth=max_stack_depth,
        thread_ids=thread_ids,
        sort_by=sort_by,
        sort_descending=sort_descending,
        offset=offset,
        limit=limit,
        sample_n=sample_n,
        sample_every=sample_every,
        stratify_by=stratify_by,
    )

    if isinstance(run, MemcheckResult):
        return _analyze_memcheck(run, group_by, top_n, spec)
    if isinstance(run, ThreadCheckResult):
        return _analyze_threadcheck(run, group_by, top_n, spec)
    return f"Run {run_id} is not a memcheck/helgrind/drd run (tool={run.tool})"


def _analyze_memcheck(
    run: MemcheckResult,
    group_by: str,
    top_n: int,
    spec: FilterSpec,
) -> str:
    raw_df = analysis.memcheck_errors_df(run)
    raw_df = apply_filters(raw_df, spec)

    if group_by == "raw" or raw_df.is_empty():
        return formatters.format_filtered(raw_df, f"Memcheck errors ({group_by})", spec)

    if group_by == "function":
        df = (
            raw_df.filter(pl.col("top_function").is_not_null())
            .group_by("top_function")
            .agg(
                pl.len().alias("count"),
                pl.col("kind").n_unique().alias("unique_kinds"),
                pl.col("bytes_leaked").sum().alias("total_bytes_leaked"),
            )
            .sort("count", descending=True)
            .head(top_n)
        )
    elif group_by == "file":
        df = (
            raw_df.filter(pl.col("top_file").is_not_null())
            .group_by("top_file")
            .agg(
                pl.len().alias("count"),
                pl.col("kind").n_unique().alias("unique_kinds"),
                pl.col("bytes_leaked").sum().alias("total_bytes_leaked"),
            )
            .sort("count", descending=True)
            .head(top_n)
        )
    else:
        df = (
            raw_df.group_by("kind")
            .agg(
                pl.len().alias("count"),
                pl.col("bytes_leaked").sum().alias("total_bytes_leaked"),
                pl.col("blocks_leaked").sum().alias("total_blocks_leaked"),
            )
            .sort("count", descending=True)
            .head(top_n)
        )
    return formatters.format_filtered(df, f"Memcheck errors by {group_by}", spec)


def _analyze_threadcheck(
    run: ThreadCheckResult,
    group_by: str,
    top_n: int,
    spec: FilterSpec,
) -> str:
    raw_df = analysis.threadcheck_errors_df(run)
    raw_df = apply_filters(raw_df, spec)

    if group_by == "raw" or raw_df.is_empty():
        return formatters.format_filtered(raw_df, f"Thread errors ({group_by})", spec)

    if group_by == "function":
        df = (
            raw_df.filter(pl.col("top_function").is_not_null())
            .group_by("top_function")
            .agg(pl.len().alias("count"), pl.col("kind").n_unique().alias("unique_kinds"))
            .sort("count", descending=True)
            .head(top_n)
        )
    else:
        df = raw_df.group_by("kind").agg(pl.len().alias("count")).sort("count", descending=True).head(top_n)
    return formatters.format_filtered(df, f"Thread errors by {group_by}", spec)


@mcp.tool()
async def valgrind_get_error_details(
    ctx: Context,
    run_id: str,
    error_index: int | None = None,
    kind: str | None = None,
    file_pattern: str | None = None,
    function_pattern: str | None = None,
    exclude_files: str | None = None,
    exclude_functions: str | None = None,
    min_bytes: int | None = None,
    thread_ids: list[int] | None = None,
    offset: int = 0,
    limit: int = 10,
    workspace_id: str | None = None,
) -> str:
    """Get detailed error info with full stack traces, with rich filtering.

    Args:
        run_id: The run_id from a memcheck/helgrind/drd run
        error_index: Show only the Nth error (0-based, bypasses other filters)
        kind: Exact error kind filter
        file_pattern: Regex matching any file in the error's stack
        function_pattern: Regex matching any function in the error's stack
        exclude_files: Regex to exclude errors with matching files
        exclude_functions: Regex to exclude errors with matching functions
        min_bytes: Minimum bytes leaked (memcheck leaks only)
        thread_ids: Only errors from these thread IDs
        offset: Skip first N matching errors
        limit: Max errors to show (default 10)
        workspace_id: Workspace containing the run
    """
    _, run = get_run(ctx, run_id, workspace_id)

    if isinstance(run, (MemcheckResult, ThreadCheckResult)):
        errors = list(run.errors)
    else:
        return f"Run {run_id} is not an error-producing run (tool={run.tool})"

    if error_index is not None:
        if 0 <= error_index < len(errors):
            return formatters.format_error_details([errors[error_index]])
        return f"Error index {error_index} out of range (0-{len(errors) - 1})"

    if kind:
        errors = [e for e in errors if e.kind == kind]
    if file_pattern:
        pat = re_mod.compile(file_pattern, re_mod.IGNORECASE)
        errors = [e for e in errors if any(f.file and pat.search(f.file) for f in e.stack)]
    if function_pattern:
        pat = re_mod.compile(function_pattern, re_mod.IGNORECASE)
        errors = [e for e in errors if any(f.fn and pat.search(f.fn) for f in e.stack)]
    if exclude_files:
        pat = re_mod.compile(exclude_files, re_mod.IGNORECASE)
        errors = [e for e in errors if not any(f.file and pat.search(f.file) for f in e.stack)]
    if exclude_functions:
        pat = re_mod.compile(exclude_functions, re_mod.IGNORECASE)
        errors = [e for e in errors if not any(f.fn and pat.search(f.fn) for f in e.stack)]
    if min_bytes is not None:
        errors = [
            e
            for e in errors
            if hasattr(e, "bytes_leaked") and e.bytes_leaked is not None and e.bytes_leaked >= min_bytes
        ]
    if thread_ids is not None:
        errors = [e for e in errors if hasattr(e, "thread_id") and e.thread_id in thread_ids]

    total_matching = len(errors)
    errors = errors[offset : offset + limit]

    if not errors:
        return f"No errors matching filters (0 of {total_matching} after offset {offset})"

    result = formatters.format_error_details(errors, max_errors=limit)
    result += f"\n\nShowing {len(errors)} of {total_matching} matching errors (offset={offset})"
    return result
