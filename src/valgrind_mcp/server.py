"""FastMCP server for Valgrind tool suite.

Exposes MCP tools for running valgrind, analyzing results with rich filtering
and sampling, and comparing runs.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncIterator

import polars as pl
from mcp.server.fastmcp import FastMCP, Context

from valgrind_mcp import analysis, formatters
from valgrind_mcp.filters import FilterSpec, apply_filters, build_filter_spec, describe_active_filters
from valgrind_mcp.models import (
    CallgrindResult,
    CachegrindResult,
    MassifResult,
    MemcheckResult,
    ThreadCheckResult,
    ValgrindRun,
)
from valgrind_mcp.parsers import (
    parse_callgrind,
    parse_cachegrind,
    parse_massif,
    parse_memcheck_xml,
    parse_threadcheck_xml,
)
from valgrind_mcp.runner import check_valgrind, run_valgrind
from valgrind_mcp.workspace import AppContext


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Create default workspace on startup, clean up on shutdown."""
    ctx = AppContext()
    ws = ctx.create_workspace("default")
    ctx.default_workspace_id = ws.workspace_id
    try:
        yield ctx
    finally:
        ctx.cleanup_all()


mcp = FastMCP(
    "valgrind-mcp",
    lifespan=app_lifespan,
    instructions=(
        "Valgrind MCP server. Run valgrind tools (memcheck, helgrind, drd, callgrind, "
        "cachegrind, massif) against binaries and analyze results with Polars. "
        "All analysis tools support rich filtering: file/function regex, exclude patterns, "
        "numeric thresholds, sorting, pagination (offset/limit), and sampling "
        "(random, every-nth, stratified). Use valgrind_check() first to verify installation. "
        "Run tools return a run_id for subsequent analysis."
    ),
)


def _get_ctx(ctx: Context) -> AppContext:
    """Extract AppContext from MCP context."""
    return ctx.request_context.lifespan_context


def _make_run_base(tool: str, binary: str, args: list[str], valgrind_args: list[str], duration: float, exit_code: int) -> ValgrindRun:
    """Create a base ValgrindRun for parser input."""
    return ValgrindRun(
        run_id=str(uuid.uuid4()),
        tool=tool,
        binary=binary,
        args=args,
        valgrind_args=valgrind_args,
        timestamp=datetime.now(timezone.utc),
        exit_code=exit_code,
        duration_seconds=duration,
    )


def _format_filtered(df, title: str, spec: FilterSpec, max_rows: int = 50) -> str:
    """Format a DataFrame with filter description header."""
    desc = describe_active_filters(spec)
    header = f"**{title}**"
    if desc != "no filters":
        header += f"\nFilters: {desc}"
    header += f"\nTotal rows after filtering: {len(df)}"
    return header + "\n\n" + formatters.format_dataframe(df, max_rows=max_rows)


# ============================================================
# Utility Tools
# ============================================================


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
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    raw_path = ws.get_raw_path(run_id)

    try:
        with open(raw_path, "r", errors="replace") as f:
            content = f.read()
    except FileNotFoundError:
        return f"Raw output file not found: {raw_path}"

    max_len = 50_000
    if len(content) > max_len:
        return content[:max_len] + f"\n\n... truncated ({len(content):,} total bytes)"
    return content


# ============================================================
# Workspace Tools
# ============================================================


@mcp.tool()
async def valgrind_create_workspace(ctx: Context, name: str = "default") -> str:
    """Create a new workspace for organizing valgrind runs.

    Returns the workspace_id to use with other tools.
    """
    app = _get_ctx(ctx)
    ws = app.create_workspace(name)
    return f"Created workspace `{name}` with ID: `{ws.workspace_id}`"


@mcp.tool()
async def valgrind_list_runs(ctx: Context, workspace_id: str | None = None) -> str:
    """List all valgrind runs in a workspace with summary info."""
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    runs = ws.list_runs()

    if not runs:
        return f"No runs in workspace `{ws.name}` (`{ws.workspace_id}`)."

    parts = [f"**Workspace:** `{ws.name}` ({len(runs)} run(s))", ""]
    for run in runs:
        parts.append(
            f"- `{run['run_id']}` | {run['tool']} | {run['binary']} | "
            f"{run['duration']} | exit {run['exit_code']}"
        )
    return "\n".join(parts)


# ============================================================
# Run Tools
# ============================================================


@mcp.tool()
async def valgrind_memcheck(
    ctx: Context,
    binary: str,
    args: list[str] | None = None,
    valgrind_args: list[str] | None = None,
    timeout: int = 300,
    workspace_id: str | None = None,
) -> str:
    """Run Valgrind's memcheck (memory error detector) against a binary.

    Detects memory leaks, invalid reads/writes, use of uninitialized values,
    and other memory errors. Returns a summary with run_id for deeper analysis.

    Args:
        binary: Path to the executable to analyze
        args: Arguments to pass to the binary
        valgrind_args: Extra valgrind flags (e.g. ["--suppressions=file.supp"])
        timeout: Max seconds to wait (default 300)
        workspace_id: Workspace to store results (uses default if omitted)
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    result = await run_valgrind(
        tool="memcheck", binary=binary, binary_args=args,
        valgrind_args=valgrind_args, timeout=timeout,
    )

    if result.exit_code == -1:
        return f"Memcheck failed: {result.stderr}"

    run_base = _make_run_base("memcheck", binary, args or [], valgrind_args or [],
                               result.duration_seconds, result.exit_code)
    parsed = parse_memcheck_xml(result.output_path, run_base)
    ws.store_run(parsed, result.output_path)

    return formatters.format_memcheck_summary(parsed)


@mcp.tool()
async def valgrind_helgrind(
    ctx: Context,
    binary: str,
    args: list[str] | None = None,
    valgrind_args: list[str] | None = None,
    timeout: int = 300,
    workspace_id: str | None = None,
) -> str:
    """Run Valgrind's Helgrind (thread error detector) against a binary.

    Detects data races, lock ordering problems, and POSIX threading API misuse.
    Returns a summary with run_id for deeper analysis.

    Args:
        binary: Path to the executable to analyze
        args: Arguments to pass to the binary
        valgrind_args: Extra valgrind flags
        timeout: Max seconds to wait (default 300)
        workspace_id: Workspace to store results
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    result = await run_valgrind(
        tool="helgrind", binary=binary, binary_args=args,
        valgrind_args=valgrind_args, timeout=timeout,
    )

    if result.exit_code == -1:
        return f"Helgrind failed: {result.stderr}"

    run_base = _make_run_base("helgrind", binary, args or [], valgrind_args or [],
                               result.duration_seconds, result.exit_code)
    parsed = parse_threadcheck_xml(result.output_path, run_base, tool="helgrind")
    ws.store_run(parsed, result.output_path)

    return formatters.format_threadcheck_summary(parsed)


@mcp.tool()
async def valgrind_drd(
    ctx: Context,
    binary: str,
    args: list[str] | None = None,
    valgrind_args: list[str] | None = None,
    timeout: int = 300,
    workspace_id: str | None = None,
) -> str:
    """Run Valgrind's DRD (thread error detector) against a binary.

    Alternative to Helgrind — detects data races and lock contention.
    Better performance but less context than Helgrind.

    Args:
        binary: Path to the executable to analyze
        args: Arguments to pass to the binary
        valgrind_args: Extra valgrind flags
        timeout: Max seconds to wait (default 300)
        workspace_id: Workspace to store results
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    result = await run_valgrind(
        tool="drd", binary=binary, binary_args=args,
        valgrind_args=valgrind_args, timeout=timeout,
    )

    if result.exit_code == -1:
        return f"DRD failed: {result.stderr}"

    run_base = _make_run_base("drd", binary, args or [], valgrind_args or [],
                               result.duration_seconds, result.exit_code)
    parsed = parse_threadcheck_xml(result.output_path, run_base, tool="drd")
    ws.store_run(parsed, result.output_path)

    return formatters.format_threadcheck_summary(parsed)


@mcp.tool()
async def valgrind_callgrind(
    ctx: Context,
    binary: str,
    args: list[str] | None = None,
    valgrind_args: list[str] | None = None,
    timeout: int = 300,
    workspace_id: str | None = None,
) -> str:
    """Run Valgrind's Callgrind (call-graph profiler) against a binary.

    Profiles function call costs, instruction counts, cache behavior,
    and branch prediction. Returns hotspots with run_id for deeper analysis.

    Args:
        binary: Path to the executable to analyze
        args: Arguments to pass to the binary
        valgrind_args: Extra valgrind flags
        timeout: Max seconds to wait (default 300)
        workspace_id: Workspace to store results
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    result = await run_valgrind(
        tool="callgrind", binary=binary, binary_args=args,
        valgrind_args=valgrind_args, timeout=timeout,
    )

    if result.exit_code == -1:
        return f"Callgrind failed: {result.stderr}"

    run_base = _make_run_base("callgrind", binary, args or [], valgrind_args or [],
                               result.duration_seconds, result.exit_code)
    parsed = parse_callgrind(result.output_path, run_base)
    ws.store_run(parsed, result.output_path)

    return formatters.format_callgrind_summary(parsed)


@mcp.tool()
async def valgrind_cachegrind(
    ctx: Context,
    binary: str,
    args: list[str] | None = None,
    valgrind_args: list[str] | None = None,
    timeout: int = 300,
    workspace_id: str | None = None,
) -> str:
    """Run Valgrind's Cachegrind (cache profiler) against a binary.

    Profiles cache miss rates and branch prediction at the source line level.
    Returns summary with run_id for deeper analysis.

    Args:
        binary: Path to the executable to analyze
        args: Arguments to pass to the binary
        valgrind_args: Extra valgrind flags
        timeout: Max seconds to wait (default 300)
        workspace_id: Workspace to store results
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    result = await run_valgrind(
        tool="cachegrind", binary=binary, binary_args=args,
        valgrind_args=valgrind_args, timeout=timeout,
    )

    if result.exit_code == -1:
        return f"Cachegrind failed: {result.stderr}"

    run_base = _make_run_base("cachegrind", binary, args or [], valgrind_args or [],
                               result.duration_seconds, result.exit_code)
    parsed = parse_cachegrind(result.output_path, run_base)
    ws.store_run(parsed, result.output_path)

    return formatters.format_cachegrind_summary(parsed)


@mcp.tool()
async def valgrind_massif(
    ctx: Context,
    binary: str,
    args: list[str] | None = None,
    valgrind_args: list[str] | None = None,
    timeout: int = 300,
    workspace_id: str | None = None,
) -> str:
    """Run Valgrind's Massif (heap profiler) against a binary.

    Profiles heap memory usage over time, showing allocation trees
    and peak memory consumption. Returns summary with run_id for deeper analysis.

    Args:
        binary: Path to the executable to analyze
        args: Arguments to pass to the binary
        valgrind_args: Extra valgrind flags
        timeout: Max seconds to wait (default 300)
        workspace_id: Workspace to store results
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    result = await run_valgrind(
        tool="massif", binary=binary, binary_args=args,
        valgrind_args=valgrind_args, timeout=timeout,
    )

    if result.exit_code == -1:
        return f"Massif failed: {result.stderr}"

    run_base = _make_run_base("massif", binary, args or [], valgrind_args or [],
                               result.duration_seconds, result.exit_code)
    parsed = parse_massif(result.output_path, run_base)
    ws.store_run(parsed, result.output_path)

    return formatters.format_massif_summary(parsed)


# ============================================================
# Analysis Tools (all with rich filtering + sampling)
# ============================================================


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

    Groups errors by kind, function, or file with aggregated counts.
    Supports regex filtering, exclusion patterns, thresholds, pagination, and sampling.

    Args:
        run_id: The run_id from a previous run tool call
        group_by: How to group — "kind", "function", "file", or "raw" for ungrouped rows
        top_n: Max groups to show (ignored when using limit/offset)
        file_pattern: Regex to include only matching files (case-insensitive)
        function_pattern: Regex to include only matching functions
        kind_pattern: Regex to include only matching error kinds (e.g. "Leak|Invalid")
        exclude_files: Regex to exclude files (e.g. "/usr/lib|/lib64|vg_replace")
        exclude_functions: Regex to exclude functions (e.g. "^__|^std::")
        min_bytes: Minimum bytes leaked (memcheck only)
        min_stack_depth: Only errors with stack depth >= this
        max_stack_depth: Only errors with stack depth <= this
        thread_ids: Only errors from these thread IDs (helgrind/drd)
        sort_by: Column to sort by (overrides default)
        sort_descending: Sort direction (default descending)
        offset: Skip first N rows
        limit: Max rows to return
        sample_n: Random sample of N rows
        sample_every: Take every Nth row
        stratify_by: Stratified sampling: sample_n per group in this column
        workspace_id: Workspace containing the run
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    spec = build_filter_spec(
        file_pattern=file_pattern, function_pattern=function_pattern,
        kind_pattern=kind_pattern, exclude_files=exclude_files,
        exclude_functions=exclude_functions, min_bytes=min_bytes,
        min_stack_depth=min_stack_depth, max_stack_depth=max_stack_depth,
        thread_ids=thread_ids, sort_by=sort_by, sort_descending=sort_descending,
        offset=offset, limit=limit, sample_n=sample_n,
        sample_every=sample_every, stratify_by=stratify_by,
    )

    if isinstance(run, MemcheckResult):
        if group_by == "raw":
            df = analysis.memcheck_errors_df(run)
            df = apply_filters(df, spec)
            return _format_filtered(df, "Memcheck errors (raw)", spec)
        # For grouped views, filter the raw df first, then group
        raw_df = analysis.memcheck_errors_df(run)
        raw_df = apply_filters(raw_df, spec)
        if raw_df.is_empty():
            return _format_filtered(raw_df, f"Memcheck errors by {group_by}", spec)
        if group_by == "function":
            df = (raw_df.filter(pl.col("top_function").is_not_null())
                  .group_by("top_function")
                  .agg(pl.len().alias("count"),
                       pl.col("kind").n_unique().alias("unique_kinds"),
                       pl.col("bytes_leaked").sum().alias("total_bytes_leaked"))
                  .sort("count", descending=True).head(top_n))
        elif group_by == "file":
            df = (raw_df.filter(pl.col("top_file").is_not_null())
                  .group_by("top_file")
                  .agg(pl.len().alias("count"),
                       pl.col("kind").n_unique().alias("unique_kinds"),
                       pl.col("bytes_leaked").sum().alias("total_bytes_leaked"))
                  .sort("count", descending=True).head(top_n))
        else:
            df = (raw_df.group_by("kind")
                  .agg(pl.len().alias("count"),
                       pl.col("bytes_leaked").sum().alias("total_bytes_leaked"),
                       pl.col("blocks_leaked").sum().alias("total_blocks_leaked"))
                  .sort("count", descending=True).head(top_n))
        return _format_filtered(df, f"Memcheck errors by {group_by}", spec)

    elif isinstance(run, ThreadCheckResult):
        if group_by == "raw":
            df = analysis.threadcheck_errors_df(run)
            df = apply_filters(df, spec)
            return _format_filtered(df, "Thread errors (raw)", spec)
        raw_df = analysis.threadcheck_errors_df(run)
        raw_df = apply_filters(raw_df, spec)
        if raw_df.is_empty():
            return _format_filtered(raw_df, f"Thread errors by {group_by}", spec)
        if group_by == "function":
            df = (raw_df.filter(pl.col("top_function").is_not_null())
                  .group_by("top_function")
                  .agg(pl.len().alias("count"),
                       pl.col("kind").n_unique().alias("unique_kinds"))
                  .sort("count", descending=True).head(top_n))
        else:
            df = (raw_df.group_by("kind")
                  .agg(pl.len().alias("count"))
                  .sort("count", descending=True).head(top_n))
        return _format_filtered(df, f"Thread errors by {group_by}", spec)

    return f"Run {run_id} is not a memcheck/helgrind/drd run (tool={run.tool})"


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

    Find top functions by cost for a given event, with regex file/function filters,
    cost thresholds, exclusion patterns, and pagination.

    Args:
        run_id: The run_id from a callgrind run
        event: Event to sort by (Ir=instructions, Dr=data reads, Dw=data writes, etc.)
        top_n: Number of top functions to show (before pagination)
        file_pattern: Regex to include only matching source files
        function_pattern: Regex to include only matching function names
        exclude_files: Regex to exclude files (e.g. "/usr/lib|libc")
        exclude_functions: Regex to exclude functions (e.g. "^_dl_|^__libc")
        min_cost: Minimum self cost for the event to include
        min_pct: Minimum self percentage to include (e.g. 1.0 for >= 1%)
        sort_by: Column to sort by (default: self_{event})
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return
        sample_n: Random sample of N rows
        workspace_id: Workspace containing the run
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    if not isinstance(run, CallgrindResult):
        return f"Run {run_id} is not a callgrind run (tool={run.tool})"

    thresholds = {}
    self_col = f"self_{event}"
    if min_cost is not None:
        thresholds[self_col] = (min_cost, None)
    if min_pct is not None:
        thresholds["self_pct"] = (min_pct, None)

    spec = build_filter_spec(
        file_pattern=file_pattern, function_pattern=function_pattern,
        exclude_files=exclude_files, exclude_functions=exclude_functions,
        sort_by=sort_by, sort_descending=sort_descending,
        offset=offset, limit=limit, sample_n=sample_n,
        thresholds=thresholds,
    )

    df = analysis.hotspots(run, event=event, top_n=top_n)
    df = apply_filters(df, spec)
    return _format_filtered(df, f"Callgrind hotspots by {event}", spec)


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
    """Analyze cachegrind results with rich filtering.

    Find functions with worst cache miss rates, filtered by file/function patterns,
    minimum miss rates, and minimum instruction counts.

    Args:
        run_id: The run_id from a cachegrind run
        top_n: Number of top functions to show
        file_pattern: Regex to include only matching source files
        function_pattern: Regex to include only matching function names
        exclude_files: Regex to exclude files
        exclude_functions: Regex to exclude functions
        min_miss_pct: Minimum I1 miss percentage to include
        min_ir: Minimum instruction references to include (filters noise from tiny functions)
        sort_by: Column to sort by (default: total_ir)
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return
        sample_n: Random sample of N rows
        workspace_id: Workspace containing the run
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    if not isinstance(run, CachegrindResult):
        return f"Run {run_id} is not a cachegrind run (tool={run.tool})"

    thresholds = {}
    if min_miss_pct is not None:
        thresholds["i1_miss_pct"] = (min_miss_pct, None)
    if min_ir is not None:
        thresholds["total_ir"] = (min_ir, None)

    spec = build_filter_spec(
        function_pattern=function_pattern, exclude_files=exclude_files,
        exclude_functions=exclude_functions, sort_by=sort_by,
        sort_descending=sort_descending, offset=offset, limit=limit,
        sample_n=sample_n, thresholds=thresholds,
        file_pattern=file_pattern,
    )

    df = analysis.cache_miss_rates(run, top_n=top_n)
    df = apply_filters(df, spec)
    return _format_filtered(df, "Cachegrind miss rates by function", spec)


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

    Shows per-line cache data with computed miss rates. Use file_pattern
    to drill into a specific source file.

    Args:
        run_id: The run_id from a cachegrind run
        file_pattern: Regex to include only matching source files (e.g. "parser\\.c")
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
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    if not isinstance(run, CachegrindResult):
        return f"Run {run_id} is not a cachegrind run (tool={run.tool})"

    thresholds = {}
    if min_ir is not None:
        thresholds["ir"] = (min_ir, None)
    if min_miss_rate is not None:
        thresholds["i1_miss_rate"] = (min_miss_rate, None)

    spec = build_filter_spec(
        file_pattern=file_pattern, function_pattern=function_pattern,
        exclude_files=exclude_files, exclude_functions=exclude_functions,
        sort_by=sort_by, sort_descending=sort_descending,
        offset=offset, limit=limit, sample_n=sample_n,
        thresholds=thresholds,
    )

    df = analysis.cachegrind_df(run)
    df = apply_filters(df, spec)
    return _format_filtered(df, "Cachegrind per-line data", spec)


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

    Shows memory timeline and peak allocation breakdown. Filter snapshots
    by time range, memory thresholds, or sample for long runs.

    Args:
        run_id: The run_id from a massif run
        time_min: Only snapshots at time >= this value
        time_max: Only snapshots at time <= this value
        min_bytes: Only snapshots with total_bytes >= this
        max_bytes: Only snapshots with total_bytes <= this
        detailed_only: Only show detailed/peak snapshots (with allocation trees)
        sample_every: Take every Nth snapshot (useful for runs with thousands)
        offset: Skip first N snapshots
        limit: Max snapshots to return
        workspace_id: Workspace containing the run
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    if not isinstance(run, MassifResult):
        return f"Run {run_id} is not a massif run (tool={run.tool})"

    spec = build_filter_spec(
        time_min=time_min, time_max=time_max,
        min_bytes=min_bytes, max_bytes=max_bytes,
        sample_every=sample_every, offset=offset, limit=limit,
    )

    parts = []

    # Timeline
    df = analysis.massif_timeline_df(run)
    if detailed_only:
        df = df.filter(pl.col("is_detailed") == True)
    df = apply_filters(df, spec)
    parts.append(_format_filtered(df, "Memory timeline", spec))

    # Peak allocations
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

    Filter by caller or callee function names, minimum call counts, and cost thresholds.

    Args:
        run_id: The run_id from a callgrind run
        caller_pattern: Regex to filter caller functions
        callee_pattern: Regex to filter callee functions
        exclude_functions: Regex to exclude from both caller and callee
        min_calls: Minimum call count to include
        min_cost: Minimum cost for the event to include
        event: Cost event to use for thresholds (default Ir)
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return
        workspace_id: Workspace containing the run
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    if not isinstance(run, CallgrindResult):
        return f"Run {run_id} is not a callgrind run (tool={run.tool})"

    df = analysis.call_graph_summary(run)
    if df.is_empty():
        return "No call graph data available."

    # Apply caller/callee pattern filters manually since they use different columns
    if caller_pattern:
        df = df.filter(pl.col("caller").str.contains(f"(?i){caller_pattern}"))
    if callee_pattern:
        df = df.filter(pl.col("callee").str.contains(f"(?i){callee_pattern}"))
    if exclude_functions:
        df = df.filter(
            ~pl.col("caller").str.contains(f"(?i){exclude_functions}")
            & ~pl.col("callee").str.contains(f"(?i){exclude_functions}")
        )

    thresholds = {}
    if min_calls is not None:
        thresholds["call_count"] = (min_calls, None)
    cost_col = f"cost_{event}"
    if min_cost is not None and cost_col in df.columns:
        thresholds[cost_col] = (min_cost, None)

    spec = build_filter_spec(
        sort_by=sort_by or "call_count", sort_descending=sort_descending,
        offset=offset, limit=limit, thresholds=thresholds,
    )

    df = apply_filters(df, spec)
    return _format_filtered(df, f"Call graph (event: {event})", spec)


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
    """Compare two valgrind runs with filtering on the comparison results.

    Shows deltas between runs — useful for before/after analysis.
    Filter to focus on specific files/functions or only significant changes.

    Args:
        run_id_a: First run (baseline)
        run_id_b: Second run (comparison)
        file_pattern: Regex to include only matching files
        function_pattern: Regex to include only matching functions
        exclude_files: Regex to exclude files
        exclude_functions: Regex to exclude functions
        min_delta: Minimum absolute delta to include (filters noise)
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return
        workspace_id: Workspace containing both runs
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run_a = ws.get_run(run_id_a)
    run_b = ws.get_run(run_id_b)

    if run_a.tool != run_b.tool:
        return f"Cannot compare different tools: {run_a.tool} vs {run_b.tool}"

    thresholds = {}

    if isinstance(run_a, MemcheckResult) and isinstance(run_b, MemcheckResult):
        df = analysis.compare_memcheck(run_a, run_b)
        if min_delta is not None and "count_delta" in df.columns:
            df = df.filter(pl.col("count_delta").abs() >= min_delta)
        spec = build_filter_spec(
            kind_pattern=function_pattern,  # "kind" column for memcheck comparison
            sort_by=sort_by, sort_descending=sort_descending,
            offset=offset, limit=limit, thresholds=thresholds,
        )
        df = apply_filters(df, spec)
        return _format_filtered(df, "Memcheck comparison (A → B)", spec)

    if isinstance(run_a, CallgrindResult) and isinstance(run_b, CallgrindResult):
        df = analysis.compare_callgrind(run_a, run_b)
        if min_delta is not None and "cost_delta" in df.columns:
            df = df.filter(pl.col("cost_delta").abs() >= min_delta)
        spec = build_filter_spec(
            function_pattern=function_pattern, exclude_functions=exclude_functions,
            sort_by=sort_by, sort_descending=sort_descending,
            offset=offset, limit=limit,
        )
        df = apply_filters(df, spec)
        return _format_filtered(df, "Callgrind comparison (A → B)", spec)

    if isinstance(run_a, MassifResult) and isinstance(run_b, MassifResult):
        info = analysis.compare_massif(run_a, run_b)
        parts = ["**Massif comparison (A → B):**", ""]
        parts.append(f"- Peak A: {info['peak_a_bytes']:,} bytes")
        parts.append(f"- Peak B: {info['peak_b_bytes']:,} bytes")
        parts.append(f"- Delta: {info['peak_delta_bytes']:+,} bytes ({info['peak_delta_pct']:+.1f}%)")
        parts.append(f"- Snapshots: {info['snapshots_a']} → {info['snapshots_b']}")
        return "\n".join(parts)

    return f"Comparison not implemented for tool: {run_a.tool}"


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

    Filter by error kind, file/function patterns in stacks, minimum leak size,
    and thread IDs. Supports pagination for large error sets.

    Args:
        run_id: The run_id from a memcheck/helgrind/drd run
        error_index: Show only the Nth error (0-based, ignores other filters)
        kind: Exact error kind filter (e.g. "Leak_DefinitelyLost")
        file_pattern: Regex matching any file in the error's stack
        function_pattern: Regex matching any function in the error's stack
        exclude_files: Regex to exclude errors with matching files in stack
        exclude_functions: Regex to exclude errors with matching functions in stack
        min_bytes: Minimum bytes leaked (memcheck leaks only)
        thread_ids: Only errors from these thread IDs (helgrind/drd)
        offset: Skip first N matching errors
        limit: Max errors to show (default 10)
        workspace_id: Workspace containing the run
    """
    import re as re_mod

    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    if isinstance(run, MemcheckResult):
        errors = list(run.errors)
    elif isinstance(run, ThreadCheckResult):
        errors = list(run.errors)
    else:
        return f"Run {run_id} is not an error-producing run (tool={run.tool})"

    # Direct index access bypasses all filters
    if error_index is not None:
        if 0 <= error_index < len(errors):
            return formatters.format_error_details([errors[error_index]])
        return f"Error index {error_index} out of range (0-{len(errors)-1})"

    # Apply filters
    if kind:
        errors = [e for e in errors if e.kind == kind]

    if file_pattern:
        pat = re_mod.compile(file_pattern, re_mod.IGNORECASE)
        errors = [e for e in errors if any(
            f.file and pat.search(f.file) for f in e.stack
        )]

    if function_pattern:
        pat = re_mod.compile(function_pattern, re_mod.IGNORECASE)
        errors = [e for e in errors if any(
            f.fn and pat.search(f.fn) for f in e.stack
        )]

    if exclude_files:
        pat = re_mod.compile(exclude_files, re_mod.IGNORECASE)
        errors = [e for e in errors if not any(
            f.file and pat.search(f.file) for f in e.stack
        )]

    if exclude_functions:
        pat = re_mod.compile(exclude_functions, re_mod.IGNORECASE)
        errors = [e for e in errors if not any(
            f.fn and pat.search(f.fn) for f in e.stack
        )]

    if min_bytes is not None:
        errors = [e for e in errors if hasattr(e, "bytes_leaked") and
                  e.bytes_leaked is not None and e.bytes_leaked >= min_bytes]

    if thread_ids is not None:
        errors = [e for e in errors if hasattr(e, "thread_id") and
                  e.thread_id in thread_ids]

    total_matching = len(errors)

    # Pagination
    errors = errors[offset:offset + limit]

    if not errors:
        return f"No errors matching filters (0 of {total_matching} after offset {offset})"

    result = formatters.format_error_details(errors, max_errors=limit)
    result += f"\n\nShowing {len(errors)} of {total_matching} matching errors (offset={offset})"
    return result


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
    """General-purpose query tool for any valgrind run data.

    Builds the raw DataFrame for any run type and applies arbitrary filters,
    column selection, sorting, sampling, and pagination. Use this when the
    specialized analysis tools don't cover your needs.

    Args:
        run_id: The run_id from any previous run
        columns: Specific columns to return (None = all). Use "schema" to list available columns.
        file_pattern: Regex to include only matching files
        function_pattern: Regex to include only matching functions
        kind_pattern: Regex to include only matching kinds
        exclude_files: Regex to exclude files
        exclude_functions: Regex to exclude functions
        min_bytes: Minimum bytes (leak bytes, heap bytes, etc.)
        max_bytes: Maximum bytes
        sort_by: Column to sort by
        sort_descending: Sort direction
        offset: Skip first N rows
        limit: Max rows to return (default 50)
        sample_n: Random sample of N rows
        sample_fraction: Random fraction (0.0-1.0) of rows
        sample_every: Take every Nth row
        sample_seed: Seed for reproducible sampling
        stratify_by: Column for stratified sampling
        time_min: Min time (massif)
        time_max: Max time (massif)
        thresholds: Dict of column -> [min, max] (use null for no bound)
        workspace_id: Workspace containing the run
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    # Build the raw DataFrame based on tool type
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

    # Handle "schema" request
    if columns == ["schema"]:
        schema_info = []
        for col_name, col_type in df.schema.items():
            schema_info.append(f"- `{col_name}`: {col_type}")
        return f"**Schema for {run.tool} run `{run_id}`:**\n\n" + "\n".join(schema_info) + f"\n\n{len(df)} total rows"

    # Convert thresholds from JSON-friendly format to tuples
    threshold_tuples = {}
    if thresholds:
        for col, bounds in thresholds.items():
            if isinstance(bounds, list) and len(bounds) == 2:
                threshold_tuples[col] = (bounds[0], bounds[1])

    spec = build_filter_spec(
        file_pattern=file_pattern, function_pattern=function_pattern,
        kind_pattern=kind_pattern, exclude_files=exclude_files,
        exclude_functions=exclude_functions, min_bytes=min_bytes,
        max_bytes=max_bytes, sort_by=sort_by, sort_descending=sort_descending,
        offset=offset, limit=limit, sample_n=sample_n,
        sample_fraction=sample_fraction, sample_every=sample_every,
        sample_seed=sample_seed, stratify_by=stratify_by,
        time_min=time_min, time_max=time_max,
        thresholds=threshold_tuples,
    )

    df = apply_filters(df, spec)

    # Column selection
    if columns and columns != ["schema"]:
        valid_cols = [c for c in columns if c in df.columns]
        if valid_cols:
            df = df.select(valid_cols)
        else:
            return f"None of the requested columns exist. Available: {df.columns}"

    return _format_filtered(df, f"Query results ({run.tool})", spec)


# ============================================================
# Entry point
# ============================================================


def main():
    """Run the MCP server via stdio transport."""
    mcp.run()


if __name__ == "__main__":
    main()
