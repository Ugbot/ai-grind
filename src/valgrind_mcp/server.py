"""FastMCP server for Valgrind tool suite.

Exposes 16 MCP tools for running valgrind, analyzing results, and comparing runs.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncIterator

from mcp.server.fastmcp import FastMCP, Context

from valgrind_mcp import analysis, formatters
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
        "Use valgrind_check() first to verify installation. "
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
# Analysis Tools
# ============================================================


@mcp.tool()
async def valgrind_analyze_errors(
    ctx: Context,
    run_id: str,
    group_by: str = "kind",
    top_n: int = 20,
    workspace_id: str | None = None,
) -> str:
    """Analyze errors from a memcheck, helgrind, or drd run.

    Groups errors by kind, function, or file and shows aggregated counts.

    Args:
        run_id: The run_id from a previous run tool call
        group_by: How to group errors — "kind", "function", or "file"
        top_n: Maximum number of groups to show
        workspace_id: Workspace containing the run
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    if isinstance(run, MemcheckResult):
        if group_by == "function":
            df = analysis.errors_by_function(run, top_n=top_n)
        elif group_by == "file":
            df = analysis.errors_by_file(run, top_n=top_n)
        else:
            df = analysis.errors_by_kind(run)
        return formatters.format_dataframe(df, title=f"Memcheck errors by {group_by}")

    elif isinstance(run, ThreadCheckResult):
        if group_by == "function":
            df = analysis.thread_errors_by_function(run, top_n=top_n)
        else:
            df = analysis.thread_errors_by_kind(run)
        return formatters.format_dataframe(df, title=f"Thread errors by {group_by}")

    return f"Run {run_id} is not a memcheck/helgrind/drd run (tool={run.tool})"


@mcp.tool()
async def valgrind_analyze_hotspots(
    ctx: Context,
    run_id: str,
    event: str = "Ir",
    top_n: int = 20,
    workspace_id: str | None = None,
) -> str:
    """Analyze callgrind hotspots — top functions by cost for a given event.

    Args:
        run_id: The run_id from a callgrind run
        event: Event to sort by (Ir=instructions, Dr=data reads, Dw=data writes, etc.)
        top_n: Number of top functions to show
        workspace_id: Workspace containing the run
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    if not isinstance(run, CallgrindResult):
        return f"Run {run_id} is not a callgrind run (tool={run.tool})"

    df = analysis.hotspots(run, event=event, top_n=top_n)
    return formatters.format_dataframe(df, title=f"Callgrind hotspots by {event}")


@mcp.tool()
async def valgrind_analyze_cache(
    ctx: Context,
    run_id: str,
    top_n: int = 20,
    workspace_id: str | None = None,
) -> str:
    """Analyze cachegrind results — functions with worst cache miss rates.

    Args:
        run_id: The run_id from a cachegrind run
        top_n: Number of top functions to show
        workspace_id: Workspace containing the run
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    if not isinstance(run, CachegrindResult):
        return f"Run {run_id} is not a cachegrind run (tool={run.tool})"

    df = analysis.cache_miss_rates(run, top_n=top_n)
    return formatters.format_dataframe(df, title="Cachegrind miss rates by function")


@mcp.tool()
async def valgrind_analyze_memory(
    ctx: Context,
    run_id: str,
    workspace_id: str | None = None,
) -> str:
    """Analyze massif results — memory timeline and peak allocation breakdown.

    Args:
        run_id: The run_id from a massif run
        workspace_id: Workspace containing the run
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    if not isinstance(run, MassifResult):
        return f"Run {run_id} is not a massif run (tool={run.tool})"

    parts = []

    # Timeline
    df = analysis.massif_timeline_df(run)
    parts.append(formatters.format_dataframe(df, title="Memory timeline"))

    # Peak allocations
    peak_allocs = analysis.peak_allocations(run)
    if peak_allocs:
        parts.append("")
        parts.append("**Peak allocation tree:**")
        for alloc in peak_allocs[:20]:
            indent = "  " * alloc["depth"]
            fn = alloc["function"] or "???"
            loc = ""
            if alloc["file"] and alloc["line"]:
                loc = f" ({alloc['file']}:{alloc['line']})"
            parts.append(f"{indent}- `{fn}`{loc}: {alloc['bytes']:,} bytes")

    return "\n".join(parts)


@mcp.tool()
async def valgrind_compare_runs(
    ctx: Context,
    run_id_a: str,
    run_id_b: str,
    workspace_id: str | None = None,
) -> str:
    """Compare two valgrind runs of the same tool type.

    Shows deltas between runs — useful for before/after analysis.

    Args:
        run_id_a: First run (baseline)
        run_id_b: Second run (comparison)
        workspace_id: Workspace containing both runs
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run_a = ws.get_run(run_id_a)
    run_b = ws.get_run(run_id_b)

    if run_a.tool != run_b.tool:
        return f"Cannot compare different tools: {run_a.tool} vs {run_b.tool}"

    if isinstance(run_a, MemcheckResult) and isinstance(run_b, MemcheckResult):
        df = analysis.compare_memcheck(run_a, run_b)
        return formatters.format_comparison(df, title="Memcheck comparison (A → B)")

    if isinstance(run_a, CallgrindResult) and isinstance(run_b, CallgrindResult):
        df = analysis.compare_callgrind(run_a, run_b)
        return formatters.format_comparison(df, title="Callgrind comparison (A → B)")

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
    workspace_id: str | None = None,
) -> str:
    """Get detailed error information including full stack traces.

    Filter by specific error index or error kind. Without filters, shows all errors.

    Args:
        run_id: The run_id from a memcheck/helgrind/drd run
        error_index: Show only the Nth error (0-based)
        kind: Filter errors by kind (e.g. "Leak_DefinitelyLost", "Race")
        workspace_id: Workspace containing the run
    """
    app = _get_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    run = ws.get_run(run_id)

    if isinstance(run, MemcheckResult):
        errors = run.errors
    elif isinstance(run, ThreadCheckResult):
        errors = run.errors
    else:
        return f"Run {run_id} is not an error-producing run (tool={run.tool})"

    if error_index is not None:
        if 0 <= error_index < len(errors):
            errors = [errors[error_index]]
        else:
            return f"Error index {error_index} out of range (0-{len(errors)-1})"

    if kind:
        errors = [e for e in errors if e.kind == kind]
        if not errors:
            return f"No errors of kind '{kind}' found"

    return formatters.format_error_details(errors)


# ============================================================
# Entry point
# ============================================================


def main():
    """Run the MCP server via stdio transport."""
    mcp.run()


if __name__ == "__main__":
    main()
