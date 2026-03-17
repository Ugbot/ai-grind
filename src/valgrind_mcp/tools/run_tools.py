"""Run tools — execute valgrind tools against binaries."""

from __future__ import annotations

from mcp.server.fastmcp import Context

from valgrind_mcp import formatters
from valgrind_mcp.models import (
    CachegrindResult,
    CallgrindResult,
    MassifResult,
    MemcheckResult,
    ThreadCheckResult,
)
from valgrind_mcp.server import mcp, run_and_parse


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
    err, parsed = await run_and_parse(ctx, "memcheck", binary, args, valgrind_args, timeout, workspace_id)
    if err:
        return err
    assert isinstance(parsed, MemcheckResult)
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

    Args:
        binary: Path to the executable to analyze
        args: Arguments to pass to the binary
        valgrind_args: Extra valgrind flags
        timeout: Max seconds to wait (default 300)
        workspace_id: Workspace to store results
    """
    err, parsed = await run_and_parse(ctx, "helgrind", binary, args, valgrind_args, timeout, workspace_id)
    if err:
        return err
    assert isinstance(parsed, ThreadCheckResult)
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
    err, parsed = await run_and_parse(ctx, "drd", binary, args, valgrind_args, timeout, workspace_id)
    if err:
        return err
    assert isinstance(parsed, ThreadCheckResult)
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
    and branch prediction.

    Args:
        binary: Path to the executable to analyze
        args: Arguments to pass to the binary
        valgrind_args: Extra valgrind flags
        timeout: Max seconds to wait (default 300)
        workspace_id: Workspace to store results
    """
    err, parsed = await run_and_parse(ctx, "callgrind", binary, args, valgrind_args, timeout, workspace_id)
    if err:
        return err
    assert isinstance(parsed, CallgrindResult)
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

    Args:
        binary: Path to the executable to analyze
        args: Arguments to pass to the binary
        valgrind_args: Extra valgrind flags
        timeout: Max seconds to wait (default 300)
        workspace_id: Workspace to store results
    """
    err, parsed = await run_and_parse(ctx, "cachegrind", binary, args, valgrind_args, timeout, workspace_id)
    if err:
        return err
    assert isinstance(parsed, CachegrindResult)
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
    and peak memory consumption.

    Args:
        binary: Path to the executable to analyze
        args: Arguments to pass to the binary
        valgrind_args: Extra valgrind flags
        timeout: Max seconds to wait (default 300)
        workspace_id: Workspace to store results
    """
    err, parsed = await run_and_parse(ctx, "massif", binary, args, valgrind_args, timeout, workspace_id)
    if err:
        return err
    assert isinstance(parsed, MassifResult)
    return formatters.format_massif_summary(parsed)
