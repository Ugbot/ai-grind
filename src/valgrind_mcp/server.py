"""FastMCP server for Valgrind tool suite.

This module defines the MCP server instance, lifespan context,
and shared helpers. Tool definitions are in the tools/ package.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.fastmcp import Context, FastMCP

from valgrind_mcp import analysis, formatters  # noqa: F401 — used by tools via server module
from valgrind_mcp.models import (
    CachegrindResult,
    CallgrindResult,
    MassifResult,
    MemcheckResult,
    ThreadCheckResult,
    ValgrindResult,
    create_run_base,
)
from valgrind_mcp.parsers import (
    parse_cachegrind,
    parse_callgrind,
    parse_massif,
    parse_memcheck_xml,
    parse_threadcheck_xml,
)
from valgrind_mcp.runner import run_valgrind
from valgrind_mcp.workspace import AppContext, Workspace


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


# --- Shared helpers used by tool modules ---


def get_app_ctx(ctx: Context) -> AppContext:
    """Extract AppContext from MCP request context."""
    return ctx.request_context.lifespan_context


def get_run(
    ctx: Context,
    run_id: str,
    workspace_id: str | None = None,
) -> tuple[Workspace, ValgrindResult]:
    """Retrieve a workspace and run result. Raises KeyError if not found."""
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    return ws, ws.get_run(run_id)


_PARSE_DISPATCH: dict[str, type] = {
    "memcheck": MemcheckResult,
    "helgrind": ThreadCheckResult,
    "drd": ThreadCheckResult,
    "callgrind": CallgrindResult,
    "cachegrind": CachegrindResult,
    "massif": MassifResult,
}


async def run_and_parse(
    ctx: Context,
    tool: str,
    binary: str,
    args: list[str] | None = None,
    valgrind_args: list[str] | None = None,
    timeout: int = 300,
    workspace_id: str | None = None,
) -> tuple[str | None, ValgrindResult | None]:
    """Run valgrind and parse results. Returns (error_msg, parsed_result).

    On success: (None, result). On failure: (error_string, None).
    """
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)

    result = await run_valgrind(
        tool=tool,
        binary=binary,
        binary_args=args,
        valgrind_args=valgrind_args,
        timeout=timeout,
    )

    if result.exit_code == -1:
        return f"{tool.capitalize()} failed: {result.stderr}", None

    run_base = create_run_base(
        tool=tool,
        binary=binary,
        args=args,
        valgrind_args=valgrind_args,
        duration_seconds=result.duration_seconds,
        exit_code=result.exit_code,
    )

    if tool == "memcheck":
        parsed = parse_memcheck_xml(result.output_path, run_base)
    elif tool in ("helgrind", "drd"):
        parsed = parse_threadcheck_xml(result.output_path, run_base, tool=tool)
    elif tool == "callgrind":
        parsed = parse_callgrind(result.output_path, run_base)
    elif tool == "cachegrind":
        parsed = parse_cachegrind(result.output_path, run_base)
    elif tool == "massif":
        parsed = parse_massif(result.output_path, run_base)
    else:
        return f"Unknown tool: {tool}", None

    ws.store_run(parsed, result.output_path)
    return None, parsed


# --- Register all tools ---
# Importing the tools package triggers @mcp.tool() registration
import valgrind_mcp.tools  # noqa: E402, F401


def main() -> None:
    """Run the MCP server via stdio transport."""
    mcp.run()


if __name__ == "__main__":
    main()
