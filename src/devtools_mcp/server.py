"""FastMCP server for devtools-mcp: unified performance engineering toolkit.

Provides a normalized interface across Valgrind, LLDB, DTrace, and perf.
Tool definitions are in the tools/ package. Backends register via registry.py.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.fastmcp import Context, FastMCP

# Import backends to trigger auto-registration
import devtools_mcp.dtrace.backend  # noqa: F401
import devtools_mcp.lldb.backend  # noqa: F401
import devtools_mcp.perf.backend  # noqa: F401
import devtools_mcp.valgrind.backend  # noqa: F401
from devtools_mcp.models import RunBase
from devtools_mcp.registry import ToolRegistry
from devtools_mcp.workspace import AppContext, Workspace


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Create default workspace, detect tools on startup, clean up on shutdown."""
    ctx = AppContext()
    ws = ctx.create_workspace("default")
    ctx.default_workspace_id = ws.workspace_id

    # Auto-detect installed tools
    ctx.registry = ToolRegistry()
    await ctx.registry.detect_all()

    try:
        yield ctx
    finally:
        ctx.cleanup_all()


mcp = FastMCP(
    "devtools-mcp",
    lifespan=app_lifespan,
    instructions=(
        "Unified performance engineering toolkit. Supports Valgrind (memcheck, helgrind, "
        "drd, callgrind, cachegrind, massif), LLDB debugging, DTrace tracing, and perf profiling. "
        "Use devtools_check() to see installed tools. Use devtools_run() to execute any tool. "
        "Use devtools_search() to find patterns across all results. Use devtools_analyze() to "
        "drill into specific runs. All analysis supports rich filtering: regex patterns, "
        "thresholds, pagination, and sampling."
    ),
)


# --- Shared helpers ---


def get_app_ctx(ctx: Context) -> AppContext:
    """Extract AppContext from MCP request context."""
    return ctx.request_context.lifespan_context


def get_run(
    ctx: Context,
    run_id: str,
    workspace_id: str | None = None,
) -> tuple[Workspace, RunBase]:
    """Retrieve a workspace and run result."""
    app = get_app_ctx(ctx)
    ws = app.get_workspace(workspace_id)
    return ws, ws.get_run(run_id)


# --- Register all tools ---
import devtools_mcp.tools  # noqa: E402, F401


def main() -> None:
    """Run the MCP server via stdio transport."""
    mcp.run()


if __name__ == "__main__":
    main()
