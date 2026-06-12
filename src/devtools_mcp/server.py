"""FastMCP server for devtools-mcp: unified performance engineering toolkit.

Provides a normalized interface across Valgrind, LLDB, DTrace, and perf.
Tool definitions are in the tools/ package. Backends register via registry.py.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.fastmcp import Context, FastMCP

# Import backends to trigger auto-registration
import devtools_mcp.cargo.backend  # noqa: F401
import devtools_mcp.cdb.backend  # noqa: F401
import devtools_mcp.dtrace.backend  # noqa: F401
import devtools_mcp.etw.backend  # noqa: F401
import devtools_mcp.gradle.backend  # noqa: F401
import devtools_mcp.jvm.backend  # noqa: F401
import devtools_mcp.lldb.backend  # noqa: F401
import devtools_mcp.maven.backend  # noqa: F401
import devtools_mcp.node.backend  # noqa: F401
import devtools_mcp.npm.backend  # noqa: F401
import devtools_mcp.perf.backend  # noqa: F401
import devtools_mcp.pnpm.backend  # noqa: F401
import devtools_mcp.py.backend  # noqa: F401
import devtools_mcp.valgrind.backend  # noqa: F401
import devtools_mcp.vtune.backend  # noqa: F401
import devtools_mcp.yarn.backend  # noqa: F401
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
        "Unified performance engineering toolkit across native, JVM, Python, and JavaScript. "
        "Backends: valgrind, lldb, dtrace, perf, etw (Windows/PerfView), vtune (Intel: "
        "hotspots/threading/memory/uarch/snapshot), jvm (jfr/threads/heap/asprof), cdb (Windows "
        "debugger), py (pyspy/dump/cprofile), node "
        "(cpu/heap). Core rule: NEVER flood the model with raw output — every run is stored as a "
        "queryable Polars DataFrame and tools return only bounded summaries + a run_id. "
        "Workflow: devtools_check() to see installed tools; devtools_run(suite, tool, binary) to "
        "run one; devtools_analyze()/devtools_query() to drill into the frame; "
        "devtools_search()/devtools_correlate() across runs; devtools_flamegraph(run_id) for an "
        "SVG + text flame graph from any sampling run; devtools_dashboard() to open a browser "
        "visualization terminal. LLDB debug_* tools provide interactive sessions. "
        "tracker_* tools: a persistent SQLite-backed task tracker (mini-JIRA) — projects, "
        "hierarchical tasks with PROJ-123 keys, acceptance criteria linked to tests, commit "
        "links, auto-tag rules, GitHub issue sync; query it via tracker_query (bounded views) "
        "and ask tracker_deps(action='resolve') what needs to happen next (ready/blocked/order)."
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
    """Run the MCP server over stdio (pipes), SSE, or streamable HTTP.

    Transport resolves from (highest first): CLI flags, env vars, then stdio.
      stdio (default)         : MCP over stdin/stdout — for editor/CLI clients.
      sse / http              : network transports bound to --host/--port.

    Env: DEVTOOLS_MCP_TRANSPORT, DEVTOOLS_MCP_HOST, DEVTOOLS_MCP_PORT.
    """
    import argparse
    import os
    import sys

    ap = argparse.ArgumentParser(prog="devtools-mcp", description="devtools-mcp server")
    ap.add_argument(
        "--transport",
        choices=("stdio", "sse", "http", "streamable-http"),
        default=os.environ.get("DEVTOOLS_MCP_TRANSPORT", "stdio"),
        help="Transport: stdio (pipes, default), sse, or http (streamable-http).",
    )
    ap.add_argument("--host", default=os.environ.get("DEVTOOLS_MCP_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("DEVTOOLS_MCP_PORT", "8000")))
    args = ap.parse_args()

    transport = "streamable-http" if args.transport in ("http", "streamable-http") else args.transport
    if transport != "stdio":
        # Never write to stdout in stdio mode (it is the transport); for network
        # transports, announce on stderr so it doesn't corrupt anything.
        mcp.settings.host = args.host
        mcp.settings.port = args.port
        path = mcp.settings.sse_path if transport == "sse" else mcp.settings.streamable_http_path
        print(f"devtools-mcp: {transport} on http://{args.host}:{args.port}{path}", file=sys.stderr)

    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
