"""FastMCP server for devtools-mcp: unified performance engineering toolkit.

Provides a normalized interface across Valgrind, LLDB, DTrace, and perf.
Tool definitions are in the tools/ package. Backends register via registry.py.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.fastmcp import Context, FastMCP

from devtools_mcp.models import RunBase
from devtools_mcp.registry import ToolRegistry, load_backends
from devtools_mcp.workspace import AppContext, Workspace

# Manifest-driven backend registration: a broken backend degrades to
# "failed to load" in devtools_check instead of killing the server.
load_backends()


def _maybe_start_dashboard(ctx: AppContext) -> None:
    """Auto-start the visualization terminal when configured (service mode).

    DEVTOOLS_MCP_DASHBOARD=1 (set by `--dashboard`, or defaulted on for network
    transports) brings the dashboard up with the server so a long-lived shared
    instance is always visible at DEVTOOLS_MCP_DASHBOARD_PORT (default 8765).
    Failure to bind is reported on stderr, never fatal — the MCP server is the
    primary service.
    """
    import os
    import sys

    if os.environ.get("DEVTOOLS_MCP_DASHBOARD", "0") != "1":
        return
    if ctx.viz_server is not None and ctx.viz_server.running:
        return  # already started eagerly in service mode (main)
    from devtools_mcp.viz.server import VizServer

    port = int(os.environ.get("DEVTOOLS_MCP_DASHBOARD_PORT", "8765"))
    assert 0 <= port <= 65535, f"dashboard port out of range: {port}"
    srv = VizServer(ctx)
    try:
        url = srv.start(port=port)
    except OSError as exc:
        print(f"devtools-mcp: dashboard failed to bind port {port}: {exc}", file=sys.stderr)
        return
    ctx.viz_server = srv
    assert ctx.viz_server.running, "dashboard reported started but is not running"
    print(f"devtools-mcp: dashboard at {url} (tracker at {url}/tracker)", file=sys.stderr)


# Set by main() in service mode: FastMCP's streamable-http lifespan only runs
# on the first client session, so a shared instance bootstraps its context (and
# dashboard) eagerly at boot and the lifespan adopts it instead of re-creating.
_EAGER_CTX: AppContext | None = None


def _bootstrap_ctx() -> AppContext:
    """Default workspace + persisted runs — shared by eager boot and the lifespan."""
    import sys

    ctx = AppContext()
    ws = ctx.create_workspace("default")
    ctx.default_workspace_id = ws.workspace_id
    loaded = ctx.hydrate_all_runs()
    if loaded:
        print(f"devtools-mcp: loaded {loaded} persisted run(s) from disk", file=sys.stderr)
    assert ctx.default_workspace_id, "bootstrap produced no default workspace"
    return ctx


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Adopt the eager service context (or create one), detect tools, clean up."""
    ctx = _EAGER_CTX if _EAGER_CTX is not None else _bootstrap_ctx()

    # Auto-detect installed tools
    ctx.registry = ToolRegistry()
    await ctx.registry.detect_all()
    _maybe_start_dashboard(ctx)

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
        "debugger), py (pyspy/dump/cprofile), node (cpu/heap), renderdoc (GPU frame capture + "
        "replay analysis: capture an app to .rdc, then analyze/counters/resources on the .rdc "
        "for the drawcall tree, GPU timings, and a GPU-time flame graph). "
        "devtools_install(suite) shows per-OS install commands for a missing backend "
        "(execute=True runs them when DEVTOOLS_MCP_ALLOW_INSTALL=1). "
        "Core rule: NEVER flood the model with raw output — every run is stored as a "
        "queryable Polars DataFrame and tools return only bounded summaries + a run_id. "
        "Workflow: devtools_check() to see installed tools; devtools_run(suite, tool, binary) to "
        "run one; devtools_analyze()/devtools_query() to drill into the frame; "
        "devtools_search()/devtools_correlate() across runs; devtools_flamegraph(run_id) for an "
        "SVG + text flame graph from any sampling run; devtools_dashboard() to open a browser "
        "visualization terminal. LLDB debug_* tools provide interactive sessions. "
        "tracker_* tools: a persistent SQLite-backed task tracker (mini-JIRA) — projects, "
        "hierarchical tasks with PROJ-123 keys, acceptance criteria linked to tests, commit "
        "links, auto-tag rules, GitHub issue sync; query it via tracker_query (bounded views), "
        "ask tracker_deps(action='resolve') what needs to happen next (ready/blocked/order), "
        "and replicate it across machines with tracker_sync (CRDT local-first; peers are other "
        "dashboards). The dashboard also renders the tracker as card boards at /tracker."
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
    ap.add_argument(
        "--dashboard",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Also serve the browser dashboard (default: on for http/sse, off for stdio).",
    )
    ap.add_argument(
        "--dashboard-port",
        type=int,
        default=int(os.environ.get("DEVTOOLS_MCP_DASHBOARD_PORT", "8765")),
        help="Dashboard port (default 8765).",
    )
    args = ap.parse_args()

    transport = "streamable-http" if args.transport in ("http", "streamable-http") else args.transport
    # Service mode: a long-lived shared instance should always be visible, so the
    # dashboard defaults on for network transports (override with --no-dashboard).
    dashboard = args.dashboard if args.dashboard is not None else transport != "stdio"
    os.environ["DEVTOOLS_MCP_DASHBOARD"] = "1" if dashboard else "0"
    os.environ["DEVTOOLS_MCP_DASHBOARD_PORT"] = str(args.dashboard_port)

    if transport != "stdio":
        # Never write to stdout in stdio mode (it is the transport); for network
        # transports, announce on stderr so it doesn't corrupt anything.
        mcp.settings.host = args.host
        mcp.settings.port = args.port
        path = mcp.settings.sse_path if transport == "sse" else mcp.settings.streamable_http_path
        print(f"devtools-mcp: {transport} on http://{args.host}:{args.port}{path}", file=sys.stderr)
        if dashboard:
            # Service mode: the streamable-http lifespan only fires on the first
            # client session, so bring the context + dashboard up eagerly — the
            # dashboard (and its collab ingest API) must be reachable from boot.
            global _EAGER_CTX
            _EAGER_CTX = _bootstrap_ctx()
            _maybe_start_dashboard(_EAGER_CTX)

    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
