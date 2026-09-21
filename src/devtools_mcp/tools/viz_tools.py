"""Visualization terminal tool: devtools_dashboard, serve runs in a browser."""

from __future__ import annotations

import os
import webbrowser

from mcp.server.fastmcp import Context

from devtools_mcp.server import get_app_ctx, mcp
from devtools_mcp.viz.server import VizServer


@mcp.tool()
async def devtools_dashboard(
    ctx: Context,
    action: str = "start",
    port: int = 8765,
    open_browser: bool = False,
) -> str:
    """Start/stop the browser visualization terminal for all runs.

    A local web UI to browse every run, view its queryable data table, read raw
    output/logs, and explore interactive (click-to-zoom) flame graphs, the same
    data the LLM sees, for a human. Nothing is sent anywhere; it binds to
    127.0.0.1 unless the operator set DEVTOOLS_MCP_DASHBOARD_HOST (the
    --dashboard-host flag) to share this replica with a second machine.

    Args:
        action: "start" (default), "stop", or "status".
        port: TCP port to bind (default 8765; 0 picks a free port).
        open_browser: If true, also open the URL in the default browser.
    """
    app = get_app_ctx(ctx)

    if action == "status":
        srv = app.viz_server
        if srv and srv.running:
            n = sum(len(ws.runs) for ws in app.workspaces.values())
            return f"Visualization terminal running at {srv.url} ({n} run(s) available)."
        return 'Visualization terminal is not running. Use devtools_dashboard(action="start").'

    if action == "stop":
        if app.viz_server and app.viz_server.running:
            url = app.viz_server.url
            app.viz_server.stop()
            return f"Stopped the visualization terminal ({url})."
        return "Visualization terminal was not running."

    if action != "start":
        return f"Unknown action '{action}' (start|stop|status)."

    if app.viz_server and app.viz_server.running:
        if open_browser:
            webbrowser.open(app.viz_server.url)
        return f"Visualization terminal already running at {app.viz_server.url}"

    srv = VizServer(app)
    # The bind address is operator-owned (DEVTOOLS_MCP_DASHBOARD_HOST / the
    # --dashboard-host flag), deliberately NOT a tool parameter: exposing this
    # port off-box must never be something a model can decide to do.
    host = os.environ.get("DEVTOOLS_MCP_DASHBOARD_HOST", "127.0.0.1").strip() or "127.0.0.1"
    try:
        url = srv.start(host=host, port=port)
    except OSError as e:
        return f"Could not start on {host}:{port}: {e}. Try a different port or 0 for any free port."
    app.viz_server = srv
    if open_browser:
        webbrowser.open(url)
    return (
        f"Visualization terminal started at **{url}**\n"
        f"- `{url}/`, all runs\n"
        f"- per run: data table, raw logs, and an interactive flame graph (click to zoom)\n"
        f"Open it in a browser (bound to `{host}`)."
    )
