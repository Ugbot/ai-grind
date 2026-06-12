"""Threaded stdlib HTTP server that turns the browser into a visualization
terminal over the live workspace. Zero third-party deps, no asyncio — safe to run
alongside the stdio MCP server in a background thread.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from devtools_mcp.flamegraph import build_call_tree, focus, render_svg
from devtools_mcp.registry import get_backend
from devtools_mcp.viz import render

_RAW_MAX = 200_000


def _find_run(app: object, run_id: str):
    """Locate a run across all workspaces. Returns (workspace, run) or (None, None)."""
    for ws in getattr(app, "workspaces", {}).values():
        if run_id in ws.runs:
            return ws, ws.runs[run_id]
    return None, None


def _df_for(run: object):
    backend = get_backend(run.suite)
    builder = backend.df_builders.get(run.tool) or backend.df_builders.get("_default")
    return builder(run) if builder else None


def _stacks_for(run: object):
    backend = get_backend(run.suite)
    if backend.stacks is None:
        return None
    return backend.stacks(run)


def _all_runs(app: object) -> list[dict]:
    rows: list[dict] = []
    for ws in getattr(app, "workspaces", {}).values():
        for rid, run in ws.runs.items():
            backend = get_backend(run.suite) if run.suite in _suites() else None
            has_stacks = bool(backend and backend.stacks and backend.stacks(run))
            rows.append(
                {
                    "run_id": rid,
                    "suite": run.suite,
                    "tool": run.tool,
                    "binary": run.binary,
                    "when": run.timestamp.strftime("%H:%M:%S"),
                    "exit": run.exit_code,
                    "has_stacks": has_stacks,
                }
            )
    rows.sort(key=lambda r: r["when"], reverse=True)
    return rows


def _suites() -> set[str]:
    from devtools_mcp.registry import list_backends

    return set(list_backends())


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args: object) -> None:  # silence stdout (would corrupt MCP stdio)
        pass

    def _send(self, body: str, status: int = 200) -> None:
        data = body.encode("utf-8", "replace")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802 (stdlib API)
        app = self.server.app_ctx  # type: ignore[attr-defined]
        parsed = urlparse(self.path)
        parts = [p for p in parsed.path.split("/") if p]
        try:
            if not parts:
                self._send(render.dashboard(_all_runs(app)))
            elif parts[0] == "health":
                self._send("ok")
            elif parts[0] in ("run", "flame", "raw") and len(parts) == 2:
                self._route_run(app, parts[0], unquote(parts[1]), parse_qs(parsed.query))
            else:
                self._send(render.page("not found", "<p>404</p>"), 404)
        except Exception as e:  # never crash the server thread
            self._send(render.page("error", f"<pre>{render._h(e)}</pre>"), 500)

    def _route_run(self, app: object, kind: str, run_id: str, query: dict) -> None:
        ws, run = _find_run(app, run_id)
        if run is None:
            self._send(render.page("not found", f"<p>no run {render._h(run_id)}</p>"), 404)
            return
        meta = {"run_id": run_id, "suite": run.suite, "tool": run.tool, "binary": run.binary}
        if kind == "run":
            df = _df_for(run)
            backend = get_backend(run.suite)
            table = render.table_from_df(df) if df is not None else "<p class='note'>no table</p>"
            self._send(render.run_page(meta, backend.format_summary(run), table, has_stacks=bool(_stacks_for(run))))
        elif kind == "flame":
            self._send(self._flame(run_id, run, query))
        else:  # raw
            text, trunc = self._raw_text(ws, run_id, run)
            self._send(render.raw_page(run_id, text, trunc))

    def _flame(self, run_id: str, run: object, query: dict) -> str:
        samples = _stacks_for(run)
        if not samples:
            return render.page("flame", "<p class='note'>this run has no stacks.</p>")
        tree = build_call_tree(samples, root_name=run.binary or "all")
        focus_name = (query.get("focus") or [None])[0]
        if focus_name:
            sub = focus(tree, focus_name)
            if sub is not None:
                tree = sub
        svg = render_svg(tree, title=f"{run.suite}:{run.tool}", width=1400, href_base=f"/flame/{run_id}")
        return render.flame_page(run_id, svg, tree.total_weight, focus_name)

    def _raw_text(self, ws: object, run_id: str, run: object) -> tuple[str, bool]:
        try:
            path = ws.get_raw_path(run_id)
            with open(path, errors="replace") as f:
                text = f.read()
        except (KeyError, FileNotFoundError, OSError):
            text = getattr(run, "raw_output", "") or "(no raw output)"
        if len(text) > _RAW_MAX:
            return text[:_RAW_MAX], True
        return text, False


class VizServer:
    """Owns the background HTTP server thread."""

    def __init__(self, app_ctx: object) -> None:
        self.app_ctx = app_ctx
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.url = ""

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self, host: str = "127.0.0.1", port: int = 8765) -> str:
        assert not self.running, "viz server already running"
        httpd = ThreadingHTTPServer((host, port), _Handler)
        httpd.app_ctx = self.app_ctx  # type: ignore[attr-defined]
        self._httpd = httpd
        self.url = f"http://{host}:{httpd.server_address[1]}"
        self._thread = threading.Thread(target=httpd.serve_forever, daemon=True, name="devtools-viz")
        self._thread.start()
        assert self.running, "viz server thread failed to start"
        return self.url

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
        self._httpd = None
        self._thread = None
        self.url = ""
