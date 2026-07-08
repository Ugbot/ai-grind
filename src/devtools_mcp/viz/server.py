"""Threaded stdlib HTTP server — visualization terminal over workspace + tracker."""

from __future__ import annotations

import contextlib
import json
import urllib.parse
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from devtools_mcp.flamegraph.render_svg import render_svg  # browser viz page only; not MCP tool API
from devtools_mcp.index import build_index, correlate_runs, search_index
from devtools_mcp.registry import get_backend
from devtools_mcp.viz import render

_RAW_MAX = 200_000
_PUSH_MAX_BYTES = 50_000_000
_TABLE_PAGE = 50


def _with_tracker(fn: Callable):
    from devtools_mcp.tracker.db import open_tracker

    db = open_tracker()
    try:
        return fn(db)
    finally:
        db.close()


def _find_run(app: object, run_id: str):
    for ws in getattr(app, "workspaces", {}).values():
        try:
            run = ws.get_run(run_id)
            return ws, run
        except KeyError:
            continue
    return None, None


def _df_for(ws: object, run: object):
    backend = get_backend(run.suite)
    builder = backend.df_builders.get(run.tool) or backend.df_builders.get("_default")
    if not builder:
        return None
    cached = ws.get_dataframe(run.run_id)
    if cached is not None:
        return cached
    df = builder(run)
    if not df.is_empty():
        ws.cache_dataframe(run.run_id, df)
    return df


def _stacks_for(run: object):
    backend = get_backend(run.suite)
    if backend.stacks is None:
        return None
    return backend.stacks(run)


def _suites() -> set[str]:
    from devtools_mcp.registry import list_backends

    return set(list_backends())


def _run_preview(run: object) -> str:
    """Best one-line preview for run cards: notes, then label context, then summary."""
    notes = getattr(run, "notes", "") or ""
    if notes.strip():
        return notes.strip()
    label = getattr(run, "label", "") or ""
    if label.strip() and label.strip() != (getattr(run, "binary", "") or "").strip():
        return label.strip()
    summary = getattr(run, "stored_summary", "") or ""
    if summary.strip():
        return summary.strip().split("\n", 1)[0]
    return ""


def _status(app: object) -> dict:
    runs = sum(len(ws.runs) for ws in getattr(app, "workspaces", {}).values())
    projects = 0
    with contextlib.suppress(Exception):
        projects = _with_tracker(lambda db: len(db.conn.execute("SELECT id FROM projects").fetchall()))
    return {"runs": runs, "projects": projects, "mcp": True}


def _all_runs(app: object, filters: dict | None = None) -> list[dict]:
    rows: list[dict] = []
    filt = filters or {}
    for ws in getattr(app, "workspaces", {}).values():
        for rid, run in ws.runs.items():
            backend = get_backend(run.suite) if run.suite in _suites() else None
            has_stacks = bool(backend and backend.stacks and backend.stacks(run))
            row = {
                "run_id": rid,
                "suite": run.suite,
                "tool": run.tool,
                "binary": run.binary,
                "when": run.timestamp.strftime("%H:%M:%S"),
                "when_full": run.timestamp.isoformat(),
                "duration": f"{run.duration_seconds:.1f}s",
                "exit": run.exit_code,
                "has_stacks": has_stacks,
                "label": run.label,
                "task_key": run.task_key,
                "tag_list": run.tags,
                "git_commit": run.git_commit,
                "git_branch": run.git_branch,
                "preview": _run_preview(run),
            }
            if filt.get("suite") and row["suite"] != filt["suite"]:
                continue
            if filt.get("tool") and row["tool"] != filt["tool"]:
                continue
            if filt.get("task_key") and row["task_key"].upper() != filt["task_key"].upper():
                continue
            q = filt.get("q", "").lower()
            if q:
                hay = " ".join(
                    [
                        row.get("binary") or "",
                        row.get("label") or "",
                        row.get("preview") or "",
                        row.get("task_key") or "",
                    ]
                ).lower()
                if q not in hay:
                    continue
            rows.append(row)
    rows.sort(key=lambda r: r["when_full"], reverse=True)
    return rows


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args: object) -> None:
        pass

    def _send(self, body: str, status: int = 200) -> None:
        data = body.encode("utf-8", "replace")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, obj: object, status: int = 200) -> None:
        data = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_bytes(self, data: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        app = self.server.app_ctx  # type: ignore[attr-defined]
        parsed = urlparse(self.path)
        parts = [p for p in parsed.path.split("/") if p]
        query = parse_qs(parsed.query)
        try:
            if not parts:
                filt = {k: (query.get(k) or [""])[0] for k in ("q", "suite", "tool", "task_key")}
                self._send(render.dashboard(_all_runs(app, filt), filt, _status(app)))
            elif parts[0] == "health":
                self._send("ok")
            elif parts[0] == "search":
                self._route_search(app, query)
            elif parts[0] == "compare":
                self._route_compare(app, query)
            elif parts[0] == "correlate":
                self._route_correlate(app, query)
            elif parts[0] == "tools":
                self._route_tools(app)
            elif parts[0] in ("run", "flame", "raw") and len(parts) >= 2:
                self._route_run(app, parts, query)
            elif parts[0] == "tracker":
                self._route_tracker(parts[1:], query)
            elif parts[0] == "api":
                self._route_api_get(parts[1:], query)
            else:
                self._send(render.page("not found", "<p>404</p>"), 404)
        except Exception as e:
            self._send(render.page("error", f"<pre>{render._h(e)}</pre>"), 500)

    def do_POST(self) -> None:  # noqa: N802
        app = self.server.app_ctx  # type: ignore[attr-defined]
        parts = [p for p in urlparse(self.path).path.split("/") if p]
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode("utf-8", "replace") if length else ""
        try:
            if parts == ["api", "crdt", "push"]:
                if length <= 0 or length > _PUSH_MAX_BYTES:
                    self._send_json({"error": f"bad Content-Length {length}"}, 400)
                    return
                payload = json.loads(body)
                from devtools_mcp.tracker import crdt

                counters = _with_tracker(lambda db: crdt.merge_ops(db, payload.get("ops", [])))
                self._send_json(counters)
            elif len(parts) == 4 and parts[0] == "tracker" and parts[1] == "task" and parts[3] == "status":
                key = unquote(parts[2]).upper()
                fields = urllib.parse.parse_qs(body)
                new_status = (fields.get("status") or ["open"])[0]
                self._post_task_status(key, new_status)
            elif len(parts) == 5 and parts[0] == "tracker" and parts[1] == "task" and parts[3] == "criteria":
                key = unquote(parts[2]).upper()
                crit_id = int(unquote(parts[4]))
                fields = urllib.parse.parse_qs(body)
                result = (fields.get("result") or ["pass"])[0]
                self._post_criteria_result(key, crit_id, result)
            elif len(parts) == 3 and parts[0] == "run" and parts[2] == "delete":
                run_id = unquote(parts[1])
                ws = app.get_workspace()
                ok = ws.delete_run(run_id)
                self._send(render.page("deleted", f"<p>Run {'deleted' if ok else 'not found'}</p>"))
            else:
                self._send_json({"error": "not found"}, 404)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _post_task_status(self, key: str, status: str) -> None:
        from devtools_mcp.tracker import tasks as tasks_mod
        from devtools_mcp.tracker.db import open_tracker

        db = open_tracker()
        try:
            tasks_mod.set_status(db, key, status)
        finally:
            db.close()
        self.send_response(303)
        self.send_header("Location", f"/tracker/task/{key}")
        self.end_headers()

    def _post_criteria_result(self, key: str, criterion_id: int, result: str) -> None:
        from devtools_mcp.tracker import criteria as criteria_mod
        from devtools_mcp.tracker.db import open_tracker

        db = open_tracker()
        try:
            criteria_mod.record_result(db, criterion_id, result)
        finally:
            db.close()
        self.send_response(303)
        self.send_header("Location", f"/tracker/task/{key}")
        self.end_headers()

    def _route_search(self, app: object, query: dict) -> None:
        ws = app.get_workspace()
        q = (query.get("q") or [None])[0]
        suite = (query.get("suite") or [None])[0]
        fp = (query.get("function_pattern") or [None])[0]
        index = build_index(ws)
        results = search_index(index, query=q, suite=suite, function_pattern=fp, limit=100)
        table = render.table_from_df(results) if not results.is_empty() else "<p class='note'>no matches</p>"
        filt = {"q": q or "", "suite": suite or "", "function_pattern": fp or ""}
        self._send(render.search_page(table, filt, _status(app)))

    def _route_compare(self, app: object, query: dict) -> None:
        a = (query.get("a") or [""])[0]
        b = (query.get("b") or [""])[0]
        html = "<p class='note'>pick two run ids</p>"
        if a and b:
            ws = app.get_workspace()
            try:
                joined = correlate_runs(ws, a, b, limit=50)
                html = render.table_from_df(joined) if not joined.is_empty() else "<p>no overlap</p>"
            except KeyError as e:
                html = f"<p>{render._h(e)}</p>"
        self._send(render.compare_page(html, a, b, _status(app)))

    def _route_correlate(self, app: object, query: dict) -> None:
        a = (query.get("a") or [""])[0]
        b = (query.get("b") or [""])[0]
        join_on = (query.get("join_on") or ["function"])[0]
        html = "<p class='note'>pick two run ids</p>"
        if a and b:
            ws = app.get_workspace()
            try:
                joined = correlate_runs(ws, a, b, join_on=join_on, limit=100)
                html = render.table_from_df(joined) if not joined.is_empty() else "<p>no overlap</p>"
            except KeyError as e:
                html = f"<p>{render._h(e)}</p>"
        self._send(render.correlate_page(html, a, b, join_on, _status(app)))

    def _route_tools(self, app: object) -> None:
        text = app.registry.format_check() if app.registry else "registry not ready"
        self._send(render.tools_page(text, _status(app)))

    def _route_tracker(self, rest: list[str], query: dict) -> None:
        from devtools_mcp.viz import tracker_data

        if not rest:
            self._send(_with_tracker(lambda db: render.tracker_overview(tracker_data.projects_overview(db))))
        elif rest[0] == "sync":
            self._send(_with_tracker(tracker_data.sync_page))
        elif rest[0] == "tags":
            self._send(_with_tracker(tracker_data.tags_page))
        elif rest[0] == "task" and len(rest) == 2:
            key = unquote(rest[1]).upper()
            from devtools_mcp.store import RunStore

            linked = RunStore().runs_for_task_key(key)
            body = _with_tracker(lambda db: tracker_data.task_detail_page(db, key, linked))
            self._send(body if body else render.page("not found", "<p>404</p>"), 200 if body else 404)
        elif len(rest) == 2:
            project_key = unquote(rest[0]).upper()
            sub = rest[1]
            if sub == "tree":
                body = _with_tracker(lambda db: tracker_data.tree_page(db, project_key))
            elif sub == "rollup":
                body = _with_tracker(lambda db: tracker_data.rollup_page(db, project_key))
            elif sub == "criteria":
                body = _with_tracker(lambda db: tracker_data.criteria_page(db, project_key))
            elif sub == "commits":
                body = _with_tracker(lambda db: tracker_data.commits_page(db, project_key))
            else:
                body = None
            self._send(body if body else render.page("not found", "<p>404</p>"), 200 if body else 404)
        elif len(rest) == 1:
            project_key = unquote(rest[0]).upper()
            from devtools_mcp.tracker import frames
            from devtools_mcp.viz import tracker_data

            def board(db):
                total = len(frames.tasks_frame(db.conn, project=project_key))
                return tracker_data.board_page(db, project_key, total_tasks=total)

            body = _with_tracker(board)
            self._send(body if body else render.page("not found", "<p>404</p>"), 200 if body else 404)
        else:
            self._send(render.page("not found", "<p>404</p>"), 404)

    def _route_api_get(self, rest: list[str], query: dict) -> None:
        from devtools_mcp.tracker import crdt

        if rest == ["crdt", "status"]:
            self._send_json(_with_tracker(crdt.status))
        elif rest == ["crdt", "ops"]:
            after = (query.get("after") or [None])[0]

            def pull(db):
                return {"site_id": db.site_id, "ops": crdt.ops_after(db.conn, after)}

            self._send_json(_with_tracker(pull))
        else:
            self._send_json({"error": "not found"}, 404)

    def _route_run(self, app: object, parts: list[str], query: dict) -> None:
        kind = parts[0]
        run_id = unquote(parts[1])
        if len(parts) == 3 and parts[2] == "download" and kind == "raw":
            self._download_raw(app, run_id)
            return
        if len(parts) == 3 and parts[2] == "export" and kind == "run":
            self._export_run(app, run_id)
            return
        ws, run = _find_run(app, run_id)
        if run is None:
            self._send(render.page("not found", f"<p>no run {render._h(run_id)}</p>"), 404)
            return
        if kind == "run":
            offset = int((query.get("offset") or ["0"])[0])
            limit = int((query.get("limit") or [str(_TABLE_PAGE)])[0])
            df = _df_for(ws, run)
            backend = get_backend(run.suite)
            summary = run.stored_summary or backend.format_summary(run)
            table = "<p class='note'>no table</p>"
            if df is not None:
                page_url = f"/run/{run_id}?limit={limit}"
                table = render.table_from_df(df, max_rows=limit, offset=offset, total=df.height, page_url=page_url)
            meta = {
                "run_id": run_id,
                "suite": run.suite,
                "tool": run.tool,
                "binary": run.binary,
                "duration": f"{run.duration_seconds:.1f}s",
                "exit_code": run.exit_code,
                "workspace_name": run.workspace_name or ws.name,
                "task_key": run.task_key,
                "tags": ",".join(run.tags),
                "label": run.label,
                "notes": run.notes,
                "git_commit": run.git_commit,
                "git_branch": run.git_branch,
            }
            self._send(render.run_page(meta, summary, table, bool(_stacks_for(run)), _status(app)))
        elif kind == "flame":
            self._send(self._flame(run_id, run, query))
        else:
            text, trunc, size = self._raw_text(ws, run_id, run)
            self._send(render.raw_page(run_id, text, trunc, size))

    def _download_raw(self, app: object, run_id: str) -> None:
        ws, run = _find_run(app, run_id)
        if run is None:
            self._send("not found", 404)
            return
        try:
            path = ws.get_raw_path(run_id)
            with open(path, "rb") as f:
                data = f.read()
            self._send_bytes(data, "application/octet-stream")
        except (KeyError, OSError):
            inline = getattr(run, "raw_output", "") or ""
            self._send_bytes(inline.encode("utf-8", "replace"), "text/plain; charset=utf-8")

    def _export_run(self, app: object, run_id: str) -> None:
        ws, run = _find_run(app, run_id)
        if run is None:
            self._send("not found", 404)
            return
        import tempfile
        from pathlib import Path

        out = Path(tempfile.gettempdir()) / f"devtools-mcp-{run_id}.zip"
        path = ws._store().export_bundle(run_id, out)
        with open(path, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Disposition", f"attachment; filename={run_id}.zip")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _flame(self, run_id: str, run: object, query: dict) -> str:
        from devtools_mcp.flamegraph.tree import build_call_tree, focus, function_stats, top_leaves

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
        stats = function_stats(tree)
        top = top_leaves(stats, 15)
        hotspot_rows = "".join(
            f"<tr><td>{render._h(name)}</td><td>{exc:,}</td><td>{inc:,}</td></tr>" for name, exc, inc in top
        )
        hotspots = (
            f"<h3>Top functions</h3><table><tr><th>function</th><th>exclusive</th><th>inclusive</th></tr>"
            f"{hotspot_rows}</table>"
            if hotspot_rows
            else ""
        )
        return render.flame_page(run_id, svg, tree.total_weight, focus_name, hotspots)

    def _raw_text(self, ws: object, run_id: str, run: object) -> tuple[str, bool, int]:
        try:
            path = ws.get_raw_path(run_id)
            with open(path, errors="replace") as f:
                text = f.read()
        except (KeyError, FileNotFoundError, OSError):
            text = getattr(run, "raw_output", "") or "(no raw output)"
        size = len(text)
        if size > _RAW_MAX:
            return text[:_RAW_MAX], True, size
        return text, False, size


class VizServer:
    """Owns the background HTTP server thread."""

    def __init__(self, app_ctx: object) -> None:
        self.app_ctx = app_ctx
        self._httpd: ThreadingHTTPServer | None = None
        self._thread = None
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
        import threading

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
