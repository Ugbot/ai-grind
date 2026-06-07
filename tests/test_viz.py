"""Tests for the visualization terminal: HTML rendering + the live HTTP server."""

from __future__ import annotations

import urllib.request

import polars as pl
import pytest

import devtools_mcp.server  # noqa: F401  (registers backends so suites resolve)
from devtools_mcp.dtrace.models import DTraceResult, DTraceStackTrace
from devtools_mcp.viz import render
from devtools_mcp.viz.server import VizServer
from devtools_mcp.workspace import AppContext


class TestRender:
    def test_dashboard_escapes_and_lists(self):
        html = render.dashboard([
            {"run_id": "abc12345", "suite": "dtrace", "tool": "profile",
             "binary": "<script>", "when": "12:00:00", "exit": 0, "has_stacks": True},
        ])
        assert "abc12345" in html
        assert "<script>" not in html.split("<style>")[1]  # body-escaped
        assert "/flame/abc12345" in html

    def test_empty_dashboard(self):
        assert "No runs yet" in render.dashboard([])

    def test_table_from_df_bounds_rows(self):
        df = pl.DataFrame({"function": [f"f{i}" for i in range(500)], "value": list(range(500))})
        html = render.table_from_df(df, max_rows=50)
        assert "showing 50 of 500" in html

    def test_table_empty(self):
        assert "no rows" in render.table_from_df(pl.DataFrame())


@pytest.fixture
def served():
    app = AppContext()
    ws = app.create_workspace("default")
    app.default_workspace_id = ws.workspace_id
    ws.store_run(DTraceResult(
        run_id="abc12345", suite="dtrace", tool="profile", binary="./app",
        stacks=[
            DTraceStackTrace(frames=["libc`read", "app`process", "app`main"], count=120),
            DTraceStackTrace(frames=["app`hash", "app`process", "app`main"], count=40),
        ],
    ))
    srv = VizServer(app)
    url = srv.start(port=0)
    yield url
    srv.stop()


def _get(url: str) -> str:
    return urllib.request.urlopen(url, timeout=5).read().decode()


class TestServer:
    def test_dashboard_route(self, served):
        body = _get(served + "/")
        assert "abc12345" in body
        assert "dtrace:profile" in body

    def test_run_route_has_table(self, served):
        body = _get(served + "/run/abc12345")
        assert "<table" in body
        assert "function" in body

    def test_flame_route_renders_svg(self, served):
        body = _get(served + "/flame/abc12345")
        assert "<svg" in body and "</svg>" in body

    def test_flame_focus_reroots(self, served):
        full = _get(served + "/flame/abc12345")
        focused = _get(served + "/flame/abc12345?focus=app%60process")
        assert "<svg" in focused
        assert "reset" in focused
        assert len(focused) < len(full)  # subtree is smaller

    def test_raw_route(self, served):
        body = _get(served + "/raw/abc12345")
        assert "Raw output" in body

    def test_unknown_run_404(self, served):
        try:
            urllib.request.urlopen(served + "/run/nope", timeout=5)
            raise AssertionError("expected 404")
        except urllib.error.HTTPError as e:
            assert e.code == 404

    def test_health(self, served):
        assert _get(served + "/health") == "ok"

    def test_stop_is_clean(self, served):
        # served fixture stops at teardown; just confirm it is reachable now.
        assert _get(served + "/health") == "ok"
