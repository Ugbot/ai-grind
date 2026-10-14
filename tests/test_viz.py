"""Tests for the visualization terminal: HTML rendering + the live HTTP server."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

import polars as pl
import pytest

import devtools_mcp.server  # noqa: F401  (registers backends so suites resolve)
from devtools_mcp.dtrace.models import DTraceResult, DTraceStackTrace
from devtools_mcp.tracker.db import ENV_DB_PATH, open_tracker
from devtools_mcp.viz import render
from devtools_mcp.viz.server import ENV_ALLOWED_HOSTS, VizServer, _local_bound_names, build_allowed_hosts
from devtools_mcp.workspace import AppContext


class TestRender:
    def test_dashboard_escapes_and_lists(self):
        html = render.dashboard(
            [
                {
                    "run_id": "abc12345",
                    "suite": "dtrace",
                    "tool": "profile",
                    "binary": "<script>",
                    "when": "12:00:00",
                    "exit": 0,
                    "has_stacks": True,
                },
            ]
        )
        assert "abc12345" in html
        assert "<script>" not in html.split("</style>")[1].split("<script>")[0]  # body-escaped binary
        assert "/flame/abc12345" in html

    def test_empty_dashboard(self):
        assert "No runs yet" in render.dashboard([])

    def test_table_from_df_bounds_rows(self):
        df = pl.DataFrame({"function": [f"f{i}" for i in range(500)], "value": list(range(500))})
        html = render.table_from_df(df, max_rows=50)
        assert "rows 1-50 of 500" in html

    def test_table_empty(self):
        assert "no rows" in render.table_from_df(pl.DataFrame())


@pytest.fixture
def served():
    app = AppContext()
    ws = app.create_workspace("default")
    app.default_workspace_id = ws.workspace_id
    ws.store_run(
        DTraceResult(
            run_id="abc12345",
            suite="dtrace",
            tool="cpu",
            binary="./app",
            stacks=[
                DTraceStackTrace(frames=["libc`read", "app`process", "app`main"], count=120),
                DTraceStackTrace(frames=["app`hash", "app`process", "app`main"], count=40),
            ],
        )
    )
    srv = VizServer(app)
    url = srv.start(port=0)
    yield url
    srv.stop()


def _get(url: str) -> str:
    return urllib.request.urlopen(url, timeout=5).read().decode()


class TestServiceMode:
    async def test_lifespan_autostarts_dashboard(self, monkeypatch, tmp_path):
        from devtools_mcp.server import app_lifespan
        from devtools_mcp.server import mcp as mcp_app
        from devtools_mcp.tracker.db import ENV_DB_PATH

        monkeypatch.setenv(ENV_DB_PATH, str(tmp_path / "tracker.db"))
        monkeypatch.setenv("DEVTOOLS_MCP_DASHBOARD", "1")
        monkeypatch.setenv("DEVTOOLS_MCP_DASHBOARD_PORT", "0")
        async with app_lifespan(mcp_app) as ctx:
            assert ctx.viz_server is not None and ctx.viz_server.running
            assert _get(ctx.viz_server.url + "/health") == "ok"
            stopped_url = ctx.viz_server.url
        # lifespan exit shuts the dashboard down with the server
        with pytest.raises(OSError):
            _get(stopped_url + "/health")

    async def test_lifespan_without_flag_stays_dark(self, monkeypatch):
        from devtools_mcp.server import app_lifespan
        from devtools_mcp.server import mcp as mcp_app

        monkeypatch.delenv("DEVTOOLS_MCP_DASHBOARD", raising=False)
        async with app_lifespan(mcp_app) as ctx:
            assert ctx.viz_server is None


class TestServer:
    def test_dashboard_route(self, served):
        body = _get(served + "/")
        assert "abc12345" in body
        assert "dtrace:cpu" in body

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


class TestDashboardBinding:
    """The dashboard's bind address + Host allowlist (two-machine tracker sync).

    The sync API lives on this server, so a peer replica's POST /api/crdt/push
    must pass `_guard_state_change`. Loopback-only stays the default; sharing is
    opt-in and must not silently widen the cross-origin guard.
    """

    def test_default_is_loopback_only(self):
        allowed = build_allowed_hosts("127.0.0.1", 8765)
        assert allowed == {
            "127.0.0.1",
            "127.0.0.1:8765",
            "localhost",
            "localhost:8765",
            "::1",
            "::1:8765",
            "[::1]",
            "[::1]:8765",
        }

    def test_wildcard_bind_admits_this_machines_own_addresses(self):
        allowed = build_allowed_hosts("0.0.0.0", 8765)
        assert "127.0.0.1:8765" in allowed  # loopback never lost
        local = _local_bound_names()
        assert local, "expected at least a hostname for this machine"
        for name in local:  # allowlist entries are lowercased (host names are case-insensitive)
            assert name.lower() in allowed and f"{name.lower()}:8765" in allowed

    def test_extra_hosts_are_explicit_opt_in(self):
        # An SSH-tunnel peer sees 127.0.0.1:8766, a DNS alias sees a name.
        allowed = build_allowed_hosts("127.0.0.1", 8765, "127.0.0.1:8766, grind.lan")
        assert "127.0.0.1:8766" in allowed
        assert "grind.lan" in allowed and "grind.lan:8765" in allowed
        assert "evil.example" not in allowed  # no wildcard entry exists

    def test_empty_extra_changes_nothing(self):
        assert build_allowed_hosts("127.0.0.1", 8765, " , ,") == build_allowed_hosts("127.0.0.1", 8765)

    def test_start_reads_allowlist_env(self, monkeypatch):
        monkeypatch.setenv(ENV_ALLOWED_HOSTS, "peer.example")
        srv = VizServer(AppContext())
        try:
            srv.start(port=0)
            port = srv._httpd.server_address[1]
            assert f"peer.example:{port}" in srv._httpd.allowed_hosts
        finally:
            srv.stop()

    def test_wildcard_bind_reports_a_usable_url(self):
        srv = VizServer(AppContext())
        try:
            url = srv.start(host="0.0.0.0", port=0)
            assert url.startswith("http://127.0.0.1:")  # 0.0.0.0 is not dialable
            assert _get(url + "/health") == "ok"
        finally:
            srv.stop()


class TestPeerPushGuard:
    """A real peer (httpx: no Origin header, Host = the address it dialled)."""

    @pytest.fixture
    def shared(self, tmp_path, monkeypatch):
        monkeypatch.setenv(ENV_DB_PATH, str(tmp_path / "tracker.db"))
        open_tracker().close()  # migrate
        srv = VizServer(AppContext())
        srv.start(host="0.0.0.0", port=0)
        yield srv
        srv.stop()

    @staticmethod
    def _push(srv, host_header: str, origin: str | None = None) -> int:
        port = srv._httpd.server_address[1]
        headers = {"Content-Type": "application/json", "Host": host_header}
        if origin is not None:
            headers["Origin"] = origin
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/crdt/push",
            data=json.dumps({"site_id": "peer-site", "ops": []}).encode(),
            headers=headers,
            method="POST",
        )
        try:
            return urllib.request.urlopen(req, timeout=5).status
        except urllib.error.HTTPError as e:
            return e.code

    def test_peer_reaching_us_by_lan_address_succeeds(self, shared):
        port = shared._httpd.server_address[1]
        lan = next((n for n in _local_bound_names() if n[0].isdigit() and ":" not in n), None)
        if lan is None:
            pytest.skip("no non-loopback IPv4 discovered for this host")
        assert self._push(shared, f"{lan}:{port}") == 200

    def test_unknown_host_still_403s(self, shared):
        port = shared._httpd.server_address[1]
        assert self._push(shared, f"attacker.example:{port}") == 403

    def test_cross_origin_still_403s_even_from_an_allowed_host(self, shared):
        # Binding wide must not weaken the browser-facing cross-origin guard.
        port = shared._httpd.server_address[1]
        assert self._push(shared, f"127.0.0.1:{port}", origin="http://evil.example") == 403

    def test_host_matching_is_case_insensitive(self, shared):
        port = shared._httpd.server_address[1]
        name = next((n for n in _local_bound_names() if n.endswith(".local")), None)
        if name is None:
            pytest.skip("no mDNS name discovered for this host")
        assert self._push(shared, f"{name.upper()}:{port}") == 200


class TestDashboardHostEnv:
    def test_maybe_start_dashboard_honours_host_env(self, monkeypatch, tmp_path):
        from devtools_mcp.server import _maybe_start_dashboard

        monkeypatch.setenv(ENV_DB_PATH, str(tmp_path / "tracker.db"))
        monkeypatch.setenv("DEVTOOLS_MCP_DASHBOARD", "1")
        monkeypatch.setenv("DEVTOOLS_MCP_DASHBOARD_PORT", "0")
        monkeypatch.setenv("DEVTOOLS_MCP_DASHBOARD_HOST", "0.0.0.0")
        ctx = AppContext()
        try:
            _maybe_start_dashboard(ctx)
            assert ctx.viz_server is not None and ctx.viz_server.running
            assert ctx.viz_server._httpd.server_address[0] == "0.0.0.0"
        finally:
            if ctx.viz_server is not None:
                ctx.viz_server.stop()

    def test_default_host_env_absent_binds_loopback(self, monkeypatch, tmp_path):
        from devtools_mcp.server import _maybe_start_dashboard

        monkeypatch.setenv(ENV_DB_PATH, str(tmp_path / "tracker.db"))
        monkeypatch.setenv("DEVTOOLS_MCP_DASHBOARD", "1")
        monkeypatch.setenv("DEVTOOLS_MCP_DASHBOARD_PORT", "0")
        monkeypatch.delenv("DEVTOOLS_MCP_DASHBOARD_HOST", raising=False)
        ctx = AppContext()
        try:
            _maybe_start_dashboard(ctx)
            assert ctx.viz_server._httpd.server_address[0] == "127.0.0.1"
        finally:
            if ctx.viz_server is not None:
                ctx.viz_server.stop()
