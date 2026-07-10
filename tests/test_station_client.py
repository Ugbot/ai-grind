"""StationClient transport behavior via httpx.MockTransport."""

from __future__ import annotations

import json

import httpx
import pytest

from devtools_mcp.station.client import StationClient, StationError

BASE = "http://station.test"


def _client(handler) -> StationClient:
    http = httpx.Client(
        base_url=BASE,
        headers={"Authorization": "Bearer lls_test"},
        transport=httpx.MockTransport(handler),
    )
    return StationClient(BASE, "lls_test", "org-1", client=http)


class TestTransport:
    def test_auth_header_and_org_path(self):
        seen: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["auth"] = request.headers["Authorization"]
            seen["path"] = request.url.path
            return httpx.Response(200, json=[])

        with _client(handler) as client:
            client.tasks_list("proj-1")
        assert seen["auth"] == "Bearer lls_test"
        assert seen["path"] == "/orgs/org-1/projects/proj-1/tasks"

    def test_error_maps_detail(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(403, json={"detail": "Insufficient project role"})

        with _client(handler) as client, pytest.raises(StationError, match="Insufficient"):
            client.projects_list()

    def test_repo_by_url_404_is_none(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, json={"detail": "Repo not found"})

        with _client(handler) as client:
            assert client.repo_by_url("git@x:y.git") is None

    def test_get_retries_connect_error(self, monkeypatch):
        monkeypatch.setattr("devtools_mcp.station.client.time.sleep", lambda _s: None)
        attempts = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise httpx.ConnectError("refused")
            return httpx.Response(200, json={"member_id": "m1"})

        with _client(handler) as client:
            assert client.auth_me()["member_id"] == "m1"
        assert attempts["n"] == 3

    def test_post_5xx_not_retried(self, monkeypatch):
        monkeypatch.setattr("devtools_mcp.station.client.time.sleep", lambda _s: None)
        attempts = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            attempts["n"] += 1
            return httpx.Response(500, json={"detail": "boom"})

        with _client(handler) as client, pytest.raises(StationError, match="boom"):
            client.task_create("p", {"title": "t"})
        assert attempts["n"] == 1  # non-idempotent: one attempt only

    def test_offline_is_station_error(self, monkeypatch):
        monkeypatch.setattr("devtools_mcp.station.client.time.sleep", lambda _s: None)

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused")

        with _client(handler) as client, pytest.raises(StationError, match="unreachable"):
            client.auth_me()

    def test_payload_shapes(self):
        seen: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen[request.url.path] = json.loads(request.content) if request.content else None
            return httpx.Response(201, json={"id": "x", "acquired": [], "conflicts": []})

        with _client(handler) as client:
            client.checkouts_acquire({"repo_id": "r", "paths": ["a.py"], "ttl_minutes": 10})
            client.perf_upload({"suite": "etw", "tool": "cpu", "data": "{}"})
        assert seen["/orgs/org-1/checkouts"]["paths"] == ["a.py"]
        assert seen["/orgs/org-1/perf-runs"]["suite"] == "etw"
