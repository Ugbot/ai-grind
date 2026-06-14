"""Tests for the tracker views in the viz terminal + CRDT sync over real HTTP."""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path

import pytest

import devtools_mcp.server  # noqa: F401  (registers backends)
from devtools_mcp.tracker import crdt, criteria, deps, tags, tasks
from devtools_mcp.tracker.db import ENV_DB_PATH, open_tracker
from devtools_mcp.tracker.sync import sync_once
from devtools_mcp.viz import render
from devtools_mcp.viz.server import VizServer
from devtools_mcp.workspace import AppContext


@pytest.fixture
def served_tracker(tmp_path: Path, monkeypatch):
    """A seeded tracker DB served by the viz server (env-pointed)."""
    monkeypatch.setenv(ENV_DB_PATH, str(tmp_path / "served.db"))
    db = open_tracker()
    tasks.create_project(db, "GR", "Grind", "demo project")
    epic, _ = tasks.create_task(db, "GR", "Build the thing", kind="epic")
    story, _ = tasks.create_task(db, "GR", "First story", kind="story", parent_key=epic.key)
    blocked, _ = tasks.create_task(db, "GR", "Blocked work", parent_key=epic.key)
    deps.add_dep(db, blocked.key, story.key)
    criterion = criteria.add_criterion(db, story.id, "it ships", test_ref="t.py::x")
    criteria.record_result(db, criterion.id, "pass")
    tags.add_tag(db, story.id, "user-story")
    db.close()
    srv = VizServer(AppContext())
    url = srv.start(port=0)
    yield url
    srv.stop()


def _get(url: str) -> str:
    return urllib.request.urlopen(url, timeout=5).read().decode()


class TestTrackerPages:
    def test_overview_lists_projects(self, served_tracker):
        body = _get(served_tracker + "/tracker")
        assert "GR" in body and "Grind" in body
        assert "card" in body

    def test_board_columns_and_cards(self, served_tracker):
        body = _get(served_tracker + "/tracker/GR")
        assert "GR-1" in body and "Build the thing" in body
        assert "Open" in body
        assert "What needs to happen" in body
        assert "waiting on" in body  # the blocked card names its blocker

    def test_task_detail(self, served_tracker):
        body = _get(served_tracker + "/tracker/task/GR-2")
        assert "First story" in body
        assert "it ships" in body
        assert "user-story" in body
        assert "t.py::x" in body

    def test_unknown_project_404(self, served_tracker):
        with pytest.raises(urllib.error.HTTPError):
            urllib.request.urlopen(served_tracker + "/tracker/NOPE", timeout=5)

    def test_crdt_status_endpoint(self, served_tracker):
        info = json.loads(_get(served_tracker + "/api/crdt/status"))
        assert info["ops"] > 0
        assert len(info["site_id"]) == 32

    def test_crdt_ops_endpoint(self, served_tracker):
        payload = json.loads(_get(served_tracker + "/api/crdt/ops"))
        assert payload["ops"]
        assert {"hlc", "site_id", "tbl", "pk", "op"} <= set(payload["ops"][0])


class TestHttpSync:
    def test_two_replicas_converge_over_http(self, served_tracker, tmp_path):
        replica = open_tracker(tmp_path / "replica.db")
        try:
            counters = sync_once(replica, served_tracker)
            assert counters["pulled_new"] > 0
            assert counters["pulled_deferred"] == 0
            assert tasks.get_task(replica.conn, "GR-1").title == "Build the thing"

            # Edit locally, sync back, confirm the served side converged.
            tasks.update_task(replica, "GR-1", title="Renamed offline")
            sync_once(replica, served_tracker)
            served_db = open_tracker()  # env still points at the served DB
            try:
                assert tasks.get_task(served_db.conn, "GR-1").title == "Renamed offline"
                assert crdt.canonical_state(served_db.conn) == crdt.canonical_state(replica.conn)
            finally:
                served_db.close()
        finally:
            replica.close()

    def test_self_sync_refused(self, served_tracker):
        from devtools_mcp.tracker.db import TrackerError

        me = open_tracker()  # same DB the server fronts -> same site id
        try:
            with pytest.raises(TrackerError, match="own site_id"):
                sync_once(me, served_tracker)
        finally:
            me.close()


class TestRenderUnits:
    def test_task_card_escapes(self):
        card = render.task_card(
            {
                "key": "GR-1",
                "title": "<script>",
                "kind": "task",
                "status": "open",
                "priority": 3,
                "tags": "a,b",
                "n_children": 2,
                "n_criteria": 4,
                "n_passed": 1,
            }
        )
        assert "<script>" not in card
        assert "GR-1" in card and "2 sub" in card

    def test_overview_empty_state(self):
        body = render.tracker_overview([])
        assert "No projects yet" in body
