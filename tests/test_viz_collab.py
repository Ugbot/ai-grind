"""Tests for the local agent-collaboration surface of the viz server:
POST /api/collab/touch ingest, conflict/status APIs, and the /collab page."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from pathlib import Path

import pytest

import devtools_mcp.server  # noqa: F401  (registers backends)
from devtools_mcp.tracker import activity
from devtools_mcp.tracker.db import ENV_DB_PATH, open_tracker
from devtools_mcp.viz.server import VizServer
from devtools_mcp.workspace import AppContext


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / ".git").mkdir(parents=True)
    (root / "src").mkdir()
    (root / "src" / "x.py").write_text("pass")
    return root


@pytest.fixture
def served(tmp_path: Path, monkeypatch):
    """An empty tracker DB served by the viz server (env-pointed)."""
    monkeypatch.setenv(ENV_DB_PATH, str(tmp_path / "collab.db"))
    open_tracker().close()  # migrate
    srv = VizServer(AppContext())
    url = srv.start(port=0)
    yield url
    srv.stop()


def _get(url: str) -> str:
    return urllib.request.urlopen(url, timeout=5).read().decode()


def _post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=5).read().decode())


def _touch(url: str, session: str, repo: Path, files: list[str], **extra) -> dict:
    payload = {"session_id": session, "cwd": str(repo), "files": files, "tool": "Edit", **extra}
    return _post_json(url + "/api/collab/touch", payload)


class TestTouchIngest:
    def test_touch_recorded(self, served, repo):
        result = _touch(served, "sess-a", repo, ["src/x.py"], agent="alice")
        assert result["recorded"] == 1
        assert result["conflicts"] == []
        db = open_tracker()
        try:
            touches = activity.recent_activity(db.conn)
            assert len(touches) == 1
            assert touches[0].agent_label == "alice"
        finally:
            db.close()

    def test_touch_returns_conflicts_from_other_session(self, served, repo):
        _touch(served, "sess-a", repo, ["src/x.py"], agent="alice")
        result = _touch(served, "sess-b", repo, ["src/x.py"], agent="bob")
        assert result["recorded"] == 1
        assert len(result["conflicts"]) == 1
        assert result["conflicts"][0]["kind"] == "recent_touch"
        assert result["conflicts"][0]["agent"] == "alice"

    def test_bad_task_key_dropped_not_rejected(self, served, repo):
        result = _touch(served, "sess-a", repo, ["src/x.py"], task_key="not a key")
        assert result["recorded"] == 1

    def test_missing_session_is_400(self, served, repo):
        with pytest.raises(urllib.error.HTTPError) as exc:
            _post_json(served + "/api/collab/touch", {"files": ["x"], "cwd": str(repo)})
        assert exc.value.code == 400

    def test_empty_body_is_400(self, served):
        req = urllib.request.Request(served + "/api/collab/touch", data=b"")
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(req, timeout=5)
        assert exc.value.code == 400


class TestCollabApis:
    def test_conflicts_endpoint(self, served, repo):
        _touch(served, "sess-a", repo, ["src/x.py"], agent="alice")
        path = str(repo / "src" / "x.py").replace("\\", "/")
        body = json.loads(_get(served + f"/api/collab/conflicts?session=sess-b&path={urllib.request.quote(path)}"))
        assert body["file"] == "src/x.py"
        assert len(body["conflicts"]) == 1

    def test_conflicts_requires_params(self, served):
        with pytest.raises(urllib.error.HTTPError) as exc:
            _get(served + "/api/collab/conflicts")
        assert exc.value.code == 400

    def test_status_endpoint_shapes(self, served, repo):
        _touch(served, "sess-a", repo, ["src/x.py"], agent="alice")
        db = open_tracker()
        try:
            activity.acquire_claim(db, "sess-a", str(repo), "src/x.py", agent_label="alice")
        finally:
            db.close()
        body = json.loads(_get(served + "/api/collab/status"))
        assert body["sessions"][0]["session_id"] == "sess-a"
        assert body["claims"][0]["file_path"] == "src/x.py"
        assert body["recent"][0]["file_path"] == "src/x.py"


class TestCollabPage:
    def test_empty_page_renders(self, served):
        body = _get(served + "/collab")
        assert "Agent collaboration" in body
        assert "coming soon" in body

    def test_sessions_claims_touches_render(self, served, repo):
        _touch(served, "sess-a", repo, ["src/x.py"], agent="alice")
        _touch(served, "sess-b", repo, ["src/x.py"], agent="bob")
        db = open_tracker()
        try:
            activity.acquire_claim(db, "sess-a", str(repo), "src/y.py", agent_label="alice")
        finally:
            db.close()
        body = _get(served + "/collab")
        assert "alice" in body and "bob" in body
        assert "src/x.py" in body and "src/y.py" in body
        assert "stuck" in body  # contested row highlight (two sessions on x.py)

    def test_task_detail_shows_file_activity(self, served, repo):
        from devtools_mcp.tracker import tasks

        db = open_tracker()
        try:
            tasks.create_project(db, "CL", "Collab")
            tasks.create_task(db, "CL", "the work")
        finally:
            db.close()
        _touch(served, "sess-a", repo, ["src/x.py"], agent="alice", task_key="CL-1")
        body = _get(served + "/tracker/task/CL-1")
        assert "File activity" in body
        assert "src/x.py" in body
