"""End-to-end tests for the Claude Code collab hooks: run the real scripts as
subprocesses feeding stdin JSON, against a live VizServer — no mocks."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

import devtools_mcp.server  # noqa: F401  (registers backends)
from devtools_mcp.tracker import activity
from devtools_mcp.tracker.db import ENV_DB_PATH, open_tracker
from devtools_mcp.viz.server import VizServer
from devtools_mcp.workspace import AppContext

HOOKS_DIR = Path(__file__).resolve().parent.parent / "hooks"
REPORT = HOOKS_DIR / "report_touch.py"
CHECK = HOOKS_DIR / "check_conflict.py"


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / ".git").mkdir(parents=True)
    (root / "src").mkdir()
    (root / "src" / "x.py").write_text("pass")
    return root


@pytest.fixture
def served(tmp_path: Path, monkeypatch):
    monkeypatch.setenv(ENV_DB_PATH, str(tmp_path / "hooks.db"))
    open_tracker().close()
    srv = VizServer(AppContext())
    url = srv.start(port=0)
    yield url
    srv.stop()


def _run_hook(script: Path, event: dict, env_extra: dict[str, str]) -> subprocess.CompletedProcess:
    import os

    env = dict(os.environ)
    env.update(env_extra)
    return subprocess.run(
        [sys.executable, str(script)],
        input=json.dumps(event),
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )


def _edit_event(repo: Path, session: str, file: str = "src/x.py") -> dict:
    return {
        "session_id": session,
        "tool_name": "Edit",
        "cwd": str(repo),
        "tool_input": {"file_path": str(repo / file)},
    }


class TestReportTouch:
    def test_touch_lands_in_db(self, served, repo):
        result = _run_hook(REPORT, _edit_event(repo, "hook-sess"), {"DEVTOOLS_MCP_COLLAB_URL": served})
        assert result.returncode == 0
        assert result.stdout == ""  # no conflicts, no output
        db = open_tracker()
        try:
            touches = activity.recent_activity(db.conn)
            assert len(touches) == 1
            assert touches[0].session_id == "hook-sess"
            assert touches[0].file_path == "src/x.py"
        finally:
            db.close()

    def test_agent_and_task_env_attached(self, served, repo):
        db = open_tracker()
        try:
            from devtools_mcp.tracker import tasks

            tasks.create_project(db, "HK", "Hooks")
            tasks.create_task(db, "HK", "hook work")
        finally:
            db.close()
        _run_hook(
            REPORT,
            _edit_event(repo, "hook-sess"),
            {"DEVTOOLS_MCP_COLLAB_URL": served, "DEVTOOLS_MCP_AGENT": "alice", "DEVTOOLS_MCP_TASK": "HK-1"},
        )
        db = open_tracker()
        try:
            touch = activity.recent_activity(db.conn)[0]
            assert touch.agent_label == "alice"
            assert touch.task_key == "HK-1"
        finally:
            db.close()

    def test_conflict_emits_additional_context(self, served, repo):
        _run_hook(
            REPORT, _edit_event(repo, "sess-a"), {"DEVTOOLS_MCP_COLLAB_URL": served, "DEVTOOLS_MCP_AGENT": "alice"}
        )
        result = _run_hook(REPORT, _edit_event(repo, "sess-b"), {"DEVTOOLS_MCP_COLLAB_URL": served})
        assert result.returncode == 0
        out = json.loads(result.stdout)
        context = out["hookSpecificOutput"]["additionalContext"]
        assert "devtools-collab" in context and "alice" in context

    def test_offline_is_silent_and_fast(self, repo):
        start = time.monotonic()
        result = _run_hook(REPORT, _edit_event(repo, "s"), {"DEVTOOLS_MCP_COLLAB_URL": "http://127.0.0.1:9"})
        elapsed = time.monotonic() - start
        assert result.returncode == 0
        assert result.stdout == ""
        assert elapsed < 10  # interpreter startup dominates; the HTTP timeout is 0.5s

    def test_kill_switch(self, served, repo):
        result = _run_hook(
            REPORT, _edit_event(repo, "s"), {"DEVTOOLS_MCP_COLLAB_URL": served, "DEVTOOLS_MCP_COLLAB": "0"}
        )
        assert result.returncode == 0 and result.stdout == ""
        db = open_tracker()
        try:
            assert activity.recent_activity(db.conn) == []
        finally:
            db.close()

    def test_non_file_tool_ignored(self, served, repo):
        event = {"session_id": "s", "tool_name": "Bash", "cwd": str(repo), "tool_input": {"command": "ls"}}
        result = _run_hook(REPORT, event, {"DEVTOOLS_MCP_COLLAB_URL": served})
        assert result.returncode == 0 and result.stdout == ""


class TestCheckConflict:
    def _claim_as(self, repo: Path, session: str, agent: str) -> None:
        db = open_tracker()
        try:
            activity.acquire_claim(db, session, str(repo), "src/x.py", agent_label=agent)
        finally:
            db.close()

    def test_no_conflict_no_output(self, served, repo):
        result = _run_hook(CHECK, _edit_event(repo, "sess-b"), {"DEVTOOLS_MCP_COLLAB_URL": served})
        assert result.returncode == 0 and result.stdout == ""

    def test_warn_mode_emits_context(self, served, repo):
        self._claim_as(repo, "sess-a", "alice")
        result = _run_hook(CHECK, _edit_event(repo, "sess-b"), {"DEVTOOLS_MCP_COLLAB_URL": served})
        out = json.loads(result.stdout)
        assert "additionalContext" in out["hookSpecificOutput"]
        assert "alice" in out["hookSpecificOutput"]["additionalContext"]

    def test_ask_mode_claims_prompt(self, served, repo):
        self._claim_as(repo, "sess-a", "alice")
        result = _run_hook(
            CHECK,
            _edit_event(repo, "sess-b"),
            {"DEVTOOLS_MCP_COLLAB_URL": served, "DEVTOOLS_MCP_COLLAB_MODE": "ask"},
        )
        out = json.loads(result.stdout)
        assert out["hookSpecificOutput"]["permissionDecision"] == "ask"
        assert "alice" in out["hookSpecificOutput"]["permissionDecisionReason"]

    def test_ask_mode_recent_touch_only_warns(self, served, repo):
        db = open_tracker()
        try:
            activity.record_touches(db, "sess-a", str(repo), ["src/x.py"], agent_label="alice")
        finally:
            db.close()
        result = _run_hook(
            CHECK,
            _edit_event(repo, "sess-b"),
            {"DEVTOOLS_MCP_COLLAB_URL": served, "DEVTOOLS_MCP_COLLAB_MODE": "ask"},
        )
        out = json.loads(result.stdout)
        assert "permissionDecision" not in out["hookSpecificOutput"]
        assert "additionalContext" in out["hookSpecificOutput"]

    def test_off_mode_silent(self, served, repo):
        self._claim_as(repo, "sess-a", "alice")
        result = _run_hook(
            CHECK,
            _edit_event(repo, "sess-b"),
            {"DEVTOOLS_MCP_COLLAB_URL": served, "DEVTOOLS_MCP_COLLAB_MODE": "off"},
        )
        assert result.returncode == 0 and result.stdout == ""

    def test_own_claim_no_prompt(self, served, repo):
        self._claim_as(repo, "sess-b", "bob")
        result = _run_hook(
            CHECK,
            _edit_event(repo, "sess-b"),
            {"DEVTOOLS_MCP_COLLAB_URL": served, "DEVTOOLS_MCP_COLLAB_MODE": "ask"},
        )
        assert result.returncode == 0 and result.stdout == ""
