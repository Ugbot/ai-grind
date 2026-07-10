"""Tasks-domain station sync against an in-memory fake platform.

The fake speaks just enough of the llm-station-remote REST API (tasks list/
create/patch) to exercise push, pull, echo suppression, conflicts,
deletes, quarantine, and pending-intent recovery end-to-end through
engine.run_sync.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import httpx
import pytest

from devtools_mcp.station import engine, links
from devtools_mcp.station.client import StationClient
from devtools_mcp.station.config import DomainRule, ProjectSection, StationConfig, StationSection
from devtools_mcp.tracker import tasks as tasks_mod
from devtools_mcp.tracker.db import TrackerDB, open_tracker, utc_now_iso

ORG = "org-1"
REMOTE_PROJECT = "proj-1"
BASE = "http://station.test"


class FakePlatform:
    """In-memory platform: org-scoped tasks with server-allocated keys."""

    def __init__(self) -> None:
        self.tasks: dict[str, dict] = {}
        self.next_seq = 1
        self.fail_create_titles: set[str] = set()
        self.writes = 0  # POST/PATCH count, for echo-suppression asserts

    def add_task(self, title: str, status: str = "open", **extra) -> dict:
        task = {
            "id": uuid.uuid4().hex,
            "project_id": REMOTE_PROJECT,
            "key": f"GRIND-{self.next_seq}",
            "parent_id": None,
            "depth": 0,
            "kind": "task",
            "title": title,
            "description": None,
            "status": status,
            "priority": 3,
            "sort_order": 0,
            "milestone_id": None,
            "sprint_id": None,
        }
        task.update(extra)
        self.next_seq += 1
        self.tasks[task["id"]] = task
        return task

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        tasks_base = f"/orgs/{ORG}/projects/{REMOTE_PROJECT}/tasks"
        if request.method == "GET" and path == tasks_base:
            return httpx.Response(200, json=list(self.tasks.values()))
        if request.method == "POST" and path == tasks_base:
            import json

            body = json.loads(request.content)
            self.writes += 1
            if body["title"] in self.fail_create_titles:
                return httpx.Response(400, json={"detail": f"rejected {body['title']!r}"})
            parent_id = body.get("parent_id")
            depth = self.tasks[parent_id]["depth"] + 1 if parent_id in self.tasks else 0
            task = self.add_task(
                body["title"],
                kind=body.get("kind", "task"),
                description=body.get("description"),
                priority=body.get("priority"),
                parent_id=parent_id,
                depth=depth,
            )
            return httpx.Response(201, json=task)
        if request.method == "PATCH" and path.startswith(tasks_base + "/"):
            import json

            task_id = path.rsplit("/", 1)[1]
            self.writes += 1
            if task_id not in self.tasks:
                return httpx.Response(404, json={"detail": "Task not found"})
            self.tasks[task_id].update({k: v for k, v in json.loads(request.content).items() if v is not None})
            return httpx.Response(200, json=self.tasks[task_id])
        return httpx.Response(404, json={"detail": f"unhandled {request.method} {path}"})


@pytest.fixture
def db(tmp_path: Path) -> TrackerDB:
    tracker = open_tracker(tmp_path / "tracker.db")
    tasks_mod.create_project(tracker, "GRIND", "Grind")
    yield tracker
    tracker.close()


@pytest.fixture
def platform() -> FakePlatform:
    return FakePlatform()


@pytest.fixture
def cfg() -> StationConfig:
    return StationConfig(
        station=StationSection(url=BASE, org=ORG),
        project=ProjectSection(local="GRIND", remote=REMOTE_PROJECT),
        domains={"tasks": DomainRule(enabled=True, direction="both")},
        source_path="test://inline",
    )


@pytest.fixture
def linked(db: TrackerDB, cfg: StationConfig) -> None:
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO station_projects (project_key, base_url, org_id, remote_project_id, "
            "remote_project_key, repo_id, member_id, config_hash, linked_at) "
            "VALUES ('GRIND', ?, ?, ?, 'GRIND', NULL, 'm1', ?, ?)",
            (BASE, ORG, REMOTE_PROJECT, cfg.config_hash(), utc_now_iso()),
        )


def _sync(db: TrackerDB, cfg: StationConfig, platform: FakePlatform, dry_run: bool = False) -> list[dict]:
    http = httpx.Client(base_url=BASE, transport=httpx.MockTransport(platform.handler))
    with StationClient(BASE, "lls_test", ORG, client=http) as client:
        return engine.run_sync(db, cfg, ("tasks",), dry_run=dry_run, client=client)


def _tasks_report(reports: list[dict]) -> dict:
    matches = [r for r in reports if r.get("domain") == "tasks"]
    assert len(matches) == 1, f"expected one tasks report, got {reports}"
    return matches[0]


class TestPush:
    def test_create_pushes_hierarchy_and_status(self, db, cfg, platform, linked):
        parent, _ = tasks_mod.create_task(db, "GRIND", "epic thing", kind="epic")
        child, _ = tasks_mod.create_task(db, "GRIND", "child story", kind="story", parent_key=parent.key)
        tasks_mod.set_status(db, child.key, "in_progress")
        report = _tasks_report(_sync(db, cfg, platform))
        assert report["pushed"] == 2
        assert report["errors"] == 0
        by_title = {t["title"]: t for t in platform.tasks.values()}
        assert by_title["epic thing"]["kind"] == "epic"
        assert by_title["child story"]["parent_id"] == by_title["epic thing"]["id"]
        assert by_title["child story"]["status"] == "in_progress"
        link = links.get_link(db.conn, "task", parent.uid)
        assert link is not None and link["state"] == "ok"

    def test_second_run_is_silent(self, db, cfg, platform, linked):
        tasks_mod.create_task(db, "GRIND", "once")
        _sync(db, cfg, platform)
        writes_after_first = platform.writes
        report = _tasks_report(_sync(db, cfg, platform))
        assert platform.writes == writes_after_first  # zero HTTP writes on echo
        assert report["pushed"] == 0

    def test_local_update_patches_remote(self, db, cfg, platform, linked):
        task, _ = tasks_mod.create_task(db, "GRIND", "original")
        _sync(db, cfg, platform)
        tasks_mod.update_task(db, task.key, title="renamed")
        report = _tasks_report(_sync(db, cfg, platform))
        assert report["pushed"] == 1
        assert any(t["title"] == "renamed" for t in platform.tasks.values())

    def test_local_delete_cancels_remote(self, db, cfg, platform, linked):
        task, _ = tasks_mod.create_task(db, "GRIND", "doomed")
        _sync(db, cfg, platform)
        with db.transaction() as conn:
            conn.execute("DELETE FROM tasks WHERE uid = ?", (task.uid,))
        report = _tasks_report(_sync(db, cfg, platform))
        assert report["pushed"] == 1
        assert all(t["status"] == "cancelled" for t in platform.tasks.values())
        link = links.get_link(db.conn, "task", task.uid)
        assert link is not None and link["state"] == "deleted"

    def test_quarantine_continues_and_skips_until_edit(self, db, cfg, platform, linked):
        platform.fail_create_titles.add("bad")
        tasks_mod.create_task(db, "GRIND", "bad")
        tasks_mod.create_task(db, "GRIND", "good")
        report = _tasks_report(_sync(db, cfg, platform))
        assert report["errors"] == 1
        assert any(t["title"] == "good" for t in platform.tasks.values())
        # unchanged quarantined row: skipped, no retry write
        platform.fail_create_titles.clear()
        writes = platform.writes
        report = _tasks_report(_sync(db, cfg, platform))
        assert platform.writes == writes
        # editing the row clears the quarantine and re-pushes
        bad = db.conn.execute("SELECT key FROM tasks WHERE title = 'bad'").fetchone()
        tasks_mod.update_task(db, bad["key"], title="bad but fixed")
        report = _tasks_report(_sync(db, cfg, platform))
        assert report["errors"] == 0
        assert any(t["title"] == "bad but fixed" for t in platform.tasks.values())


class TestPull:
    def test_remote_create_gets_fresh_local_key(self, db, cfg, platform, linked):
        # Local GRIND-1 exists; the platform also allocates GRIND-1 — keys are
        # not identity, the pulled task must get a fresh local key.
        tasks_mod.create_task(db, "GRIND", "local first")
        remote = platform.add_task("from platform")
        assert remote["key"] == "GRIND-1"
        _sync(db, cfg, platform)
        row = db.conn.execute("SELECT * FROM tasks WHERE title = 'from platform'").fetchone()
        assert row is not None
        assert row["key"] != "GRIND-1"  # fresh local key, no collision
        link = links.link_by_remote(db.conn, "task", ORG, remote["id"])
        assert link is not None and link["local_id"] == row["uid"]

    def test_remote_update_applies_locally(self, db, cfg, platform, linked):
        task, _ = tasks_mod.create_task(db, "GRIND", "shared")
        _sync(db, cfg, platform)
        remote_id = links.get_link(db.conn, "task", task.uid)["remote_id"]
        platform.tasks[remote_id]["title"] = "edited remotely"
        platform.tasks[remote_id]["status"] = "done"
        report = _tasks_report(_sync(db, cfg, platform))
        assert report["pulled"] == 1
        row = db.conn.execute("SELECT * FROM tasks WHERE uid = ?", (task.uid,)).fetchone()
        assert row["title"] == "edited remotely"
        assert row["status"] == "done"

    def test_conflict_local_wins_reaches_fixpoint(self, db, cfg, platform, linked):
        task, _ = tasks_mod.create_task(db, "GRIND", "contested")
        _sync(db, cfg, platform)
        remote_id = links.get_link(db.conn, "task", task.uid)["remote_id"]
        platform.tasks[remote_id]["title"] = "remote edit"
        tasks_mod.update_task(db, task.key, title="local edit")
        report = _tasks_report(_sync(db, cfg, platform))
        assert report["conflicts"] == 1
        row = db.conn.execute("SELECT title FROM tasks WHERE uid = ?", (task.uid,)).fetchone()
        assert row["title"] == "local edit"  # local untouched
        assert platform.tasks[remote_id]["title"] == "local edit"  # push overwrote
        # fixpoint: next run writes nothing
        writes = platform.writes
        _sync(db, cfg, platform)
        assert platform.writes == writes

    def test_remote_disappearance_marks_link_only(self, db, cfg, platform, linked):
        task, _ = tasks_mod.create_task(db, "GRIND", "vanishing")
        _sync(db, cfg, platform)
        platform.tasks.clear()
        _sync(db, cfg, platform)
        link = links.get_link(db.conn, "task", task.uid)
        assert link is not None and link["state"] == "deleted"
        assert db.conn.execute("SELECT COUNT(*) FROM tasks WHERE uid = ?", (task.uid,)).fetchone()[0] == 1


class TestRecovery:
    def test_pending_link_resolved_by_title(self, db, cfg, platform, linked):
        # Simulate a crash between POST and resolve: the remote task exists,
        # the local link is still pending.
        task, _ = tasks_mod.create_task(db, "GRIND", "orphaned create")
        remote = platform.add_task("orphaned create")
        links.put_pending(db, "task", task.uid, ORG)
        report = _sync(db, cfg, platform)
        recovery = [r for r in report if r.get("domain") == "recovery"]
        assert recovery and "resolved 1" in recovery[0]["notes"][0]
        link = links.get_link(db.conn, "task", task.uid)
        assert link["state"] == "ok" and link["remote_id"] == remote["id"]
        # and no duplicate was created
        assert sum(1 for t in platform.tasks.values() if t["title"] == "orphaned create") == 1

    def test_pending_without_remote_is_recreated(self, db, cfg, platform, linked):
        task, _ = tasks_mod.create_task(db, "GRIND", "post never landed")
        links.put_pending(db, "task", task.uid, ORG)
        _sync(db, cfg, platform)
        link = links.get_link(db.conn, "task", task.uid)
        assert link is not None and link["state"] == "ok"
        assert sum(1 for t in platform.tasks.values() if t["title"] == "post never landed") == 1


class TestGuards:
    def test_unlinked_project_refuses(self, db, cfg, platform):
        with pytest.raises(Exception, match="not linked"):
            _sync(db, cfg, platform)

    def test_changed_config_refuses(self, db, cfg, platform, linked):
        cfg.domains["tasks"].direction = "push"
        with pytest.raises(Exception, match="changed since"):
            _sync(db, cfg, platform)

    def test_dry_run_writes_nothing(self, db, cfg, platform, linked):
        tasks_mod.create_task(db, "GRIND", "planned only")
        report = _tasks_report(_sync(db, cfg, platform, dry_run=True))
        assert report["pushed"] == 1
        assert platform.writes == 0
        assert links.get_link(db.conn, "task", db.conn.execute("SELECT uid FROM tasks").fetchone()["uid"]) is None


def test_no_llm_station_import() -> None:
    """Hard rule: zero code coupling with the platform repo."""
    station_dir = Path(__file__).resolve().parents[1] / "src" / "devtools_mcp" / "station"
    files = list(station_dir.rglob("*.py"))
    assert files, "station package missing"
    assert len(files) < 50, "station package unexpectedly large"
    import re

    forbidden = re.compile(r"^\s*(import llm_station|from llm_station)", re.MULTILINE)
    for file in files:  # bounded
        text = file.read_text(encoding="utf-8")
        assert forbidden.search(text) is None, f"{file} imports platform code"
