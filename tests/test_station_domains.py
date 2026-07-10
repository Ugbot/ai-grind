"""Claims/skills/perf station domains: unit behavior with a fake platform."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from devtools_mcp.station import engine, links
from devtools_mcp.station.client import StationClient
from devtools_mcp.station.config import DomainRule, ProjectSection, StationConfig, StationSection
from devtools_mcp.station.domains.claims import _ttl_minutes, remote_conflicts_for
from devtools_mcp.tracker import activity
from devtools_mcp.tracker import tasks as tasks_mod
from devtools_mcp.tracker.db import TrackerDB, open_tracker, utc_now_iso

ORG = "org-1"
REMOTE_PROJECT = "proj-1"
REPO_ID = "repo-9"
BASE = "http://station.test"


class FakePlatform:
    """Checkouts + skills + perf endpoints, enough for the three domains."""

    def __init__(self) -> None:
        self.checkouts: dict[str, dict] = {}
        self.skills: dict[str, dict] = {}
        self.perf_runs: list[dict] = []
        self.heartbeats: list[list[str]] = []
        self.released: list[str] = []

    def add_checkout(self, member_id: str, path: str) -> dict:
        checkout = {
            "id": uuid.uuid4().hex,
            "repo_id": REPO_ID,
            "member_id": member_id,
            "path": path,
            "path_type": "file",
            "mode": "exclusive",
            "intent": None,
            "task_key": None,
            "checked_out_at": utc_now_iso(),
            "expires_at": utc_now_iso(),
            "released_at": None,
        }
        self.checkouts[checkout["id"]] = checkout
        return checkout

    def handler(self, request: httpx.Request) -> httpx.Response:
        path, method = request.url.path, request.method
        body = json.loads(request.content) if request.content else {}
        if path == f"/orgs/{ORG}/checkouts" and method == "POST":
            acquired = [self.add_checkout("m1", p) for p in body["paths"]]
            return httpx.Response(201, json={"acquired": acquired, "conflicts": []})
        if path == f"/orgs/{ORG}/checkouts" and method == "GET":
            return httpx.Response(200, json=[c for c in self.checkouts.values() if c["released_at"] is None])
        if path == f"/orgs/{ORG}/checkouts/release":
            self.released.extend(body["checkout_ids"])
            for checkout_id in body["checkout_ids"]:
                if checkout_id in self.checkouts:
                    self.checkouts[checkout_id]["released_at"] = utc_now_iso()
            return httpx.Response(204)
        if path == f"/orgs/{ORG}/checkouts/heartbeat":
            self.heartbeats.append(body["checkout_ids"])
            return httpx.Response(200, json={"extended": len(body["checkout_ids"])})
        if path == f"/orgs/{ORG}/skills" and method == "POST":
            skill = {"id": uuid.uuid4().hex, "name": body["name"], **body}
            self.skills[body["name"]] = skill
            return httpx.Response(201, json=skill)
        if path == f"/orgs/{ORG}/perf-runs" and method == "POST":
            run = {"id": uuid.uuid4().hex, **body}
            self.perf_runs.append(run)
            return httpx.Response(201, json=run)
        if path == f"/orgs/{ORG}/perf-runs" and method == "GET":
            return httpx.Response(200, json=self.perf_runs)
        return httpx.Response(404, json={"detail": f"unhandled {method} {path}"})


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
def repo_root(tmp_path: Path) -> Path:
    root = tmp_path / "workrepo"
    (root / ".devtools-mcp").mkdir(parents=True)
    (root / ".devtools-mcp" / "station.toml").write_text("# test", encoding="utf-8")
    return root


def _cfg(repo_root: Path, **domains: DomainRule) -> StationConfig:
    return StationConfig(
        station=StationSection(url=BASE, org=ORG),
        project=ProjectSection(local="GRIND", remote=REMOTE_PROJECT),
        domains=dict(domains),
        source_path=str(repo_root / ".devtools-mcp" / "station.toml"),
    )


def _link(db: TrackerDB, cfg: StationConfig) -> None:
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO station_projects (project_key, base_url, org_id, remote_project_id, "
            "remote_project_key, repo_id, member_id, config_hash, linked_at) "
            "VALUES ('GRIND', ?, ?, ?, 'GRIND', ?, 'm1', ?, ?)",
            (BASE, ORG, REMOTE_PROJECT, REPO_ID, cfg.config_hash(), utc_now_iso()),
        )


def _sync(db: TrackerDB, cfg: StationConfig, platform: FakePlatform, domain: str) -> dict:
    http = httpx.Client(base_url=BASE, transport=httpx.MockTransport(platform.handler))
    with StationClient(BASE, "lls_test", ORG, client=http) as client:
        reports = engine.run_sync(db, cfg, (domain,), client=client)
    matches = [r for r in reports if r.get("domain") == domain]
    assert len(matches) == 1, f"expected one {domain} report, got {reports}"
    return matches[0]


class TestClaims:
    def test_ttl_covers_local_lease_with_slack(self):
        expires = (datetime.now(UTC) + timedelta(minutes=10)).isoformat()
        assert 14 <= _ttl_minutes(expires) <= 16  # ~10 + 5 slack
        long_expires = (datetime.now(UTC) + timedelta(hours=100)).isoformat()
        assert _ttl_minutes(long_expires) == 480  # clamped
        assert _ttl_minutes("garbage") == 2  # floor on unparseable

    def test_claim_acquires_then_heartbeats_then_releases(self, db, platform, repo_root):
        cfg = _cfg(repo_root, collab=DomainRule(enabled=True, direction="both"))
        _link(db, cfg)
        claim = activity.acquire_claim(db, "s1", str(repo_root), "src/main.py")
        report = _sync(db, cfg, platform, "collab")
        assert report["pushed"] == 1
        assert len(platform.checkouts) == 1
        # second run: heartbeat, not re-acquire
        report = _sync(db, cfg, platform, "collab")
        assert platform.heartbeats and len(platform.checkouts) == 1
        # release locally -> checkout released remotely
        activity.release_claims(db, "s1")
        report = _sync(db, cfg, platform, "collab")
        assert platform.released
        link = links.get_link(db.conn, "claim", str(claim.id))
        assert link is not None and link["state"] == "deleted"

    def test_mirror_excludes_self_and_never_touches_claims(self, db, platform, repo_root):
        cfg = _cfg(repo_root, collab=DomainRule(enabled=True, direction="both"))
        _link(db, cfg)
        platform.add_checkout("m1", "mine.py")  # self: excluded
        platform.add_checkout("m2", "theirs.py")  # other member: mirrored
        report = _sync(db, cfg, platform, "collab")
        assert report["pulled"] == 1
        mirror = db.conn.execute("SELECT * FROM station_remote_checkouts").fetchall()
        assert len(mirror) == 1 and mirror[0]["member_id"] == "m2"
        assert db.conn.execute("SELECT COUNT(*) FROM file_claims").fetchone()[0] == 0
        conflicts = remote_conflicts_for(db.conn, "theirs.py")
        assert conflicts and conflicts[0]["member_id"] == "m2"


class TestSkills:
    def test_manifest_diff_pushes_once(self, db, platform, repo_root, tmp_path, monkeypatch):
        skills_root = tmp_path / "skillslib"
        skills_root.mkdir()
        (skills_root / "myskill").mkdir()
        (skills_root / "myskill" / "SKILL.md").write_text("# my skill", encoding="utf-8")
        item = {"name": "myskill", "type": "skill", "category": "dev", "dest": "myskill", "sha256": "a" * 64}
        (skills_root / "MANIFEST.json").write_text(json.dumps({"items": [item]}), encoding="utf-8")
        monkeypatch.setenv("DEVTOOLS_MCP_SKILLS_ROOT", str(skills_root))
        cfg = _cfg(repo_root, skills=DomainRule(enabled=True, direction="push"))
        _link(db, cfg)
        report = _sync(db, cfg, platform, "skills")
        assert report["pushed"] == 1
        assert platform.skills["myskill"]["body"] == "# my skill"
        report = _sync(db, cfg, platform, "skills")
        assert report["pushed"] == 0 and report["skipped"] == 1  # hash-stable


class TestPerf:
    def test_run_uploads_once_and_recovers_from_tags(self, db, platform, repo_root, tmp_path, monkeypatch):
        runs_root = tmp_path / "datadir"
        run_dir = runs_root / "runs" / "run-abc123"
        run_dir.mkdir(parents=True)
        (run_dir / "result.json").write_text("{}", encoding="utf-8")
        (run_dir / "meta.json").write_text(json.dumps({"suite": "etw", "tool": "cpu"}), encoding="utf-8")
        from devtools_mcp.store.run_store import RunStore

        monkeypatch.setattr("devtools_mcp.station.domains.perf.RunStore", lambda: RunStore(runs_root))
        cfg = _cfg(repo_root, perf=DomainRule(enabled=True, direction="push"))
        _link(db, cfg)
        report = _sync(db, cfg, platform, "perf")
        assert report["pushed"] == 1
        assert platform.perf_runs[0]["tags"] == ["local-run:run-abc123"]
        # drop the link: recovery via tags must not re-upload
        with db.transaction() as conn:
            conn.execute("DELETE FROM station_links WHERE domain = 'perf_run'")
        report = _sync(db, cfg, platform, "perf")
        assert report["pushed"] == 0
        assert len(platform.perf_runs) == 1
        assert "recovered 1" in " ".join(report["notes"])
