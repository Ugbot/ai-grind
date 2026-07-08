"""Tests for the local agent-collaboration domain: file touches and claims."""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from devtools_mcp.tracker import activity
from devtools_mcp.tracker.activity import (
    ClaimHeldError,
    acquire_claim,
    active_claims,
    conflicts_for,
    normalize,
    recent_activity,
    record_touches,
    release_claims,
    sessions_overview,
)
from devtools_mcp.tracker.db import TrackerDB, TrackerError, open_tracker, utc_now_iso


@pytest.fixture
def db(tmp_path: Path) -> TrackerDB:
    tracker = open_tracker(tmp_path / "tracker.db")
    yield tracker
    tracker.close()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "myrepo"
    (root / ".git").mkdir(parents=True)
    (root / "src").mkdir()
    (root / "src" / "x.py").write_text("pass\n")
    return root


class TestNormalize:
    def test_finds_git_root(self, repo):
        root, rel = normalize(str(repo / "src"), str(repo / "src" / "x.py"))
        assert root == repo.resolve().as_posix()
        assert rel == "src/x.py"

    def test_relative_path_resolved_against_cwd(self, repo):
        root, rel = normalize(str(repo), "src/x.py")
        assert root == repo.resolve().as_posix()
        assert rel == "src/x.py"

    def test_no_git_falls_back_to_cwd(self, tmp_path):
        plain = tmp_path / "plain"
        plain.mkdir()
        (plain / "f.txt").write_text("x")
        root, rel = normalize(str(plain), str(plain / "f.txt"))
        assert root == plain.resolve().as_posix()
        assert rel == "f.txt"

    def test_posix_slashes(self, repo):
        _, rel = normalize(str(repo), str(repo / "src" / "x.py"))
        assert "\\" not in rel


class TestRecordTouches:
    def test_insert_and_read_back(self, db, repo):
        n = record_touches(db, "sess-a", str(repo), ["src/x.py"], agent_label="alice", tool_name="Edit")
        assert n == 1
        touches = recent_activity(db.conn)
        assert len(touches) == 1
        assert touches[0].session_id == "sess-a"
        assert touches[0].agent_label == "alice"
        assert touches[0].file_path == "src/x.py"

    def test_debounce_updates_instead_of_inserting(self, db, repo):
        record_touches(db, "sess-a", str(repo), ["src/x.py"])
        record_touches(db, "sess-a", str(repo), ["src/x.py"])
        count = db.conn.execute("SELECT COUNT(*) FROM file_activity").fetchone()[0]
        assert count == 1

    def test_different_sessions_do_not_debounce(self, db, repo):
        record_touches(db, "sess-a", str(repo), ["src/x.py"])
        record_touches(db, "sess-b", str(repo), ["src/x.py"])
        count = db.conn.execute("SELECT COUNT(*) FROM file_activity").fetchone()[0]
        assert count == 2

    def test_bad_task_key_rejected(self, db, repo):
        with pytest.raises(TrackerError, match="Bad task key"):
            record_touches(db, "sess-a", str(repo), ["src/x.py"], task_key="not a key")

    def test_bad_op_rejected(self, db, repo):
        with pytest.raises(TrackerError, match="Bad op"):
            record_touches(db, "sess-a", str(repo), ["src/x.py"], op="delete")

    def test_empty_files_rejected(self, db, repo):
        with pytest.raises(TrackerError, match="files must not be empty"):
            record_touches(db, "sess-a", str(repo), [])

    def test_too_many_files_rejected(self, db, repo):
        files = [f"f{i}.py" for i in range(51)]
        with pytest.raises(TrackerError, match="too many files"):
            record_touches(db, "sess-a", str(repo), files)

    def test_empty_session_rejected(self, db, repo):
        with pytest.raises(TrackerError, match="session_id"):
            record_touches(db, "", str(repo), ["src/x.py"])

    def test_prune_bounds_per_repo(self, db, repo, monkeypatch):
        monkeypatch.setattr(activity, "ACTIVITY_MAX_PER_REPO", 5)
        for i in range(8):
            record_touches(db, f"sess-{i}", str(repo), [f"file{i}.py"])
        count = db.conn.execute("SELECT COUNT(*) FROM file_activity").fetchone()[0]
        assert count == 5


class TestClaims:
    def test_acquire_then_other_session_blocked(self, db, repo):
        claim = acquire_claim(db, "sess-a", str(repo), "src/x.py", agent_label="alice", ttl_s=60)
        assert claim.active
        with pytest.raises(ClaimHeldError) as exc:
            acquire_claim(db, "sess-b", str(repo), "src/x.py")
        assert exc.value.holder.session_id == "sess-a"
        assert "alice" in str(exc.value)

    def test_renew_own_claim(self, db, repo):
        first = acquire_claim(db, "sess-a", str(repo), "src/x.py", ttl_s=60)
        second = acquire_claim(db, "sess-a", str(repo), "src/x.py", ttl_s=3600)
        assert second.id == first.id
        assert second.expires_at > first.expires_at

    def test_expired_claim_is_reaped_on_acquire(self, db, repo):
        acquire_claim(db, "sess-a", str(repo), "src/x.py", ttl_s=60)
        past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
        with db.transaction() as conn:
            conn.execute("UPDATE file_claims SET expires_at = ?", (past,))
        claim = acquire_claim(db, "sess-b", str(repo), "src/x.py")
        assert claim.session_id == "sess-b"

    def test_release_frees_the_file(self, db, repo):
        claim = acquire_claim(db, "sess-a", str(repo), "src/x.py")
        released = release_claims(db, "sess-a", repo_root=claim.repo_root, file_path=claim.file_path)
        assert released == 1
        fresh = acquire_claim(db, "sess-b", str(repo), "src/x.py")
        assert fresh.session_id == "sess-b"

    def test_release_all_for_session(self, db, repo):
        acquire_claim(db, "sess-a", str(repo), "src/x.py")
        acquire_claim(db, "sess-a", str(repo), "src/y.py")
        assert release_claims(db, "sess-a") == 2
        assert active_claims(db.conn) == []

    def test_touch_renews_own_claim(self, db, repo):
        claim = acquire_claim(db, "sess-a", str(repo), "src/x.py", ttl_s=30)
        record_touches(db, "sess-a", str(repo), ["src/x.py"])
        row = db.conn.execute("SELECT expires_at FROM file_claims WHERE id = ?", (claim.id,)).fetchone()
        assert row["expires_at"] > claim.expires_at

    def test_bad_ttl_rejected(self, db, repo):
        with pytest.raises(TrackerError, match="ttl_s out of range"):
            acquire_claim(db, "sess-a", str(repo), "src/x.py", ttl_s=0)
        with pytest.raises(TrackerError, match="ttl_s out of range"):
            acquire_claim(db, "sess-a", str(repo), "src/x.py", ttl_s=9 * 3600)

    def test_concurrent_acquire_single_winner(self, tmp_path, repo):
        """Two connections racing for one file: exactly one wins."""
        path = tmp_path / "race.db"
        open_tracker(path).close()  # migrate once
        results: list[str] = []
        errors: list[Exception] = []
        barrier = threading.Barrier(2)

        def contender(name: str) -> None:
            tracker = open_tracker(path)
            try:
                barrier.wait(timeout=10)
                acquire_claim(tracker, name, str(repo), "src/x.py")
                results.append(name)
            except ClaimHeldError as exc:
                errors.append(exc)
            finally:
                tracker.close()

        threads = [threading.Thread(target=contender, args=(f"sess-{i}",)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert len(results) == 1, f"winners: {results}, errors: {errors}"
        assert len(errors) == 1


class TestConflicts:
    def test_other_claim_reported(self, db, repo):
        claim = acquire_claim(db, "sess-a", str(repo), "src/x.py", agent_label="alice", task_key=None)
        found = conflicts_for(db.conn, "sess-b", claim.repo_root, claim.file_path)
        assert len(found) == 1
        assert found[0]["kind"] == "claim"
        assert found[0]["session_id"] == "sess-a"

    def test_own_claim_not_a_conflict(self, db, repo):
        claim = acquire_claim(db, "sess-a", str(repo), "src/x.py")
        assert conflicts_for(db.conn, "sess-a", claim.repo_root, claim.file_path) == []

    def test_recent_touch_by_other_reported(self, db, repo):
        record_touches(db, "sess-a", str(repo), ["src/x.py"], agent_label="alice")
        root, rel = normalize(str(repo), "src/x.py")
        found = conflicts_for(db.conn, "sess-b", root, rel)
        assert len(found) == 1
        assert found[0]["kind"] == "recent_touch"

    def test_claim_shadows_touch_for_same_session(self, db, repo):
        acquire_claim(db, "sess-a", str(repo), "src/x.py")
        record_touches(db, "sess-a", str(repo), ["src/x.py"])
        root, rel = normalize(str(repo), "src/x.py")
        found = conflicts_for(db.conn, "sess-b", root, rel)
        kinds = [c["kind"] for c in found]
        assert kinds == ["claim"]  # one entry per session, claim wins


class TestOverview:
    def test_sessions_overview(self, db, repo):
        record_touches(db, "sess-a", str(repo), ["src/x.py"], agent_label="alice")
        record_touches(db, "sess-b", str(repo), ["src/y.py"])
        acquire_claim(db, "sess-a", str(repo), "src/x.py")
        sessions = sessions_overview(db.conn)
        assert len(sessions) == 2
        by_id = {s["session_id"]: s for s in sessions}
        assert by_id["sess-a"]["claims"] == 1
        assert by_id["sess-a"]["agent_label"] == "alice"
        assert by_id["sess-b"]["claims"] == 0

    def test_recent_activity_scoped_and_bounded(self, db, repo):
        record_touches(db, "sess-a", str(repo), ["a.py", "b.py", "c.py"])
        touches = recent_activity(db.conn, repo_root=repo.resolve().as_posix(), limit=2)
        assert len(touches) == 2
        assert recent_activity(db.conn, repo_root="/nowhere") == []

    def test_active_claims_excludes_expired(self, db, repo):
        acquire_claim(db, "sess-a", str(repo), "src/x.py")
        past = (datetime.now(UTC) - timedelta(seconds=5)).isoformat()
        with db.transaction() as conn:
            conn.execute("UPDATE file_claims SET expires_at = ?", (past,))
        assert active_claims(db.conn) == []
        assert utc_now_iso() > past
