"""Tests for git-log scanning: real temp git repos, key matching, dedupe."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from devtools_mcp.tracker import tasks
from devtools_mcp.tracker.commits import commits_for_task, scan_repo
from devtools_mcp.tracker.db import TrackerDB, TrackerError, open_tracker

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git not on PATH")


@pytest.fixture
def db(tmp_path: Path) -> TrackerDB:
    tracker = open_tracker(tmp_path / "tracker.db")
    yield tracker
    tracker.close()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A real git repository with no commits yet."""
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo_dir, check=True)
    return repo_dir


def _commit(repo_dir: Path, message: str) -> str:
    """Create an empty commit and return its hash."""
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            message,
        ],
        cwd=repo_dir,
        check=True,
    )
    proc = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo_dir, check=True, capture_output=True, text=True)
    return proc.stdout.strip()


class TestScan:
    def test_links_known_keys_only(self, db, repo):
        tasks.create_project(db, "GR", "Grind")
        task, _ = tasks.create_task(db, "GR", "tracked work")
        wanted = _commit(repo, f"feat: implement {task.key} core path")
        _commit(repo, "chore: no key here")
        _commit(repo, "fix: OTHER-99 unknown project convention")

        counters = scan_repo(db, str(repo))
        assert counters["scanned"] == 3
        assert counters["matched"] == 1
        assert counters["linked"] == 1
        assert counters["skipped_unknown_key"] == 1

        links = commits_for_task(db, task.key)
        assert [link.commit_hash for link in links] == [wanted]
        assert "core path" in links[0].message_snippet

    def test_key_in_commit_body(self, db, repo):
        tasks.create_project(db, "GR", "Grind")
        task, _ = tasks.create_task(db, "GR", "t")
        wanted = _commit(repo, f"feat: big change\n\nDetails here.\nCloses {task.key}.")
        counters = scan_repo(db, str(repo))
        assert counters["linked"] == 1
        links = commits_for_task(db, task.key)
        assert links[0].commit_hash == wanted
        assert links[0].message_snippet == "feat: big change"  # snippet stays the subject

    def test_same_key_twice_in_message_links_once(self, db, repo):
        tasks.create_project(db, "GR", "Grind")
        task, _ = tasks.create_task(db, "GR", "t")
        _commit(repo, f"{task.key}: fix\n\nReverts part of {task.key}.")
        counters = scan_repo(db, str(repo))
        assert counters["matched"] == 1
        assert counters["linked"] == 1

    def test_rescan_dedupes(self, db, repo):
        tasks.create_project(db, "GR", "Grind")
        task, _ = tasks.create_task(db, "GR", "t")
        _commit(repo, f"{task.key}: first")
        assert scan_repo(db, str(repo))["linked"] == 1
        again = scan_repo(db, str(repo))
        assert again["matched"] == 1
        assert again["linked"] == 0  # idempotent

    def test_one_commit_many_keys(self, db, repo):
        tasks.create_project(db, "GR", "Grind")
        a, _ = tasks.create_task(db, "GR", "a")
        b, _ = tasks.create_task(db, "GR", "b")
        _commit(repo, f"refactor touching {a.key} and {b.key}")
        counters = scan_repo(db, str(repo))
        assert counters["linked"] == 2
        assert len(commits_for_task(db, a.key)) == 1
        assert len(commits_for_task(db, b.key)) == 1

    def test_known_project_unknown_number(self, db, repo):
        tasks.create_project(db, "GR", "Grind")
        _commit(repo, "fix GR-999 which does not exist")
        counters = scan_repo(db, str(repo))
        assert counters["linked"] == 0
        assert counters["matched"] == 0
        assert counters["skipped_unknown_key"] == 1

    def test_max_commits_respected(self, db, repo):
        tasks.create_project(db, "GR", "Grind")
        task, _ = tasks.create_task(db, "GR", "t")
        _commit(repo, f"{task.key}: oldest, beyond window")
        _commit(repo, "middle")
        _commit(repo, "newest")
        counters = scan_repo(db, str(repo), max_commits=2)
        assert counters["scanned"] == 2
        assert counters["linked"] == 0  # the key-bearing commit is outside the window

    def test_not_a_repo_is_error(self, db, tmp_path):
        bare = tmp_path / "notrepo"
        bare.mkdir()
        with pytest.raises(TrackerError, match="git log failed"):
            scan_repo(db, str(bare))

    def test_bad_max_commits(self, db, repo):
        with pytest.raises(TrackerError, match="max_commits"):
            scan_repo(db, str(repo), max_commits=0)
