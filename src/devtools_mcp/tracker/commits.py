"""Commit linking: manual links and git-log scanning for task keys.

Scan runs `git log` in a given repository and links any commit whose message
contains a task key (PROJ-123) belonging to a known project. Linking is
idempotent — the (task, hash, repo) UNIQUE constraint dedupes re-scans.
"""

from __future__ import annotations

import re
import subprocess

from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso
from devtools_mcp.tracker.models import CommitLink
from devtools_mcp.tracker.tasks import get_task

TASK_KEY_SCAN_RE = re.compile(r"\b([A-Z][A-Z0-9]{1,9}-\d+)\b")
SNIPPET_MAX: int = 120
SCAN_MAX_COMMITS: int = 5000
GIT_TIMEOUT_SECONDS: int = 30
COMMIT_HASH_RE = re.compile(r"^[0-9a-f]{7,40}$")


def link_commit(
    db: TrackerDB,
    task_key: str,
    repo_path: str,
    commit_hash: str,
    message_snippet: str = "",
) -> bool:
    """Link a commit to a task. Returns False if the link already existed."""
    commit_hash = commit_hash.strip().lower()
    if not COMMIT_HASH_RE.match(commit_hash):
        raise TrackerError(f"Bad commit hash {commit_hash!r}: need 7-40 hex chars")
    if not repo_path.strip():
        raise TrackerError("repo_path must not be empty")
    snippet = message_snippet.strip()[:SNIPPET_MAX]
    with db.transaction() as conn:
        task = get_task(conn, task_key)
        cursor = conn.execute(
            "INSERT OR IGNORE INTO task_commits "
            "(task_id, commit_hash, repo_path, message_snippet, linked_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (task.id, commit_hash, repo_path.strip(), snippet, utc_now_iso()),
        )
        inserted = cursor.rowcount
    assert inserted in (0, 1), f"inserted {inserted} rows for one link"
    return inserted == 1


def commits_for_task(db: TrackerDB, task_key: str) -> list[CommitLink]:
    """All commit links on a task, newest link first."""
    assert task_key, "empty task key"
    task = get_task(db.conn, task_key)
    rows = db.conn.execute(
        "SELECT * FROM task_commits WHERE task_id = ? ORDER BY id DESC LIMIT ?",
        (task.id, SCAN_MAX_COMMITS),
    ).fetchall()
    links = [CommitLink.from_row(row) for row in rows]
    assert len(links) <= SCAN_MAX_COMMITS, "commit list over bound"
    return links


def _git_log(repo_path: str, max_commits: int) -> list[tuple[str, str]]:
    """Read (hash, subject) pairs from git log; raises TrackerError on git failure."""
    assert 1 <= max_commits <= SCAN_MAX_COMMITS, f"max_commits {max_commits} out of bounds"
    try:
        proc = subprocess.run(
            ["git", "log", f"--max-count={max_commits}", "--pretty=format:%H%x09%s"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=GIT_TIMEOUT_SECONDS,
        )
    except FileNotFoundError as exc:
        raise TrackerError("git executable not found on PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise TrackerError(f"git log timed out after {GIT_TIMEOUT_SECONDS}s") from exc
    if proc.returncode != 0:
        raise TrackerError(f"git log failed in {repo_path!r}: {proc.stderr.strip()[:200]}")
    pairs: list[tuple[str, str]] = []
    for line in proc.stdout.splitlines()[:max_commits]:  # bounded by max_commits
        commit_hash, _, subject = line.partition("\t")
        if commit_hash:
            pairs.append((commit_hash.strip(), subject.strip()))
    assert len(pairs) <= max_commits, "git log returned more than requested"
    return pairs


def scan_repo(
    db: TrackerDB,
    repo_path: str,
    max_commits: int = 500,
) -> dict[str, int]:
    """Scan git log for task keys and auto-link commits.

    Returns counters: scanned, matched, linked (new), skipped_unknown_key.
    Only keys whose project exists in the tracker are linked; an unknown key
    (e.g. some other convention in messages) is counted, not an error.
    """
    if not (1 <= max_commits <= SCAN_MAX_COMMITS):
        raise TrackerError(f"max_commits must be 1..{SCAN_MAX_COMMITS}, got {max_commits}")
    pairs = _git_log(repo_path, max_commits)
    known_keys = {row[0] for row in db.conn.execute("SELECT key FROM projects").fetchall()}
    counters = {"scanned": len(pairs), "matched": 0, "linked": 0, "skipped_unknown_key": 0}
    for commit_hash, subject in pairs:  # bounded by max_commits
        for task_key in TASK_KEY_SCAN_RE.findall(subject):
            project_key = task_key.rsplit("-", 1)[0]
            if project_key not in known_keys:
                counters["skipped_unknown_key"] += 1
                continue
            counters["matched"] += 1
            try:
                if link_commit(db, task_key, repo_path, commit_hash, subject):
                    counters["linked"] += 1
            except TrackerError:
                # Key looks like ours but the task number doesn't exist; count, move on.
                counters["skipped_unknown_key"] += 1
                counters["matched"] -= 1
    assert counters["linked"] <= counters["matched"], "linked more than matched"
    assert counters["scanned"] <= max_commits, "scanned more than requested"
    return counters
