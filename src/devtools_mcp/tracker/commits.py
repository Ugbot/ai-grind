"""Commit linking: manual links and git-log scanning for task keys.

Scan runs `git log` in a given repository and links any commit whose message
contains a task key (PROJ-123) belonging to a known project. Linking is
idempotent, the (task, hash, repo) UNIQUE constraint dedupes re-scans.
"""

from __future__ import annotations

import os
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


def _git_log(repo_path: str, max_commits: int) -> list[tuple[str, str, str]]:
    """Read (hash, subject, full_message) from git log.

    Uses unit/record separators (\\x1f / \\x1e) so multi-line commit bodies
    parse unambiguously, task keys are scanned in the whole message, not
    just the subject line.
    """
    assert 1 <= max_commits <= SCAN_MAX_COMMITS, f"max_commits {max_commits} out of bounds"
    # Never let git block on a credential/pager prompt: those turn the bounded
    # timeout into an indefinite hang. --no-pager + no terminal prompt + no
    # optional locks keep `git log` a pure, non-interactive read.
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0"}
    try:
        proc = subprocess.run(
            ["git", "--no-pager", "log", f"--max-count={max_commits}", "--pretty=format:%H%x1f%s%x1f%B%x1e"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=GIT_TIMEOUT_SECONDS,
            env=env,
        )
    except FileNotFoundError as exc:
        raise TrackerError("git executable not found on PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise TrackerError(f"git log timed out after {GIT_TIMEOUT_SECONDS}s") from exc
    if proc.returncode != 0:
        raise TrackerError(f"git log failed in {repo_path!r}: {proc.stderr.strip()[:200]}")
    entries: list[tuple[str, str, str]] = []
    for record in proc.stdout.split("\x1e")[:max_commits]:  # bounded by max_commits
        fields = record.strip().split("\x1f")
        if len(fields) != 3 or not fields[0].strip():
            continue
        commit_hash, subject, message = fields
        entries.append((commit_hash.strip(), subject.strip(), message.strip()))
    assert len(entries) <= max_commits, "git log returned more than requested"
    return entries


def link_entries(
    db: TrackerDB,
    repo_path: str,
    entries: list[tuple[str, str, str]],
) -> dict[str, int]:
    """Link pre-fetched (hash, subject, message) git-log entries to tasks.

    Returns counters: scanned, matched, linked (new), skipped_unknown_key.
    Only keys whose project exists in the tracker are linked; an unknown key
    (e.g. some other convention in messages) is counted, not an error.

    Split out from scan_repo so the blocking git read (`_git_log`) can run off
    the event loop while every insert here batches into ONE BEGIN IMMEDIATE
    transaction, not one transaction (and one CRDT-trigger commit) per link,
    which is what made a large scan crawl and starve the server.
    """
    assert isinstance(entries, list), f"entries must be a list, got {type(entries)}"
    if not repo_path.strip():
        raise TrackerError("repo_path must not be empty")
    repo = repo_path.strip()
    known_keys = {row[0] for row in db.conn.execute("SELECT key FROM projects").fetchall()}
    counters = {"scanned": len(entries), "matched": 0, "linked": 0, "skipped_unknown_key": 0}
    with db.transaction() as conn:
        for commit_hash, subject, message in entries:  # bounded: len(entries) <= SCAN_MAX_COMMITS
            chash = commit_hash.strip().lower()
            if not COMMIT_HASH_RE.match(chash):
                continue  # git never emits a malformed hash; defensive skip
            snippet = subject.strip()[:SNIPPET_MAX]
            for task_key in sorted(set(TASK_KEY_SCAN_RE.findall(message))):
                project_key = task_key.rsplit("-", 1)[0]
                if project_key not in known_keys:
                    counters["skipped_unknown_key"] += 1
                    continue
                try:
                    task = get_task(conn, task_key)
                except TrackerError:
                    # Known project, but the task number doesn't exist: count, move on.
                    counters["skipped_unknown_key"] += 1
                    continue
                counters["matched"] += 1
                cursor = conn.execute(
                    "INSERT OR IGNORE INTO task_commits "
                    "(task_id, commit_hash, repo_path, message_snippet, linked_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (task.id, chash, repo, snippet, utc_now_iso()),
                )
                if cursor.rowcount == 1:
                    counters["linked"] += 1
    assert counters["linked"] <= counters["matched"], "linked more than matched"
    assert counters["scanned"] == len(entries), "scanned counter drifted"
    return counters


def scan_repo(
    db: TrackerDB,
    repo_path: str,
    max_commits: int = 500,
) -> dict[str, int]:
    """Scan git log for task keys and auto-link commits (synchronous).

    Thin composition of `_git_log` (blocking subprocess) + `link_entries`
    (single-transaction DB writes). The async handler offloads `_git_log` to a
    worker thread and calls `link_entries` directly so the event loop is never
    blocked; this helper keeps the simple sync path for tests and CLI use.
    """
    if not (1 <= max_commits <= SCAN_MAX_COMMITS):
        raise TrackerError(f"max_commits must be 1..{SCAN_MAX_COMMITS}, got {max_commits}")
    entries = _git_log(repo_path, max_commits)
    return link_entries(db, repo_path, entries)
