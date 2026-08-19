"""Local agent collaboration: file-touch activity and advisory file claims.

Multiple agents on one machine share the tracker DB; this module records which
files each session touches (fed by Claude Code hooks or the tracker_files
tool), grants short advisory leases ("claims") on files, and answers "who else
is working here?". Everything is site-local, deliberately outside the CRDT
sync set (see schema.py MIGRATION_V5), because claims are transient
machine-local state; the upcoming team collab server owns the cross-machine
story.

Conventions match commits.py: writes take a TrackerDB (they own the
transaction), reads take a sqlite3.Connection (they compose inside one).
Claim acquisition is atomic via BEGIN IMMEDIATE, the partial unique index
idx_claims_active is the backstop invariant.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso
from devtools_mcp.tracker.models import FileClaim, FileTouch
from devtools_mcp.tracker.tasks import TASK_KEY_RE

FILES_PER_CALL_MAX: int = 50
FILE_PATH_MAX: int = 512
SESSION_ID_MAX: int = 128
CLAIM_TTL_DEFAULT_S: int = 900  # 15-minute lease
CLAIM_TTL_MAX_S: int = 8 * 3600
RECENT_TOUCH_WINDOW_S: int = 600  # "someone else was just here" horizon
DEBOUNCE_WINDOW_S: int = 60  # same session+file+op inside this = UPDATE, not INSERT
ACTIVITY_MAX_PER_REPO: int = 5000  # pruned on write
CONFLICTS_MAX: int = 20
LIST_MAX: int = 1000
REPO_WALK_MAX: int = 32  # bounded .git ancestor walk


class ClaimHeldError(TrackerError):
    """Another session holds an active claim on the file."""

    def __init__(self, holder: FileClaim) -> None:
        assert holder.released_at is None, "holder claim is not active"
        self.holder = holder
        who = holder.agent_label or holder.session_id
        task = f" (task {holder.task_key})" if holder.task_key else ""
        super().__init__(f"{holder.file_path} is claimed by {who}{task} until {holder.expires_at}")


def _iso_in(seconds: int) -> str:
    """UTC ISO-8601 timestamp `seconds` from now (lexicographically comparable)."""
    assert 0 <= seconds <= CLAIM_TTL_MAX_S, f"ttl out of bounds: {seconds}"
    stamp = (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat()
    assert stamp.endswith("+00:00"), f"expected UTC timestamp, got {stamp!r}"
    return stamp


def _iso_ago(seconds: int) -> str:
    """UTC ISO-8601 timestamp `seconds` in the past."""
    assert seconds >= 0, f"negative window: {seconds}"
    stamp = (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()
    assert stamp.endswith("+00:00"), f"expected UTC timestamp, got {stamp!r}"
    return stamp


def normalize(cwd: str, path: str) -> tuple[str, str]:
    """Resolve `path` (absolute or cwd-relative) to (repo_root, repo-relative posix path).

    The repo root is the nearest ancestor containing `.git` (bounded walk);
    without one, the cwd is the root. Single chokepoint for Windows/POSIX
    separator and casing normalization (columns are COLLATE NOCASE on top).
    """
    assert cwd, "empty cwd"
    assert path, "empty path"
    base = Path(cwd)
    abs_path = (base / path).resolve() if not Path(path).is_absolute() else Path(path).resolve()
    root = abs_path if abs_path.is_dir() else abs_path.parent
    found = base.resolve()
    for _ in range(REPO_WALK_MAX):  # bounded
        if (root / ".git").exists():
            found = root
            break
        if root.parent == root:
            break
        root = root.parent
    try:
        rel_posix = PurePosixPath(abs_path.relative_to(found)).as_posix()[:FILE_PATH_MAX]
    except ValueError:
        rel_posix = abs_path.name[:FILE_PATH_MAX]
    repo_root = found.as_posix()
    assert repo_root, "normalize produced empty repo root"
    assert rel_posix, "normalize produced empty relative path"
    return repo_root, rel_posix


def _validate_identity(session_id: str, task_key: str | None) -> str | None:
    """Validate session/task inputs shared by every write. Returns clean task_key."""
    if not session_id or not session_id.strip():
        raise TrackerError("session_id must not be empty")
    if len(session_id) > SESSION_ID_MAX:
        raise TrackerError(f"session_id longer than {SESSION_ID_MAX} chars")
    if task_key is not None:
        task_key = task_key.strip().upper()
        if not TASK_KEY_RE.match(task_key):
            raise TrackerError(f"Bad task key {task_key!r}: expected PROJ-123")
    return task_key


def record_touches(
    db: TrackerDB,
    session_id: str,
    cwd: str,
    files: list[str],
    *,
    agent_label: str = "",
    task_key: str | None = None,
    tool_name: str = "",
    op: str = "edit",
) -> int:
    """Record file touches for a session. Returns rows written (new or refreshed).

    Debounced: a touch by the same session+file+op inside DEBOUNCE_WINDOW_S
    refreshes the existing row's ts instead of inserting. Touching a file also
    renews this session's own active claim on it (implicit heartbeat). Activity
    per repo is pruned to ACTIVITY_MAX_PER_REPO.
    """
    task_key = _validate_identity(session_id, task_key)
    if op not in ("edit", "write", "read"):
        raise TrackerError(f"Bad op {op!r}: expected edit|write|read")
    if not files:
        raise TrackerError("files must not be empty")
    if len(files) > FILES_PER_CALL_MAX:
        raise TrackerError(f"too many files in one call: {len(files)} > {FILES_PER_CALL_MAX}")
    now = utc_now_iso()
    debounce_floor = _iso_ago(DEBOUNCE_WINDOW_S)
    written = 0
    touched_repos: set[str] = set()
    with db.transaction() as conn:
        for raw in files:  # bounded by FILES_PER_CALL_MAX
            repo_root, rel = normalize(cwd, raw)
            touched_repos.add(repo_root)
            cursor = conn.execute(
                "UPDATE file_activity SET ts = ?, task_key = COALESCE(?, task_key), "
                "agent_label = CASE WHEN ? != '' THEN ? ELSE agent_label END "
                "WHERE session_id = ? AND repo_root = ? AND file_path = ? AND op = ? AND ts >= ?",
                (now, task_key, agent_label, agent_label, session_id, repo_root, rel, op, debounce_floor),
            )
            if cursor.rowcount == 0:
                conn.execute(
                    "INSERT INTO file_activity "
                    "(session_id, agent_label, task_key, repo_root, file_path, op, tool_name, ts) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (session_id, agent_label, task_key, repo_root, rel, op, tool_name, now),
                )
            written += 1
            conn.execute(  # implicit heartbeat on own active claim
                "UPDATE file_claims SET expires_at = ? "
                "WHERE session_id = ? AND repo_root = ? AND file_path = ? "
                "AND released_at IS NULL AND expires_at > ?",
                (_iso_in(CLAIM_TTL_DEFAULT_S), session_id, repo_root, rel, now),
            )
        for repo_root in touched_repos:  # bounded by FILES_PER_CALL_MAX
            conn.execute(  # prune: keep the newest ACTIVITY_MAX_PER_REPO rows per repo
                "DELETE FROM file_activity WHERE repo_root = ? AND id IN ("
                "SELECT id FROM file_activity WHERE repo_root = ? "
                "ORDER BY id DESC LIMIT -1 OFFSET ?)",
                (repo_root, repo_root, ACTIVITY_MAX_PER_REPO),
            )
    assert 0 < written <= FILES_PER_CALL_MAX, f"wrote {written} touches"
    return written


def acquire_claim(
    db: TrackerDB,
    session_id: str,
    cwd: str,
    file: str,
    *,
    agent_label: str = "",
    task_key: str | None = None,
    ttl_s: int = CLAIM_TTL_DEFAULT_S,
) -> FileClaim:
    """Acquire (or renew) an advisory lease on a file.

    Atomic under BEGIN IMMEDIATE: expired claims on the path are reaped first;
    an unexpired claim by another session raises ClaimHeldError carrying the
    holder; our own claim is renewed. Raises TrackerError on bad input.
    """
    task_key = _validate_identity(session_id, task_key)
    if not 0 < ttl_s <= CLAIM_TTL_MAX_S:
        raise TrackerError(f"ttl_s out of range: {ttl_s} (max {CLAIM_TTL_MAX_S})")
    repo_root, rel = normalize(cwd, file)
    now = utc_now_iso()
    expires = _iso_in(ttl_s)
    with db.transaction() as conn:
        conn.execute(  # reap expired leases on this path
            "UPDATE file_claims SET released_at = ? "
            "WHERE repo_root = ? AND file_path = ? AND released_at IS NULL AND expires_at <= ?",
            (now, repo_root, rel, now),
        )
        row = conn.execute(
            "SELECT * FROM file_claims WHERE repo_root = ? AND file_path = ? AND released_at IS NULL",
            (repo_root, rel),
        ).fetchone()
        if row is not None:
            existing = FileClaim.from_row(row)
            if existing.session_id != session_id:
                raise ClaimHeldError(existing)
            conn.execute(  # renew own lease
                "UPDATE file_claims SET expires_at = ?, task_key = COALESCE(?, task_key) WHERE id = ?",
                (expires, task_key, existing.id),
            )
            claim_id = existing.id
        else:
            cursor = conn.execute(
                "INSERT INTO file_claims "
                "(session_id, agent_label, task_key, repo_root, file_path, claimed_at, expires_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (session_id, agent_label, task_key, repo_root, rel, now, expires),
            )
            assert cursor.lastrowid is not None, "insert produced no rowid"
            claim_id = cursor.lastrowid
        fresh = conn.execute("SELECT * FROM file_claims WHERE id = ?", (claim_id,)).fetchone()
    claim = FileClaim.from_row(fresh)
    assert claim.session_id == session_id, "claim ownership invariant broken"
    assert claim.released_at is None, "acquired claim is not active"
    return claim


def release_claims(
    db: TrackerDB,
    session_id: str,
    *,
    repo_root: str | None = None,
    file_path: str | None = None,
) -> int:
    """Release this session's active claims (all, per repo, or one file). Returns count."""
    if not session_id or not session_id.strip():
        raise TrackerError("session_id must not be empty")
    if file_path is not None and repo_root is None:
        raise TrackerError("file_path filter requires repo_root")
    sql = "UPDATE file_claims SET released_at = ? WHERE session_id = ? AND released_at IS NULL"
    params: list[str] = [utc_now_iso(), session_id]
    if repo_root is not None:
        sql += " AND repo_root = ?"
        params.append(repo_root)
    if file_path is not None:
        sql += " AND file_path = ?"
        params.append(file_path)
    with db.transaction() as conn:
        released = conn.execute(sql, params).rowcount
    assert released >= 0, f"negative rowcount {released}"
    return released


def conflicts_for(
    conn: sqlite3.Connection,
    session_id: str,
    repo_root: str,
    file_path: str,
) -> list[dict]:
    """Who ELSE is on this file: active claims plus recent touches by other sessions."""
    assert session_id, "empty session id"
    assert repo_root and file_path, "empty repo/path"
    now = utc_now_iso()
    out: list[dict] = []
    claim_rows = conn.execute(
        "SELECT * FROM file_claims WHERE repo_root = ? AND file_path = ? "
        "AND released_at IS NULL AND expires_at > ? AND session_id != ? LIMIT ?",
        (repo_root, file_path, now, session_id, CONFLICTS_MAX),
    ).fetchall()
    for row in claim_rows:  # bounded
        claim = FileClaim.from_row(row)
        out.append(
            {
                "kind": "claim",
                "file": claim.file_path,
                "session_id": claim.session_id,
                "agent": claim.agent_label,
                "task_key": claim.task_key,
                "expires_at": claim.expires_at,
            }
        )
    touch_rows = conn.execute(
        "SELECT * FROM file_activity WHERE repo_root = ? AND file_path = ? "
        "AND ts > ? AND session_id != ? ORDER BY ts DESC LIMIT ?",
        (repo_root, file_path, _iso_ago(RECENT_TOUCH_WINDOW_S), session_id, CONFLICTS_MAX),
    ).fetchall()
    seen_sessions = {c["session_id"] for c in out}
    for row in touch_rows:  # bounded
        touch = FileTouch.from_row(row)
        if touch.session_id in seen_sessions:
            continue
        seen_sessions.add(touch.session_id)
        out.append(
            {
                "kind": "recent_touch",
                "file": touch.file_path,
                "session_id": touch.session_id,
                "agent": touch.agent_label,
                "task_key": touch.task_key,
                "ts": touch.ts,
            }
        )
    assert len(out) <= 2 * CONFLICTS_MAX, "conflict list exceeded bound"
    return out


def active_claims(conn: sqlite3.Connection, repo_root: str | None = None) -> list[FileClaim]:
    """All unexpired active claims, optionally scoped to one repo."""
    now = utc_now_iso()
    sql = "SELECT * FROM file_claims WHERE released_at IS NULL AND expires_at > ?"
    params: list[str] = [now]
    if repo_root:
        sql += " AND repo_root = ?"
        params.append(repo_root)
    sql += " ORDER BY expires_at LIMIT ?"
    rows = conn.execute(sql, (*params, LIST_MAX)).fetchall()
    claims = [FileClaim.from_row(row) for row in rows]
    assert len(claims) <= LIST_MAX, "claim list exceeded bound"
    return claims


def recent_activity(conn: sqlite3.Connection, repo_root: str | None = None, limit: int = 100) -> list[FileTouch]:
    """Most recent touches, newest first, optionally scoped to one repo."""
    assert 0 < limit <= LIST_MAX, f"limit out of bounds: {limit}"
    sql = "SELECT * FROM file_activity"
    params: list[str] = []
    if repo_root:
        sql += " WHERE repo_root = ?"
        params.append(repo_root)
    sql += " ORDER BY ts DESC LIMIT ?"
    rows = conn.execute(sql, (*params, limit)).fetchall()
    touches = [FileTouch.from_row(row) for row in rows]
    assert len(touches) <= limit, "activity list exceeded bound"
    return touches


def sessions_overview(conn: sqlite3.Connection) -> list[dict]:
    """Distinct sessions with label, last-seen, touch count and active-claim count."""
    now = utc_now_iso()
    rows = conn.execute(
        "SELECT a.session_id, "
        "MAX(a.agent_label) AS agent_label, "
        "MAX(a.ts) AS last_seen, "
        "COUNT(*) AS touches, "
        "(SELECT COUNT(*) FROM file_claims c WHERE c.session_id = a.session_id "
        " AND c.released_at IS NULL AND c.expires_at > ?) AS claims "
        "FROM file_activity a GROUP BY a.session_id ORDER BY last_seen DESC LIMIT ?",
        (now, LIST_MAX),
    ).fetchall()
    out = [dict(row) for row in rows]
    assert len(out) <= LIST_MAX, "session list exceeded bound"
    return out
