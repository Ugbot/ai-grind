"""CRDT layer for the tracker: hybrid logical clocks, op-log, LWW merge.

The local-first model, v1:

- Every replica (site) has a random `site_id` and a hybrid logical clock (HLC).
- SQLite triggers (schema v3) capture every mutation of the synced tables into
  `crdt_ops` as a row-level op: (hlc, site_id, tbl, pk, upsert|delete, payload).
  Payloads are **site-independent** — rows are keyed by `uid` (random 128-bit)
  and references use uids / natural keys, never local rowids.
- Merge is last-writer-wins per row, ordered by HLC (which is lexicographically
  sortable and globally unique via the site suffix). Ops are stored verbatim so
  they propagate transitively through any sync topology.
- Human task keys (PROJ-123) can collide when two sites allocate concurrently;
  the merge re-keys deterministically (the lexically smaller uid keeps the key)
  so all replicas converge to the same state.

Synced tables: projects, tasks, acceptance_criteria, task_deps, task_tags,
task_commits. Site-local for now: tag_rules, external_refs, schema/crdt meta.

This is a deliberate *start*: row-level LWW (a field edit overwrites the whole
row on conflict). The upgrade path is field-level LWW with the same op-log.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid

from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso

SYNC_TABLES: tuple[str, ...] = (
    "projects",
    "tasks",
    "acceptance_criteria",
    "task_deps",
    "task_tags",
    "task_commits",
)
MAX_OPS_PER_BATCH: int = 100_000
MAX_MERGE_PASSES: int = 10


class HLC:
    """Hybrid logical clock issuing lexicographically sortable, unique stamps.

    Format: `{unix_ms:013d}-{counter:05d}-{site8}` — physical time first, a
    counter for same-millisecond ordering, the site id for global uniqueness.
    """

    def __init__(self, site_id: str) -> None:
        assert len(site_id) >= 8, f"site_id too short: {site_id!r}"
        self._site = site_id[:8]
        self._last_ms = 0
        self._counter = 0
        self._lock = threading.Lock()

    def next_str(self) -> str:
        """Issue the next stamp (monotonic even if the wall clock stalls)."""
        with self._lock:
            now_ms = int(time.time() * 1000)
            if now_ms > self._last_ms:
                self._last_ms, self._counter = now_ms, 0
            else:
                self._counter += 1
                assert self._counter < 99_999, "HLC counter overflow in one ms"
            stamp = f"{self._last_ms:013d}-{self._counter:05d}-{self._site}"
        assert len(stamp) == 13 + 1 + 5 + 1 + 8, f"malformed stamp {stamp!r}"
        return stamp

    def observe(self, remote: str) -> None:
        """Advance past a remote stamp so our next stamp sorts after it."""
        assert isinstance(remote, str), "remote stamp must be str"
        try:
            ms = int(remote[:13])
            counter = int(remote[14:19])
        except ValueError:
            return  # malformed remote stamp: ignore, never crash sync
        with self._lock:
            if ms > self._last_ms or (ms == self._last_ms and counter > self._counter):
                self._last_ms, self._counter = ms, counter


def new_site_id() -> str:
    """Random 32-hex site identity."""
    site = uuid.uuid4().hex
    assert len(site) == 32, "uuid4 hex must be 32 chars"
    return site


def ensure_identity(conn: sqlite3.Connection) -> str:
    """Make sure crdt_site / crdt_state rows exist; returns the site_id."""
    assert conn is not None, "ensure_identity on missing connection"
    conn.execute("INSERT OR IGNORE INTO crdt_site (id, site_id) VALUES (1, ?)", (new_site_id(),))
    conn.execute("INSERT OR IGNORE INTO crdt_state (key, value) VALUES ('applying', '0')")
    site = conn.execute("SELECT site_id FROM crdt_site WHERE id = 1").fetchone()[0]
    assert site, "crdt_site row missing after ensure"
    return site


def site_id(db: TrackerDB) -> str:
    """This replica's site id."""
    row = db.conn.execute("SELECT site_id FROM crdt_site WHERE id = 1").fetchone()
    assert row is not None, "tracker db has no crdt identity"
    return row[0]


def ops_after(conn: sqlite3.Connection, after_hlc: str | None = None) -> list[dict]:
    """All ops with hlc strictly greater than `after_hlc` (None = everything)."""
    if after_hlc is None:
        rows = conn.execute(
            "SELECT hlc, site_id, tbl, pk, op, payload FROM crdt_ops ORDER BY hlc LIMIT ?",
            (MAX_OPS_PER_BATCH,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT hlc, site_id, tbl, pk, op, payload FROM crdt_ops WHERE hlc > ? " "ORDER BY hlc LIMIT ?",
            (after_hlc, MAX_OPS_PER_BATCH),
        ).fetchall()
    assert len(rows) <= MAX_OPS_PER_BATCH, "ops query over bound"
    return [dict(row) for row in rows]


def latest_hlc(conn: sqlite3.Connection) -> str | None:
    """Highest stamp in the local op-log (the sync watermark)."""
    row = conn.execute("SELECT MAX(hlc) FROM crdt_ops").fetchone()
    assert row is not None, "MAX query returned no row"
    return row[0]


# --- merge ------------------------------------------------------------------


def merge_ops(db: TrackerDB, ops: list[dict]) -> dict[str, int]:
    """Merge remote ops into this replica (LWW by HLC). Returns counters.

    Idempotent: ops already seen (same hlc+site) are skipped. Applied ops are
    stored verbatim so they keep propagating. Trigger capture is suppressed for
    the duration so merged rows don't echo as new local ops.
    """
    if len(ops) > MAX_OPS_PER_BATCH:
        raise TrackerError(f"op batch too large: {len(ops)} > {MAX_OPS_PER_BATCH}")
    counters = {"received": len(ops), "new": 0, "applied": 0, "stale": 0, "deferred": 0}
    if not ops:
        return counters
    incoming = sorted(ops, key=lambda o: o["hlc"])
    with db.transaction() as conn:
        conn.execute("UPDATE crdt_state SET value = '1' WHERE key = 'applying'")
        try:
            fresh = _record_incoming(conn, incoming, counters)
            pending = [op for op in fresh if _is_winner(conn, op)]
            counters["stale"] += len(fresh) - len(pending)
            for _ in range(MAX_MERGE_PASSES):  # bounded: deferred ops shrink each pass
                still = [op for op in pending if not _apply_op(conn, op)]
                counters["applied"] += len(pending) - len(still)
                if not still or len(still) == len(pending):
                    pending = still
                    break
                pending = still
            counters["deferred"] = len(pending)
            _recompute_depths(conn)
        finally:
            conn.execute("UPDATE crdt_state SET value = '0' WHERE key = 'applying'")
    for op in incoming:
        db.hlc.observe(op["hlc"])
    assert (
        counters["applied"] + counters["stale"] + counters["deferred"] == counters["new"]
    ), "merge counters do not add up"
    return counters


def _record_incoming(conn: sqlite3.Connection, ops: list[dict], counters: dict) -> list[dict]:
    """Store unseen ops verbatim; returns only the newly recorded ones."""
    fresh: list[dict] = []
    for op in ops:  # bounded by MAX_OPS_PER_BATCH
        cursor = conn.execute(
            "INSERT OR IGNORE INTO crdt_ops (hlc, site_id, tbl, pk, op, payload) " "VALUES (?, ?, ?, ?, ?, ?)",
            (op["hlc"], op["site_id"], op["tbl"], op["pk"], op["op"], op.get("payload")),
        )
        if cursor.rowcount == 1:
            counters["new"] += 1
            fresh.append(op)
    assert counters["new"] == len(fresh), "fresh op count mismatch"
    return fresh


def _is_winner(conn: sqlite3.Connection, op: dict) -> bool:
    """LWW: the op only applies if no op for the same row has a higher HLC."""
    row = conn.execute("SELECT MAX(hlc) FROM crdt_ops WHERE tbl = ? AND pk = ?", (op["tbl"], op["pk"])).fetchone()
    newest = row[0]
    assert newest is not None, "winner check ran before op was recorded"
    return op["hlc"] >= newest


def _apply_op(conn: sqlite3.Connection, op: dict) -> bool:
    """Apply one winning op. Returns False if it must be deferred (missing ref)."""
    appliers = {
        "projects": _apply_project,
        "tasks": _apply_task,
        "acceptance_criteria": _apply_criterion,
        "task_deps": _apply_dep,
        "task_tags": _apply_task_tag,
        "task_commits": _apply_commit,
    }
    applier = appliers.get(op["tbl"])
    if applier is None:
        return True  # unknown table: recorded for propagation, nothing to apply
    payload = json.loads(op["payload"]) if op.get("payload") else None
    return applier(conn, op, payload)


def _task_id_by_uid(conn: sqlite3.Connection, uid: str | None) -> int | None:
    if not uid:
        return None
    row = conn.execute("SELECT id FROM tasks WHERE uid = ?", (uid,)).fetchone()
    return row[0] if row else None


def _apply_project(conn: sqlite3.Connection, op: dict, payload: dict | None) -> bool:
    if op["op"] == "delete":
        conn.execute("DELETE FROM projects WHERE key = ?", (op["pk"],))
        return True
    assert payload is not None, "project upsert without payload"
    existing = conn.execute("SELECT next_seq FROM projects WHERE key = ?", (payload["key"],)).fetchone()
    # next_seq merges as MAX so neither replica re-issues a key the other used.
    next_seq = max(payload["next_seq"], existing[0]) if existing else payload["next_seq"]
    conn.execute(
        "INSERT INTO projects (key, name, description, close_policy, next_seq, created_at) "
        "VALUES (:key, :name, :description, :close_policy, :next_seq, :created_at) "
        "ON CONFLICT(key) DO UPDATE SET name = :name, description = :description, "
        "close_policy = :close_policy, next_seq = :next_seq",
        {**payload, "next_seq": next_seq},
    )
    return True


def _apply_task(conn: sqlite3.Connection, op: dict, payload: dict | None) -> bool:
    if op["op"] == "delete":
        conn.execute("DELETE FROM tasks WHERE uid = ?", (op["pk"],))
        return True
    assert payload is not None, "task upsert without payload"
    project = conn.execute("SELECT id FROM projects WHERE key = ?", (payload["project"],)).fetchone()
    if project is None:
        return False  # project op not applied yet — defer
    parent_id = _task_id_by_uid(conn, payload.get("parent_uid"))
    if payload.get("parent_uid") and parent_id is None:
        return False  # parent not landed yet — defer
    key = _resolve_key_collision(conn, payload["uid"], payload["key"], project[0])
    conn.execute(
        "INSERT INTO tasks (uid, project_id, key, parent_id, depth, kind, title, description, "
        "status, priority, sort_order, created_at, updated_at, closed_at) "
        "VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(uid) DO UPDATE SET project_id = excluded.project_id, key = excluded.key, "
        "parent_id = excluded.parent_id, kind = excluded.kind, title = excluded.title, "
        "description = excluded.description, status = excluded.status, "
        "priority = excluded.priority, sort_order = excluded.sort_order, "
        "updated_at = excluded.updated_at, closed_at = excluded.closed_at",
        (
            payload["uid"],
            project[0],
            key,
            parent_id,
            payload["kind"],
            payload["title"],
            payload["description"],
            payload["status"],
            payload["priority"],
            payload["sort_order"],
            payload["created_at"],
            payload["updated_at"],
            payload.get("closed_at"),
        ),
    )
    return True


def _resolve_key_collision(conn: sqlite3.Connection, uid: str, key: str, project_id: int) -> str:
    """Deterministic re-keying: the lexically smaller uid keeps a contested key."""
    holder = conn.execute("SELECT uid FROM tasks WHERE key = ?", (key,)).fetchone()
    if holder is None or holder[0] == uid:
        return key
    if uid < holder[0]:  # incoming wins the key; re-key the local holder
        new_key = _allocate_key(conn, project_id)
        conn.execute("UPDATE tasks SET key = ? WHERE uid = ?", (new_key, holder[0]))
        return key
    return _allocate_key(conn, project_id)  # incoming loses; gets a fresh key


def _allocate_key(conn: sqlite3.Connection, project_id: int) -> str:
    row = conn.execute(
        "UPDATE projects SET next_seq = next_seq + 1 WHERE id = ? RETURNING key, next_seq - 1",
        (project_id,),
    ).fetchone()
    assert row is not None, f"project {project_id} vanished during re-key"
    new_key = f"{row[0]}-{row[1]}"
    assert "-" in new_key, "malformed reallocated key"
    return new_key


def _apply_criterion(conn: sqlite3.Connection, op: dict, payload: dict | None) -> bool:
    if op["op"] == "delete":
        conn.execute("DELETE FROM acceptance_criteria WHERE uid = ?", (op["pk"],))
        return True
    assert payload is not None, "criterion upsert without payload"
    task_id = _task_id_by_uid(conn, payload["task_uid"])
    if task_id is None:
        return False
    conn.execute(
        "INSERT INTO acceptance_criteria (uid, task_id, text, test_ref, last_result, "
        "last_run_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(uid) DO UPDATE SET task_id = excluded.task_id, text = excluded.text, "
        "test_ref = excluded.test_ref, last_result = excluded.last_result, "
        "last_run_at = excluded.last_run_at",
        (
            payload["uid"],
            task_id,
            payload["text"],
            payload.get("test_ref"),
            payload.get("last_result"),
            payload.get("last_run_at"),
            payload["created_at"],
        ),
    )
    return True


def _apply_dep(conn: sqlite3.Connection, op: dict, payload: dict | None) -> bool:
    if op["op"] == "delete":
        task_uid, _, dep_uid = op["pk"].partition(":")
        conn.execute(
            "DELETE FROM task_deps WHERE task_id = (SELECT id FROM tasks WHERE uid = ?) "
            "AND depends_on_id = (SELECT id FROM tasks WHERE uid = ?)",
            (task_uid, dep_uid),
        )
        return True
    assert payload is not None, "dep upsert without payload"
    task_id = _task_id_by_uid(conn, payload["task_uid"])
    dep_id = _task_id_by_uid(conn, payload["depends_on_uid"])
    if task_id is None or dep_id is None:
        return False
    conn.execute(
        "INSERT OR IGNORE INTO task_deps (task_id, depends_on_id, created_at) VALUES (?, ?, ?)",
        (task_id, dep_id, payload["created_at"]),
    )
    return True


def _apply_task_tag(conn: sqlite3.Connection, op: dict, payload: dict | None) -> bool:
    if op["op"] == "delete":
        task_uid, _, tag_name = op["pk"].partition(":")
        conn.execute(
            "DELETE FROM task_tags WHERE task_id = (SELECT id FROM tasks WHERE uid = ?) "
            "AND tag_id = (SELECT id FROM tags WHERE name = ?)",
            (task_uid, tag_name),
        )
        return True
    assert payload is not None, "task_tag upsert without payload"
    task_id = _task_id_by_uid(conn, payload["task_uid"])
    if task_id is None:
        return False
    conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (payload["tag"],))
    conn.execute(
        "INSERT OR IGNORE INTO task_tags (task_id, tag_id) " "VALUES (?, (SELECT id FROM tags WHERE name = ?))",
        (task_id, payload["tag"]),
    )
    return True


def _apply_commit(conn: sqlite3.Connection, op: dict, payload: dict | None) -> bool:
    if op["op"] == "delete":
        return True  # commit links are add-only in v1
    assert payload is not None, "commit upsert without payload"
    task_id = _task_id_by_uid(conn, payload["task_uid"])
    if task_id is None:
        return False
    conn.execute(
        "INSERT OR IGNORE INTO task_commits (task_id, commit_hash, repo_path, "
        "message_snippet, linked_at) VALUES (?, ?, ?, ?, ?)",
        (
            task_id,
            payload["commit_hash"],
            payload["repo_path"],
            payload.get("message_snippet", ""),
            payload["linked_at"],
        ),
    )
    return True


def _recompute_depths(conn: sqlite3.Connection) -> None:
    """Rebase every task's depth from its parent chain (after structural merges)."""
    conn.execute(
        "WITH RECURSIVE d(id, depth) AS ("
        "  SELECT id, 0 FROM tasks WHERE parent_id IS NULL"
        "  UNION ALL"
        "  SELECT t.id, d.depth + 1 FROM tasks t JOIN d ON t.parent_id = d.id"
        "  WHERE d.depth < 5"
        ") UPDATE tasks SET depth = (SELECT depth FROM d WHERE d.id = tasks.id) "
        "WHERE id IN (SELECT id FROM d)"
    )


# --- canonical state (testing / status) --------------------------------------


def canonical_state(conn: sqlite3.Connection) -> dict:
    """Site-independent snapshot of all synced data, for convergence checks."""
    state: dict = {}
    state["projects"] = sorted(
        tuple(row) for row in conn.execute("SELECT key, name, description, close_policy FROM projects").fetchall()
    )
    state["tasks"] = sorted(
        tuple(row)
        for row in conn.execute(
            "SELECT t.uid, t.key, p.key, parent.uid, t.kind, t.title, t.description, "
            "t.status, t.priority FROM tasks t JOIN projects p ON p.id = t.project_id "
            "LEFT JOIN tasks parent ON parent.id = t.parent_id"
        ).fetchall()
    )
    state["criteria"] = sorted(
        tuple(row)
        for row in conn.execute(
            "SELECT ac.uid, t.uid, ac.text, ac.test_ref, ac.last_result "
            "FROM acceptance_criteria ac JOIN tasks t ON t.id = ac.task_id"
        ).fetchall()
    )
    state["deps"] = sorted(
        tuple(row)
        for row in conn.execute(
            "SELECT a.uid, b.uid FROM task_deps d JOIN tasks a ON a.id = d.task_id "
            "JOIN tasks b ON b.id = d.depends_on_id"
        ).fetchall()
    )
    state["tags"] = sorted(
        tuple(row)
        for row in conn.execute(
            "SELECT t.uid, tg.name FROM task_tags tt JOIN tasks t ON t.id = tt.task_id "
            "JOIN tags tg ON tg.id = tt.tag_id"
        ).fetchall()
    )
    state["commits"] = sorted(
        tuple(row)
        for row in conn.execute(
            "SELECT t.uid, tc.commit_hash, tc.repo_path FROM task_commits tc " "JOIN tasks t ON t.id = tc.task_id"
        ).fetchall()
    )
    assert set(state) == {
        "projects",
        "tasks",
        "criteria",
        "deps",
        "tags",
        "commits",
    }, "canonical state missing a section"
    return state


def status(db: TrackerDB) -> dict:
    """Replica status: site id, op counts, watermark, peers."""
    ops = db.conn.execute("SELECT COUNT(*) FROM crdt_ops").fetchone()[0]
    peers = [dict(row) for row in db.conn.execute("SELECT * FROM crdt_peers").fetchall()]
    return {
        "site_id": site_id(db),
        "ops": ops,
        "latest_hlc": latest_hlc(db.conn),
        "peers": peers,
        "checked_at": utc_now_iso(),
    }
