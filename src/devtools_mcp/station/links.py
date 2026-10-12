"""Row-level identity map between local records and platform rows.

Human keys are never identity: local GRIND-19 and platform GRIND-19 are
unrelated strings. station_links is the only join. `synced_hash` is the
echo-suppression mechanism, after a push we store the hash of what we
sent, after a pull-apply the hash of what we applied; a row whose current
hash equals the link hash is a no-op in either direction.

Task creates use a two-phase intent: the link row is committed with
remote_id='pending:<uuid>' BEFORE the POST, so a crash between the 201 and
the link update leaves a resolvable pending row instead of a duplicate.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid

from devtools_mcp.tracker.db import TrackerDB, utc_now_iso

LINK_DOMAINS: tuple[str, ...] = (
    "task",
    "criterion",
    "dep",
    "commit",
    "session",
    "handoff",
    "claim",
    "skill",
    "perf_run",
)
PENDING_PREFIX: str = "pending:"
LINKS_MAX: int = 100_000


def canonical_hash(fields: dict) -> str:
    """sha256 over canonical JSON of exactly the synced field set."""
    assert isinstance(fields, dict) and fields, "canonical_hash needs a non-empty dict"
    blob = json.dumps(fields, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.sha256(blob.encode("utf-8")).hexdigest()
    assert len(digest) == 64, "sha256 hexdigest must be 64 chars"
    return digest


def get_link(conn: sqlite3.Connection, domain: str, local_id: str) -> sqlite3.Row | None:
    assert domain in LINK_DOMAINS, f"unknown link domain {domain!r}"
    assert local_id, "local_id must be non-empty"
    return conn.execute("SELECT * FROM station_links WHERE domain = ? AND local_id = ?", (domain, local_id)).fetchone()


def link_by_remote(conn: sqlite3.Connection, domain: str, org_id: str, remote_id: str) -> sqlite3.Row | None:
    assert domain in LINK_DOMAINS, f"unknown link domain {domain!r}"
    assert remote_id, "remote_id must be non-empty"
    return conn.execute(
        "SELECT * FROM station_links WHERE domain = ? AND org_id = ? AND remote_id = ?",
        (domain, org_id, remote_id),
    ).fetchone()


def links_for_domain(conn: sqlite3.Connection, domain: str, org_id: str) -> list[sqlite3.Row]:
    assert domain in LINK_DOMAINS, f"unknown link domain {domain!r}"
    rows = conn.execute(
        "SELECT * FROM station_links WHERE domain = ? AND org_id = ? LIMIT ?",
        (domain, org_id, LINKS_MAX),
    ).fetchall()
    assert len(rows) <= LINKS_MAX, "links query over bound"
    return rows


def put_pending(db: TrackerDB, domain: str, local_id: str, org_id: str) -> str:
    """Commit a pending-intent link BEFORE the create POST. Returns the token."""
    assert domain in LINK_DOMAINS, f"unknown link domain {domain!r}"
    assert local_id and org_id, "local_id and org_id must be non-empty"
    token = PENDING_PREFIX + uuid.uuid4().hex
    now = utc_now_iso()
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO station_links (domain, local_id, remote_id, org_id, state, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, 'pending', ?, ?) "
            "ON CONFLICT(domain, local_id) DO UPDATE SET remote_id = excluded.remote_id, "
            "state = 'pending', updated_at = excluded.updated_at",
            (domain, local_id, token, org_id, now, now),
        )
    assert token.startswith(PENDING_PREFIX), "pending token malformed"
    return token


def resolve_link(
    db: TrackerDB,
    domain: str,
    local_id: str,
    remote_id: str,
    remote_key: str | None,
    synced_hash: str | None,
) -> None:
    """Resolve an existing (usually pending) link to a real platform id + hash."""
    assert domain in LINK_DOMAINS, f"unknown link domain {domain!r}"
    assert remote_id and not remote_id.startswith(PENDING_PREFIX), f"resolve needs a real id, got {remote_id!r}"
    with db.transaction() as conn:
        cursor = conn.execute(
            "UPDATE station_links SET remote_id = ?, remote_key = ?, synced_hash = ?, "
            "state = 'ok', last_error = NULL, updated_at = ? WHERE domain = ? AND local_id = ?",
            (remote_id, remote_key, synced_hash, utc_now_iso(), domain, local_id),
        )
        assert cursor.rowcount == 1, f"resolve_link on missing link {domain}:{local_id}"


def insert_link(
    db: TrackerDB,
    domain: str,
    local_id: str,
    remote_id: str,
    org_id: str,
    remote_key: str | None,
    synced_hash: str | None,
) -> None:
    """Insert (or replace) a resolved link, used by pull-applies."""
    assert domain in LINK_DOMAINS, f"unknown link domain {domain!r}"
    assert local_id and remote_id and org_id, "link ids must be non-empty"
    now = utc_now_iso()
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO station_links (domain, local_id, remote_id, org_id, remote_key, "
            "synced_hash, state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 'ok', ?, ?) "
            "ON CONFLICT(domain, local_id) DO UPDATE SET remote_id = excluded.remote_id, "
            "remote_key = excluded.remote_key, synced_hash = excluded.synced_hash, "
            "state = 'ok', last_error = NULL, updated_at = excluded.updated_at",
            (domain, local_id, remote_id, org_id, remote_key, synced_hash, now, now),
        )


def update_hash(db: TrackerDB, domain: str, local_id: str, synced_hash: str) -> None:
    assert domain in LINK_DOMAINS, f"unknown link domain {domain!r}"
    assert len(synced_hash) == 64, f"bad hash {synced_hash!r}"
    with db.transaction() as conn:
        cursor = conn.execute(
            "UPDATE station_links SET synced_hash = ?, state = 'ok', last_error = NULL, "
            "updated_at = ? WHERE domain = ? AND local_id = ?",
            (synced_hash, utc_now_iso(), domain, local_id),
        )
        assert cursor.rowcount == 1, f"update_hash on missing link {domain}:{local_id}"


def mark_error(db: TrackerDB, domain: str, local_id: str, org_id: str, error: str, attempted_hash: str | None) -> None:
    """Quarantine a row: 4xx from the platform. Skipped until its hash changes.

    The attempted hash is stored so re-diffs skip the row while its content
    is unchanged (no hammering) but retry as soon as the user edits it.
    """
    assert domain in LINK_DOMAINS, f"unknown link domain {domain!r}"
    assert error and org_id, "mark_error needs an error message and org_id"
    now = utc_now_iso()
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO station_links (domain, local_id, remote_id, org_id, synced_hash, state, "
            "last_error, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, 'error', ?, ?, ?) "
            "ON CONFLICT(domain, local_id) DO UPDATE SET state = 'error', "
            "last_error = excluded.last_error, synced_hash = excluded.synced_hash, "
            "updated_at = excluded.updated_at",
            (domain, local_id, "error:" + local_id, org_id, attempted_hash, error[:500], now, now),
        )


def mark_deleted(db: TrackerDB, domain: str, local_id: str) -> None:
    assert domain in LINK_DOMAINS, f"unknown link domain {domain!r}"
    assert local_id, "local_id must be non-empty"
    with db.transaction() as conn:
        conn.execute(
            "UPDATE station_links SET state = 'deleted', updated_at = ? WHERE domain = ? AND local_id = ?",
            (utc_now_iso(), domain, local_id),
        )


def pending_links(conn: sqlite3.Connection, domain: str, limit: int) -> list[sqlite3.Row]:
    assert domain in LINK_DOMAINS, f"unknown link domain {domain!r}"
    assert 1 <= limit <= 1000, f"limit out of range: {limit}"
    rows = conn.execute(
        "SELECT * FROM station_links WHERE domain = ? AND state = 'pending' LIMIT ?",
        (domain, limit),
    ).fetchall()
    assert len(rows) <= limit, "pending query over bound"
    return rows
