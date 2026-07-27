"""Versioned schema migrations for the skilldocs database.

Each migration is (version, statements). Statements run one-by-one inside a
single transaction; the version row is inserted in the same transaction so a
migration either fully applies or not at all. Mirrors tracker/schema.py.

Migration v1 captures the schema that used to be created ad-hoc via scattered
``CREATE TABLE IF NOT EXISTS`` in store.py and control.py. Because those tables
predate versioning, v1 keeps the ``IF NOT EXISTS`` guards: a pre-versioning
skilldocs.db (no ``schema_migrations`` table) reports version 0, so v1 runs and
the guards no-op over the already-present tables, then the version row is
stamped — the DB upgrades cleanly. A fresh DB is created identically.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

MIGRATIONS_MAX: int = 50

# v1: the live-skill CRDT store + local control state.
#   skill_docs     one row per live skill (created/updated bookkeeping).
#   skill_updates  the CRDT update log — one blob per mutation, periodically
#                  compacted to a single snapshot (store.py::_compact).
#   skill_control  small last-writer-wins key/value table for the active power
#                  mode, per-skill overrides, and the disabled set.
MIGRATION_V1: tuple[str, ...] = (
    "CREATE TABLE IF NOT EXISTS skill_docs ("
    "name TEXT PRIMARY KEY, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS skill_updates ("
    "id INTEGER PRIMARY KEY, name TEXT NOT NULL, update_blob BLOB NOT NULL, ts TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS idx_skill_updates_name ON skill_updates(name, id)",
    "CREATE TABLE IF NOT EXISTS skill_control (" "key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)",
)

MIGRATIONS: tuple[tuple[int, tuple[str, ...]], ...] = ((1, MIGRATION_V1),)

assert 0 < len(MIGRATIONS) <= MIGRATIONS_MAX, "migration count out of bounds"
assert all(
    MIGRATIONS[i][0] == i + 1 for i in range(len(MIGRATIONS))
), "migration versions must be contiguous starting at 1"


def _utc_now_iso() -> str:
    stamp = datetime.now(UTC).isoformat()
    assert stamp.endswith("+00:00"), f"expected UTC timestamp, got {stamp!r}"
    return stamp


def schema_version(conn: sqlite3.Connection) -> int:
    """Current applied schema version (0 if the migrations table is absent)."""
    assert conn is not None, "schema_version on missing connection"
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'").fetchone()
    if row is None:
        return 0
    version = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0]
    assert 0 <= version <= MIGRATIONS_MAX, f"schema version {version} out of bounds"
    return int(version)


def apply_migrations(conn: sqlite3.Connection) -> int:
    """Apply pending migrations in order. Returns the final schema version."""
    assert conn is not None, "apply_migrations on missing connection"
    conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
    current = schema_version(conn)
    applied = 0
    for version, statements in MIGRATIONS:  # bounded: len(MIGRATIONS) <= MIGRATIONS_MAX
        if version <= current:
            continue
        conn.execute("BEGIN IMMEDIATE")
        try:
            for stmt in statements:
                conn.execute(stmt)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                (version, _utc_now_iso()),
            )
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
        applied += 1
    final = schema_version(conn)
    assert final == MIGRATIONS[-1][0], f"migrations incomplete: at v{final}"
    assert applied <= len(MIGRATIONS), "applied more migrations than exist"
    return final
