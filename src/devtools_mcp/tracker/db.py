"""Tracker database: connection management, pragmas, and migrations.

One global SQLite file (default ~/.devtools-mcp/tracker.db, overridable via
DEVTOOLS_MCP_TRACKER_DB). WAL journal, foreign keys ON, explicit transactions
via TrackerDB.transaction() (BEGIN IMMEDIATE so concurrent server instances
serialize writes safely).
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from devtools_mcp.tracker.schema import MIGRATIONS, MIGRATIONS_MAX

ENV_DB_PATH: str = "DEVTOOLS_MCP_TRACKER_DB"
BUSY_TIMEOUT_MS: int = 5000


class TrackerError(Exception):
    """Runtime tracker error (bad input, policy violation, missing row).

    These are expected conditions reported back to the caller. Never a
    programmer-error invariant (those are asserts).
    """


def utc_now_iso() -> str:
    """Current UTC time as an ISO-8601 string (stored in all *_at columns)."""
    now = datetime.now(UTC).isoformat()
    assert now.endswith("+00:00"), f"expected UTC timestamp, got {now!r}"
    assert len(now) >= 25, f"truncated timestamp {now!r}"
    return now


def resolve_db_path() -> Path:
    """Resolve the tracker DB path: per-store env override first, else under the
    shared data root (honoring DEVTOOLS_MCP_DATA via store/paths.py::data_root)."""
    from devtools_mcp.store.paths import data_root

    override = os.environ.get(ENV_DB_PATH, "").strip()
    path = Path(override) if override else data_root() / "tracker.db"
    assert path.name, f"db path has no filename: {path!r}"
    assert not path.is_dir(), f"db path is a directory: {path}"
    return path


class TrackerDB:
    """Owns one sqlite3 connection to the tracker database."""

    def __init__(self, path: Path) -> None:
        assert isinstance(path, Path), f"path must be Path, got {type(path)}"
        path.parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None: autocommit mode; transactions are explicit
        # BEGIN IMMEDIATE blocks via transaction() below.
        self.path = path
        self.conn = sqlite3.connect(str(path), isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
        fk_on = self.conn.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk_on == 1, "foreign_keys pragma did not take"
        apply_migrations(self.conn)
        self._init_crdt()

    def _init_crdt(self) -> None:
        """Wire this connection into the CRDT layer: identity, clock, capture fn.

        The v3 capture triggers call `crdt_hlc()`; registering it per connection
        means every mutation through this TrackerDB is stamped, and a connection
        that skips registration fails loudly instead of silently dropping ops.
        """
        from devtools_mcp.tracker import crdt  # deferred: crdt imports this module

        self.site_id = crdt.ensure_identity(self.conn)
        assert len(self.site_id) >= 8, f"bad site id {self.site_id!r}"
        self.hlc = crdt.HLC(self.site_id)
        self.conn.create_function("crdt_hlc", 0, self.hlc.next_str, deterministic=False)
        watermark = crdt.latest_hlc(self.conn)
        if watermark is not None:
            self.hlc.observe(watermark)  # never re-issue a stamp from a past session

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Explicit write transaction: BEGIN IMMEDIATE / COMMIT, ROLLBACK on error."""
        assert self.conn is not None, "transaction on closed TrackerDB"
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def close(self) -> None:
        """Close the connection (idempotent). Releases WAL sidecar files."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None  # type: ignore[assignment]
        assert self.conn is None, "close did not clear connection"


def open_tracker(path: Path | None = None) -> TrackerDB:
    """Open (and migrate) the tracker database at `path` or the resolved default."""
    resolved = path if path is not None else resolve_db_path()
    assert isinstance(resolved, Path), f"resolved path must be Path, got {type(resolved)}"
    db = TrackerDB(resolved)
    assert db.conn is not None, "open_tracker produced closed db"
    return db


def schema_version(conn: sqlite3.Connection) -> int:
    """Current applied schema version (0 if the migrations table is absent)."""
    assert conn is not None, "schema_version on missing connection"
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'").fetchone()
    if row is None:
        return 0
    version = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0]
    assert 0 <= version <= MIGRATIONS_MAX, f"schema version {version} out of bounds"
    return version


def apply_migrations(conn: sqlite3.Connection) -> int:
    """Apply pending migrations in order. Returns the final schema version."""
    assert conn is not None, "apply_migrations on missing connection"
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (" "version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
    )
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
                (version, utc_now_iso()),
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
