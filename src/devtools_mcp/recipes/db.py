"""Recipes database: connection management, pragmas, and migrations.

One global SQLite file (default ~/.devtools-mcp/recipes.db, overridable via
DEVTOOLS_MCP_RECIPES_DB). WAL journal, foreign keys ON, explicit transactions
via RecipesDB.transaction() (BEGIN IMMEDIATE so concurrent server instances
serialize writes safely). Mirrors tracker/db.py.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from devtools_mcp.recipes.schema import MIGRATIONS, MIGRATIONS_MAX

ENV_DB_PATH: str = "DEVTOOLS_MCP_RECIPES_DB"
ENV_DBOS_DB_PATH: str = "DEVTOOLS_MCP_DBOS_DB"
BUSY_TIMEOUT_MS: int = 5000


class RecipesError(Exception):
    """Runtime recipes error (bad input, missing recipe, malformed spec).

    An expected condition reported back to the caller — never a programmer-error
    invariant (those are asserts).
    """


def utc_now_iso() -> str:
    """Current UTC time as an ISO-8601 string (stored in all *_at columns)."""
    now = datetime.now(UTC).isoformat()
    assert now.endswith("+00:00"), f"expected UTC timestamp, got {now!r}"
    assert len(now) >= 25, f"truncated timestamp {now!r}"
    return now


def resolve_db_path() -> Path:
    """Resolve the recipes DB path: per-store env override first, else under the
    shared data root (honoring DEVTOOLS_MCP_DATA via store/paths.py::data_root)."""
    from devtools_mcp.store.paths import data_root

    override = os.environ.get(ENV_DB_PATH, "").strip()
    path = Path(override) if override else data_root() / "recipes.db"
    assert path.name, f"db path has no filename: {path!r}"
    assert not path.is_dir(), f"db path is a directory: {path}"
    return path


def resolve_dbos_db_path() -> Path:
    """Resolve the DBOS *system* DB path (durability layer, separate from recipes.db).

    Per-store env override (DEVTOOLS_MCP_DBOS_DB) first, else under the shared
    data root (honoring DEVTOOLS_MCP_DATA). Uses the ``.db`` extension for
    consistency with the other SQLite stores.
    """
    from devtools_mcp.store.paths import data_root

    override = os.environ.get(ENV_DBOS_DB_PATH, "").strip()
    path = Path(override) if override else data_root() / "dbos.db"
    assert path.name, f"dbos db path has no filename: {path!r}"
    assert not path.is_dir(), f"dbos db path is a directory: {path}"
    return path


class RecipesDB:
    """Owns one sqlite3 connection to the recipes database."""

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

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Explicit write transaction: BEGIN IMMEDIATE / COMMIT, ROLLBACK on error."""
        assert self.conn is not None, "transaction on closed RecipesDB"
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


def open_recipes(path: Path | None = None) -> RecipesDB:
    """Open (and migrate) the recipes database at `path` or the resolved default."""
    resolved = path if path is not None else resolve_db_path()
    assert isinstance(resolved, Path), f"resolved path must be Path, got {type(resolved)}"
    db = RecipesDB(resolved)
    assert db.conn is not None, "open_recipes produced closed db"
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
