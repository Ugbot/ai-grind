"""A small SQLite index over the on-disk run catalog.

Run blobs (meta.json/result.json/parquet/raw/artifacts) stay as files under
``<root>/runs/<run_id>/`` — this index is purely additive: a durable, queryable
table so ``list_run_ids()`` and ``runs_for_task_key()`` don't have to scan every
directory and JSON-parse every meta.json. Follows the tracker pattern (its own
DB file under the data root, WAL + foreign_keys + versioned migrations, a
per-store env override). A disk-scan backfill keeps runs written before the
index existed (or by another process) resolvable.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

ENV_DB_PATH: str = "DEVTOOLS_MCP_RUNS_DB"
BUSY_TIMEOUT_MS: int = 5000
MIGRATIONS_MAX: int = 50

# v1: one row per persisted run — the fields lookups filter/sort on. The blobs
# themselves stay on disk; this is a pure index.
MIGRATION_V1: tuple[str, ...] = (
    """
    CREATE TABLE runs (
        run_id     TEXT PRIMARY KEY,
        suite      TEXT NOT NULL DEFAULT '',
        tool       TEXT NOT NULL DEFAULT '',
        task_key   TEXT NOT NULL DEFAULT '',
        git_commit TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT '',
        tags       TEXT NOT NULL DEFAULT '',
        workspace  TEXT NOT NULL DEFAULT ''
    )
    """,
    "CREATE INDEX idx_runs_task_key ON runs(task_key)",
    "CREATE INDEX idx_runs_created ON runs(created_at)",
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


def resolve_db_path(root: Path | None = None) -> Path:
    """Resolve the runs-index DB path.

    An explicit ``root`` base dir wins (mirrors the run store's root); otherwise
    the per-store env override (DEVTOOLS_MCP_RUNS_DB) wins; otherwise it lives
    under the shared data root (honoring DEVTOOLS_MCP_DATA).
    """
    if root is not None:
        path = root / "runs.db"
    else:
        override = os.environ.get(ENV_DB_PATH, "").strip()
        if override:
            path = Path(override)
        else:
            from devtools_mcp.store.paths import data_root

            path = data_root() / "runs.db"
    assert path.name, f"db path has no filename: {path!r}"
    assert not path.is_dir(), f"db path is a directory: {path}"
    return path


def schema_version(conn: sqlite3.Connection) -> int:
    """Current applied schema version (0 if the migrations table is absent)."""
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations'").fetchone()
    if row is None:
        return 0
    version = conn.execute("SELECT COALESCE(MAX(version), 0) FROM schema_migrations").fetchone()[0]
    assert 0 <= version <= MIGRATIONS_MAX, f"schema version {version} out of bounds"
    return int(version)


def apply_migrations(conn: sqlite3.Connection) -> int:
    """Apply pending migrations in order. Returns the final schema version."""
    conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
    current = schema_version(conn)
    for version, statements in MIGRATIONS:  # bounded
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
    final = schema_version(conn)
    assert final == MIGRATIONS[-1][0], f"migrations incomplete: at v{final}"
    return final


class RunIndex:
    """Owns one sqlite3 connection to the runs-index database."""

    def __init__(self, path: Path) -> None:
        assert isinstance(path, Path), f"path must be Path, got {type(path)}"
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(str(path), isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
        fk_on = self.conn.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk_on == 1, "foreign_keys pragma did not take"
        apply_migrations(self.conn)

    def upsert(
        self,
        run_id: str,
        *,
        suite: str = "",
        tool: str = "",
        task_key: str = "",
        git_commit: str = "",
        created_at: str = "",
        tags: str = "",
        workspace: str = "",
    ) -> None:
        assert run_id, "run_id required"
        self.conn.execute(
            "INSERT INTO runs (run_id, suite, tool, task_key, git_commit, created_at, tags, workspace) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(run_id) DO UPDATE SET "
            "suite=excluded.suite, tool=excluded.tool, task_key=excluded.task_key, "
            "git_commit=excluded.git_commit, created_at=excluded.created_at, "
            "tags=excluded.tags, workspace=excluded.workspace",
            (run_id, suite, tool, task_key.upper(), git_commit, created_at, tags, workspace),
        )

    def has(self, run_id: str) -> bool:
        return self.conn.execute("SELECT 1 FROM runs WHERE run_id = ?", (run_id,)).fetchone() is not None

    def indexed_ids(self) -> set[str]:
        return {row[0] for row in self.conn.execute("SELECT run_id FROM runs")}

    def list_ids(self) -> list[str]:
        return [row[0] for row in self.conn.execute("SELECT run_id FROM runs ORDER BY run_id")]

    def ids_for_task_key(self, task_key: str) -> list[str]:
        key = task_key.strip().upper()
        rows = self.conn.execute("SELECT run_id FROM runs WHERE task_key = ? ORDER BY run_id", (key,)).fetchall()
        return [row[0] for row in rows]

    def delete(self, run_id: str) -> None:
        self.conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None  # type: ignore[assignment]
