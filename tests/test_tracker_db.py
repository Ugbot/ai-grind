"""Tests for the tracker database layer: pragmas, migrations, path resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

from devtools_mcp.tracker.db import (
    ENV_DB_PATH,
    TrackerDB,
    apply_migrations,
    open_tracker,
    resolve_db_path,
    schema_version,
    utc_now_iso,
)
from devtools_mcp.tracker.schema import MIGRATIONS

EXPECTED_TABLES = {
    "schema_migrations",
    "projects",
    "tasks",
    "acceptance_criteria",
    "task_commits",
    "tags",
    "task_tags",
    "tag_rules",
    "external_refs",
    "file_activity",
    "file_claims",
}


@pytest.fixture
def db(tmp_path: Path) -> TrackerDB:
    tracker = open_tracker(tmp_path / "tracker.db")
    yield tracker
    tracker.close()


class TestPathResolution:
    def test_env_override(self, tmp_path, monkeypatch):
        target = tmp_path / "custom" / "my.db"
        monkeypatch.setenv(ENV_DB_PATH, str(target))
        assert resolve_db_path() == target

    def test_default_under_home(self, monkeypatch):
        monkeypatch.delenv(ENV_DB_PATH, raising=False)
        monkeypatch.delenv("DEVTOOLS_MCP_DATA", raising=False)
        path = resolve_db_path()
        assert path.name == "tracker.db"
        assert path.parent.name == ".devtools-mcp"
        assert Path.home() in path.parents

    def test_honors_data_root_override(self, tmp_path, monkeypatch):
        monkeypatch.delenv(ENV_DB_PATH, raising=False)
        monkeypatch.setenv("DEVTOOLS_MCP_DATA", str(tmp_path / "custom-data"))
        path = resolve_db_path()
        assert path == tmp_path / "custom-data" / "tracker.db"

    def test_open_tracker_uses_env(self, tmp_path, monkeypatch):
        target = tmp_path / "envdb" / "tracker.db"
        monkeypatch.setenv(ENV_DB_PATH, str(target))
        tracker = open_tracker()
        try:
            assert tracker.path == target
            assert target.exists()
        finally:
            tracker.close()


class TestPragmas:
    def test_wal_mode(self, db):
        mode = db.conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"

    def test_foreign_keys_on(self, db):
        fk = db.conn.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk == 1


class TestMigrations:
    def test_fresh_db_fully_migrated(self, db):
        assert schema_version(db.conn) == MIGRATIONS[-1][0]
        names = {row[0] for row in db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert names >= EXPECTED_TABLES

    def test_reopen_idempotent(self, tmp_path):
        path = tmp_path / "tracker.db"
        first = open_tracker(path)
        version_first = schema_version(first.conn)
        first.close()
        second = open_tracker(path)
        try:
            assert schema_version(second.conn) == version_first
            count = second.conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
            assert count == len(MIGRATIONS)
        finally:
            second.close()

    def test_apply_migrations_skips_applied(self, db):
        applied = apply_migrations(db.conn)
        assert applied == MIGRATIONS[-1][0]

    def test_migration_rows_have_timestamps(self, db):
        rows = db.conn.execute("SELECT version, applied_at FROM schema_migrations").fetchall()
        assert len(rows) == len(MIGRATIONS)
        for row in rows:
            assert row["applied_at"].endswith("+00:00")


class TestV5Migration:
    def test_upgrades_v4_db_preserving_rows(self, tmp_path):
        """A DB stopped at v4 migrates to v5 on reopen with existing rows intact."""
        import sqlite3

        path = tmp_path / "old.db"
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("BEGIN")
        conn.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
        for version, statements in MIGRATIONS[:4]:
            if version == 3:
                # insert before the v3 capture triggers exist — they call the
                # Python-registered crdt_hlc(), absent on this raw connection
                conn.execute(
                    "INSERT INTO projects (key, name, created_at) VALUES (?, ?, ?)",
                    ("OLD", "pre-v5 project", utc_now_iso()),
                )
            for stmt in statements:
                conn.execute(stmt)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                (version, utc_now_iso()),
            )
        conn.commit()
        conn.close()
        db = open_tracker(path)
        try:
            assert schema_version(db.conn) == MIGRATIONS[-1][0]
            row = db.conn.execute("SELECT key FROM projects").fetchone()
            assert row["key"] == "OLD"
            names = {r[0] for r in db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            assert {"file_activity", "file_claims"} <= names
        finally:
            db.close()

    def test_active_claim_unique_index(self, db):
        """The partial unique index rejects a second active claim at the SQL level."""
        import sqlite3

        args = ("s1", "", None, "/repo", "src/x.py", utc_now_iso(), utc_now_iso())
        insert = (
            "INSERT INTO file_claims "
            "(session_id, agent_label, task_key, repo_root, file_path, claimed_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        with db.transaction() as conn:
            conn.execute(insert, args)
        with pytest.raises(sqlite3.IntegrityError), db.transaction() as conn:
            conn.execute(insert, ("s2", *args[1:]))
        with db.transaction() as conn:  # released claim frees the slot
            conn.execute("UPDATE file_claims SET released_at = ?", (utc_now_iso(),))
            conn.execute(insert, ("s2", *args[1:]))
        count = db.conn.execute("SELECT COUNT(*) FROM file_claims").fetchone()[0]
        assert count == 2


class TestTransactions:
    def test_commit_persists(self, db):
        with db.transaction() as conn:
            conn.execute(
                "INSERT INTO projects (key, name, created_at) VALUES (?, ?, ?)",
                ("AB", "test", utc_now_iso()),
            )
        count = db.conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
        assert count == 1

    def test_rollback_on_error(self, db):
        with pytest.raises(RuntimeError), db.transaction() as conn:
            conn.execute(
                "INSERT INTO projects (key, name, created_at) VALUES (?, ?, ?)",
                ("CD", "test", utc_now_iso()),
            )
            raise RuntimeError("boom")
        count = db.conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
        assert count == 0

    def test_close_idempotent(self, tmp_path):
        tracker = open_tracker(tmp_path / "t.db")
        tracker.close()
        tracker.close()  # no error
        assert tracker.conn is None


class TestUtcNow:
    def test_format(self):
        stamp = utc_now_iso()
        assert stamp.endswith("+00:00")
        assert "T" in stamp
