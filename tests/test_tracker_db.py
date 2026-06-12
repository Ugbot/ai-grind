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
        path = resolve_db_path()
        assert path.name == "tracker.db"
        assert path.parent.name == ".devtools-mcp"
        assert Path.home() in path.parents

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
