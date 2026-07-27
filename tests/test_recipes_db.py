"""Tests for the recipes database layer: pragmas, migrations, path resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

from devtools_mcp.recipes.db import (
    ENV_DB_PATH,
    RecipesDB,
    apply_migrations,
    open_recipes,
    resolve_db_path,
    schema_version,
    utc_now_iso,
)
from devtools_mcp.recipes.schema import MIGRATIONS

EXPECTED_TABLES = {"schema_migrations", "recipes", "recipe_runs", "run_steps"}


@pytest.fixture
def db(tmp_path: Path) -> RecipesDB:
    recipes = open_recipes(tmp_path / "recipes.db")
    yield recipes
    recipes.close()


class TestPathResolution:
    def test_env_override(self, tmp_path, monkeypatch):
        target = tmp_path / "custom" / "my.db"
        monkeypatch.setenv(ENV_DB_PATH, str(target))
        assert resolve_db_path() == target

    def test_default_under_home(self, monkeypatch):
        monkeypatch.delenv(ENV_DB_PATH, raising=False)
        monkeypatch.delenv("DEVTOOLS_MCP_DATA", raising=False)
        path = resolve_db_path()
        assert path.name == "recipes.db"
        assert path.parent.name == ".devtools-mcp"
        assert Path.home() in path.parents

    def test_honors_data_root_override(self, tmp_path, monkeypatch):
        monkeypatch.delenv(ENV_DB_PATH, raising=False)
        monkeypatch.setenv("DEVTOOLS_MCP_DATA", str(tmp_path / "custom-data"))
        path = resolve_db_path()
        assert path == tmp_path / "custom-data" / "recipes.db"

    def test_open_recipes_uses_env(self, tmp_path, monkeypatch):
        target = tmp_path / "envdb" / "recipes.db"
        monkeypatch.setenv(ENV_DB_PATH, str(target))
        db = open_recipes()
        try:
            assert db.path == target
            assert target.exists()
        finally:
            db.close()


class TestPragmas:
    def test_wal_mode(self, db):
        assert db.conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"

    def test_foreign_keys_on(self, db):
        assert db.conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


class TestMigrations:
    def test_fresh_db_fully_migrated(self, db):
        assert schema_version(db.conn) == MIGRATIONS[-1][0]
        names = {row[0] for row in db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert names >= EXPECTED_TABLES

    def test_reopen_idempotent(self, tmp_path):
        path = tmp_path / "recipes.db"
        first = open_recipes(path)
        version_first = schema_version(first.conn)
        first.close()
        second = open_recipes(path)
        try:
            assert schema_version(second.conn) == version_first
            count = second.conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
            assert count == len(MIGRATIONS)
        finally:
            second.close()

    def test_apply_migrations_skips_applied(self, db):
        assert apply_migrations(db.conn) == MIGRATIONS[-1][0]

    def test_cascade_delete_runs_and_steps(self, db):
        """Deleting a recipe cascades to its runs and their steps (FK ON DELETE)."""
        with db.transaction() as conn:
            conn.execute(
                "INSERT INTO recipes (key, name, kind, spec_hash, created_at, updated_at) "
                "VALUES ('k', 'n', 'test', 'h', ?, ?)",
                (utc_now_iso(), utc_now_iso()),
            )
            conn.execute(
                "INSERT INTO recipe_runs (recipe_id, spec_hash, status, started_at) VALUES (1, 'h', 'passed', ?)",
                (utc_now_iso(),),
            )
            conn.execute(
                "INSERT INTO run_steps (run_id, ordinal, label, command, status) VALUES (1, 0, 'l', 'c', 'passed')"
            )
        with db.transaction() as conn:
            conn.execute("DELETE FROM recipes WHERE key = 'k'")
        assert db.conn.execute("SELECT COUNT(*) FROM recipe_runs").fetchone()[0] == 0
        assert db.conn.execute("SELECT COUNT(*) FROM run_steps").fetchone()[0] == 0


class TestTransactions:
    def test_rollback_on_error(self, db):
        with pytest.raises(RuntimeError), db.transaction() as conn:
            conn.execute(
                "INSERT INTO recipes (key, name, kind, spec_hash, created_at, updated_at) "
                "VALUES ('x', 'n', 'test', 'h', ?, ?)",
                (utc_now_iso(), utc_now_iso()),
            )
            raise RuntimeError("boom")
        assert db.conn.execute("SELECT COUNT(*) FROM recipes").fetchone()[0] == 0

    def test_close_idempotent(self, tmp_path):
        db = open_recipes(tmp_path / "t.db")
        db.close()
        db.close()  # no error
        assert db.conn is None
