"""Versioned schema migrations for the recipes database.

Each migration is (version, statements). Statements run one-by-one inside a
single transaction; the version row is inserted in the same transaction so a
migration either fully applies or not at all. Mirrors tracker/schema.py.
"""

from __future__ import annotations

MIGRATIONS_MAX: int = 50

# v1: the whole recipe/pipeline engine —
#   recipes       one row per registered pipeline (an ordered list of steps),
#                 addressed by a stable `key`; `spec_hash` fingerprints the
#                 executable spec (kind + env_axes + steps) for cache validity.
#   recipe_runs   one row per execution of a recipe.
#   run_steps     one row per step within a run (ordered by `ordinal`).
MIGRATION_V1: tuple[str, ...] = (
    """
    CREATE TABLE recipes (
        id         INTEGER PRIMARY KEY,
        key        TEXT NOT NULL,
        name       TEXT NOT NULL,
        kind       TEXT NOT NULL DEFAULT 'task',
        summary    TEXT NOT NULL DEFAULT '',
        env_axes   TEXT NOT NULL DEFAULT '{}',
        steps      TEXT NOT NULL DEFAULT '[]',
        spec_hash  TEXT NOT NULL,
        source     TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    # Unique index doubles as the required index on recipes.key.
    "CREATE UNIQUE INDEX idx_recipes_key ON recipes(key)",
    """
    CREATE TABLE recipe_runs (
        id          INTEGER PRIMARY KEY,
        recipe_id   INTEGER NOT NULL REFERENCES recipes(id) ON DELETE CASCADE,
        spec_hash   TEXT NOT NULL,
        status      TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'passed', 'failed', 'cancelled')),
        exit_code   INTEGER,
        raw_path    TEXT,
        started_at  TEXT NOT NULL,
        finished_at TEXT
    )
    """,
    "CREATE INDEX idx_recipe_runs_recipe ON recipe_runs(recipe_id)",
    """
    CREATE TABLE run_steps (
        id          INTEGER PRIMARY KEY,
        run_id      INTEGER NOT NULL REFERENCES recipe_runs(id) ON DELETE CASCADE,
        ordinal     INTEGER NOT NULL,
        label       TEXT NOT NULL,
        command     TEXT NOT NULL,
        exit_code   INTEGER,
        status      TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'passed', 'failed', 'skipped')),
        duration_ms INTEGER,
        tail        TEXT NOT NULL DEFAULT ''
    )
    """,
    "CREATE INDEX idx_run_steps_run ON run_steps(run_id)",
)

MIGRATIONS: tuple[tuple[int, tuple[str, ...]], ...] = ((1, MIGRATION_V1),)

assert 0 < len(MIGRATIONS) <= MIGRATIONS_MAX, "migration count out of bounds"
assert all(
    MIGRATIONS[i][0] == i + 1 for i in range(len(MIGRATIONS))
), "migration versions must be contiguous starting at 1"
