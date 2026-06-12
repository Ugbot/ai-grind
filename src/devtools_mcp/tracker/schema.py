"""Versioned schema migrations for the tracker database.

Each migration is (version, statements). Statements run one-by-one inside a
single transaction; the version row is inserted in the same transaction so a
migration either fully applies or not at all.
"""

from __future__ import annotations

MIGRATIONS_MAX: int = 50

MIGRATION_V1: tuple[str, ...] = (
    """
    CREATE TABLE projects (
        id           INTEGER PRIMARY KEY,
        key          TEXT NOT NULL UNIQUE,
        name         TEXT NOT NULL,
        description  TEXT NOT NULL DEFAULT '',
        close_policy TEXT NOT NULL DEFAULT 'advisory'
                     CHECK (close_policy IN ('advisory', 'strict')),
        next_seq     INTEGER NOT NULL DEFAULT 1,
        created_at   TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE tasks (
        id          INTEGER PRIMARY KEY,
        project_id  INTEGER NOT NULL REFERENCES projects(id),
        key         TEXT NOT NULL UNIQUE,
        parent_id   INTEGER REFERENCES tasks(id),
        depth       INTEGER NOT NULL DEFAULT 0 CHECK (depth >= 0 AND depth <= 5),
        kind        TEXT NOT NULL DEFAULT 'task',
        title       TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        status      TEXT NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open', 'in_progress', 'blocked', 'done', 'cancelled')),
        priority    INTEGER NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
        sort_order  REAL NOT NULL DEFAULT 0.0,
        created_at  TEXT NOT NULL,
        updated_at  TEXT NOT NULL,
        closed_at   TEXT
    )
    """,
    "CREATE INDEX idx_tasks_parent ON tasks(parent_id)",
    "CREATE INDEX idx_tasks_project_status ON tasks(project_id, status)",
    """
    CREATE TABLE acceptance_criteria (
        id          INTEGER PRIMARY KEY,
        task_id     INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
        text        TEXT NOT NULL,
        test_ref    TEXT,
        last_result TEXT CHECK (last_result IN ('pass', 'fail')),
        last_run_at TEXT,
        created_at  TEXT NOT NULL
    )
    """,
    "CREATE INDEX idx_criteria_task ON acceptance_criteria(task_id)",
    """
    CREATE TABLE task_commits (
        id              INTEGER PRIMARY KEY,
        task_id         INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
        commit_hash     TEXT NOT NULL,
        repo_path       TEXT NOT NULL,
        message_snippet TEXT NOT NULL DEFAULT '',
        linked_at       TEXT NOT NULL,
        UNIQUE (task_id, commit_hash, repo_path)
    )
    """,
    """
    CREATE TABLE tags (
        id   INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE
    )
    """,
    """
    CREATE TABLE task_tags (
        task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
        tag_id  INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
        PRIMARY KEY (task_id, tag_id)
    )
    """,
    """
    CREATE TABLE tag_rules (
        id                INTEGER PRIMARY KEY,
        project_id        INTEGER REFERENCES projects(id),
        tag_id            INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
        match_kind        TEXT,
        match_regex       TEXT,
        match_parent_kind TEXT,
        enabled           INTEGER NOT NULL DEFAULT 1,
        created_at        TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE external_refs (
        task_id     INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
        provider    TEXT NOT NULL,
        ref_id      TEXT NOT NULL,
        repo        TEXT NOT NULL,
        url         TEXT NOT NULL,
        state       TEXT,
        last_synced TEXT,
        PRIMARY KEY (task_id, provider)
    )
    """,
)

# v2: explicit task dependencies — `task_id` cannot start until `depends_on_id`
# is done/cancelled. Edges feed the resolver (deps.py).
MIGRATION_V2: tuple[str, ...] = (
    """
    CREATE TABLE task_deps (
        task_id       INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
        depends_on_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
        created_at    TEXT NOT NULL,
        PRIMARY KEY (task_id, depends_on_id),
        CHECK (task_id != depends_on_id)
    )
    """,
    "CREATE INDEX idx_deps_depends_on ON task_deps(depends_on_id)",
)

MIGRATIONS: tuple[tuple[int, tuple[str, ...]], ...] = (
    (1, MIGRATION_V1),
    (2, MIGRATION_V2),
)

assert 0 < len(MIGRATIONS) <= MIGRATIONS_MAX, "migration count out of bounds"
assert all(
    MIGRATIONS[i][0] == i + 1 for i in range(len(MIGRATIONS))
), "migration versions must be contiguous starting at 1"
