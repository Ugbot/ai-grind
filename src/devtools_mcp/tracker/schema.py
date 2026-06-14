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

# Capture-trigger generator for the CRDT op-log (migration v3). Each synced
# table gets AFTER INSERT/UPDATE/DELETE triggers that append a row-level op
# with a site-independent payload (uids and natural keys, never local rowids).
# The WHEN guard suppresses capture while merge_ops is applying remote state.
_NOT_APPLYING = "(SELECT value FROM crdt_state WHERE key = 'applying') = '0'"

_TASK_PAYLOAD = (
    "json_object('uid', NEW.uid, "
    "'project', (SELECT key FROM projects WHERE id = NEW.project_id), "
    "'key', NEW.key, "
    "'parent_uid', (SELECT uid FROM tasks WHERE id = NEW.parent_id), "
    "'kind', NEW.kind, 'title', NEW.title, 'description', NEW.description, "
    "'status', NEW.status, 'priority', NEW.priority, 'sort_order', NEW.sort_order, "
    "'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'closed_at', NEW.closed_at)"
)

_PROJECT_PAYLOAD = (
    "json_object('key', NEW.key, 'name', NEW.name, 'description', NEW.description, "
    "'close_policy', NEW.close_policy, 'next_seq', NEW.next_seq, 'created_at', NEW.created_at)"
)

_CRITERION_PAYLOAD = (
    "json_object('uid', NEW.uid, "
    "'task_uid', (SELECT uid FROM tasks WHERE id = NEW.task_id), "
    "'text', NEW.text, 'test_ref', NEW.test_ref, 'last_result', NEW.last_result, "
    "'last_run_at', NEW.last_run_at, 'created_at', NEW.created_at)"
)

_DEP_PAYLOAD = (
    "json_object('task_uid', (SELECT uid FROM tasks WHERE id = NEW.task_id), "
    "'depends_on_uid', (SELECT uid FROM tasks WHERE id = NEW.depends_on_id), "
    "'created_at', NEW.created_at)"
)

_TASK_TAG_PAYLOAD = (
    "json_object('task_uid', (SELECT uid FROM tasks WHERE id = NEW.task_id), "
    "'tag', (SELECT name FROM tags WHERE id = NEW.tag_id))"
)

_COMMIT_PAYLOAD = (
    "json_object('task_uid', (SELECT uid FROM tasks WHERE id = NEW.task_id), "
    "'commit_hash', NEW.commit_hash, 'repo_path', NEW.repo_path, "
    "'message_snippet', NEW.message_snippet, 'linked_at', NEW.linked_at)"
)

# (table, pk expr over NEW, pk expr over OLD, payload expr, has UPDATE trigger,
#  extra WHEN for DELETE — guards cascades where the parent row is already gone)
_CAPTURE_SPECS: tuple[tuple[str, str, str, str, bool, str], ...] = (
    ("projects", "NEW.key", "OLD.key", _PROJECT_PAYLOAD, True, ""),
    ("tasks", "NEW.uid", "OLD.uid", _TASK_PAYLOAD, True, ""),
    (
        "acceptance_criteria",
        "NEW.uid",
        "OLD.uid",
        _CRITERION_PAYLOAD,
        True,
        " AND (SELECT uid FROM tasks WHERE id = OLD.task_id) IS NOT NULL",
    ),
    (
        "task_deps",
        "(SELECT uid FROM tasks WHERE id = NEW.task_id) || ':' || "
        "(SELECT uid FROM tasks WHERE id = NEW.depends_on_id)",
        "(SELECT uid FROM tasks WHERE id = OLD.task_id) || ':' || "
        "(SELECT uid FROM tasks WHERE id = OLD.depends_on_id)",
        _DEP_PAYLOAD,
        False,
        " AND (SELECT uid FROM tasks WHERE id = OLD.task_id) IS NOT NULL"
        " AND (SELECT uid FROM tasks WHERE id = OLD.depends_on_id) IS NOT NULL",
    ),
    (
        "task_tags",
        "(SELECT uid FROM tasks WHERE id = NEW.task_id) || ':' || " "(SELECT name FROM tags WHERE id = NEW.tag_id)",
        "(SELECT uid FROM tasks WHERE id = OLD.task_id) || ':' || " "(SELECT name FROM tags WHERE id = OLD.tag_id)",
        _TASK_TAG_PAYLOAD,
        False,
        " AND (SELECT uid FROM tasks WHERE id = OLD.task_id) IS NOT NULL"
        " AND (SELECT name FROM tags WHERE id = OLD.tag_id) IS NOT NULL",
    ),
    (
        "task_commits",
        "(SELECT uid FROM tasks WHERE id = NEW.task_id) || '|' || NEW.commit_hash " "|| '|' || NEW.repo_path",
        "(SELECT uid FROM tasks WHERE id = OLD.task_id) || '|' || OLD.commit_hash " "|| '|' || OLD.repo_path",
        _COMMIT_PAYLOAD,
        False,
        " AND (SELECT uid FROM tasks WHERE id = OLD.task_id) IS NOT NULL",
    ),
)


def _capture_triggers() -> tuple[str, ...]:
    """Generate the op-capture triggers for every synced table."""
    assert len(_CAPTURE_SPECS) == 6, "capture spec count drifted"
    ddl: list[str] = []
    insert_op = (
        "INSERT INTO crdt_ops (hlc, site_id, tbl, pk, op, payload) VALUES "
        "(crdt_hlc(), (SELECT site_id FROM crdt_site WHERE id = 1), '{tbl}', {pk}, '{op}', {payload})"
    )
    for tbl, pk_new, pk_old, payload, has_update, delete_guard in _CAPTURE_SPECS:  # bounded
        events = [("ins", "INSERT"), ("upd", "UPDATE")] if has_update else [("ins", "INSERT")]
        for short, event in events:
            ddl.append(
                f"CREATE TRIGGER crdt_cap_{tbl}_{short} AFTER {event} ON {tbl} "
                f"WHEN {_NOT_APPLYING} BEGIN "
                + insert_op.format(tbl=tbl, pk=pk_new, op="upsert", payload=payload)
                + "; END"
            )
        ddl.append(
            f"CREATE TRIGGER crdt_cap_{tbl}_del AFTER DELETE ON {tbl} "
            f"WHEN {_NOT_APPLYING}{delete_guard} BEGIN "
            + insert_op.format(tbl=tbl, pk=pk_old, op="delete", payload="NULL")
            + "; END"
        )
    assert len(ddl) == 15, f"expected 15 capture triggers, built {len(ddl)}"
    return tuple(ddl)


# v3: CRDT collaboration layer — global row identities (uid), the replica's
# site id + clock state, the op-log every mutation is captured into (via the
# triggers above), and per-peer sync watermarks. See tracker/crdt.py.
MIGRATION_V3: tuple[str, ...] = (
    "ALTER TABLE tasks ADD COLUMN uid TEXT",
    "UPDATE tasks SET uid = lower(hex(randomblob(16))) WHERE uid IS NULL",
    "CREATE UNIQUE INDEX idx_tasks_uid ON tasks(uid)",
    "ALTER TABLE acceptance_criteria ADD COLUMN uid TEXT",
    "UPDATE acceptance_criteria SET uid = lower(hex(randomblob(16))) WHERE uid IS NULL",
    "CREATE UNIQUE INDEX idx_criteria_uid ON acceptance_criteria(uid)",
    """
    CREATE TABLE crdt_site (
        id      INTEGER PRIMARY KEY CHECK (id = 1),
        site_id TEXT NOT NULL
    )
    """,
    "INSERT INTO crdt_site (id, site_id) VALUES (1, lower(hex(randomblob(16))))",
    """
    CREATE TABLE crdt_state (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
    "INSERT INTO crdt_state (key, value) VALUES ('applying', '0')",
    """
    CREATE TABLE crdt_ops (
        seq     INTEGER PRIMARY KEY,
        hlc     TEXT NOT NULL,
        site_id TEXT NOT NULL,
        tbl     TEXT NOT NULL,
        pk      TEXT NOT NULL,
        op      TEXT NOT NULL CHECK (op IN ('upsert', 'delete')),
        payload TEXT,
        UNIQUE (hlc, site_id)
    )
    """,
    "CREATE INDEX idx_crdt_ops_hlc ON crdt_ops(hlc)",
    "CREATE INDEX idx_crdt_ops_row ON crdt_ops(tbl, pk)",
    """
    CREATE TABLE crdt_peers (
        url             TEXT PRIMARY KEY,
        last_pulled_hlc TEXT,
        last_synced     TEXT
    )
    """,
    *_capture_triggers(),
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

# v4: indexes for the reverse-direction probes the schema was missing.
#   - task_tags(tag_id): every tag aggregation/filter/rollup join and the
#     `tags ON DELETE CASCADE` probe task_tags by tag_id; the PK (task_id,
#     tag_id) cannot serve that left-suffix lookup. Mirrors idx_deps_depends_on.
#   - tasks(project_id, kind): the per-project+kind rollup (GROUP BY p.key,
#     t.kind) and kind-filtered task queries; project_status doesn't cover kind.
MIGRATION_V4: tuple[str, ...] = (
    "CREATE INDEX idx_task_tags_tag ON task_tags(tag_id)",
    "CREATE INDEX idx_tasks_project_kind ON tasks(project_id, kind)",
)

MIGRATIONS: tuple[tuple[int, tuple[str, ...]], ...] = (
    (1, MIGRATION_V1),
    (2, MIGRATION_V2),
    (3, MIGRATION_V3),
    (4, MIGRATION_V4),
)

assert 0 < len(MIGRATIONS) <= MIGRATIONS_MAX, "migration count out of bounds"
assert all(
    MIGRATIONS[i][0] == i + 1 for i in range(len(MIGRATIONS))
), "migration versions must be contiguous starting at 1"
