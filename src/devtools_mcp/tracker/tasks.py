"""Projects and tasks: CRUD, key allocation, hierarchy, status workflow.

Hierarchy is a self-referencing parent_id with depth 0..MAX_DEPTH (6 levels).
Keys are human-readable PROJ-123, allocated atomically from projects.next_seq
inside the same transaction as the insert, so they are safe across concurrent
server instances sharing the database.
"""

from __future__ import annotations

import re
import sqlite3
import uuid

from devtools_mcp.tracker import criteria as criteria_mod
from devtools_mcp.tracker import tags as tags_mod
from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso
from devtools_mcp.tracker.models import (
    CLOSED_STATUSES,
    MAX_DEPTH,
    POLICIES,
    PRIORITY_MAX,
    PRIORITY_MIN,
    STATUSES,
    Project,
    Task,
)

PROJECT_KEY_RE = re.compile(r"^[A-Z][A-Z0-9]{1,9}$")
TASK_KEY_RE = re.compile(r"^([A-Z][A-Z0-9]{1,9})-(\d+)$")
MAX_BREAKDOWN: int = 20
MAX_LIST: int = 10_000

# Default child kind when breaking a task down, by parent kind.
CHILD_KIND: dict[str, str] = {"epic": "story", "story": "task", "task": "subtask"}


def create_project(
    db: TrackerDB,
    key: str,
    name: str,
    description: str = "",
    close_policy: str = "advisory",
) -> Project:
    """Create a project. Key must match [A-Z][A-Z0-9]{1,9} (e.g. 'GRIND')."""
    key = key.strip().upper()
    if not PROJECT_KEY_RE.match(key):
        raise TrackerError(f"Bad project key {key!r}: need 2-10 chars, [A-Z][A-Z0-9]*")
    if close_policy not in POLICIES:
        raise TrackerError(f"Bad close_policy {close_policy!r}; one of: {', '.join(POLICIES)}")
    if not name.strip():
        raise TrackerError("Project name must not be empty")
    with db.transaction() as conn:
        existing = conn.execute("SELECT id FROM projects WHERE key = ?", (key,)).fetchone()
        if existing is not None:
            raise TrackerError(f"Project {key} already exists")
        conn.execute(
            "INSERT INTO projects (key, name, description, close_policy, created_at) " "VALUES (?, ?, ?, ?, ?)",
            (key, name.strip(), description, close_policy, utc_now_iso()),
        )
        project = get_project(conn, key)
    assert project.key == key, "created project key mismatch"
    assert project.next_seq == 1, "fresh project must start sequence at 1"
    return project


def get_project(conn: sqlite3.Connection, key: str) -> Project:
    """Fetch a project by key; raises TrackerError if absent."""
    assert key, "empty project key"
    row = conn.execute("SELECT * FROM projects WHERE key = ?", (key.strip().upper(),)).fetchone()
    if row is None:
        raise TrackerError(f"Project {key!r} not found. Create it with tracker_project create.")
    return Project.from_row(row)


def list_projects(conn: sqlite3.Connection) -> list[Project]:
    """All projects, oldest first."""
    rows = conn.execute("SELECT * FROM projects ORDER BY id LIMIT ?", (MAX_LIST,)).fetchall()
    projects = [Project.from_row(row) for row in rows]
    assert len(projects) <= MAX_LIST, "project list over bound"
    return projects


def set_policy(db: TrackerDB, key: str, close_policy: str) -> Project:
    """Change a project's close policy (advisory|strict)."""
    if close_policy not in POLICIES:
        raise TrackerError(f"Bad close_policy {close_policy!r}; one of: {', '.join(POLICIES)}")
    with db.transaction() as conn:
        project = get_project(conn, key)
        conn.execute("UPDATE projects SET close_policy = ? WHERE id = ?", (close_policy, project.id))
        updated = get_project(conn, key)
    assert updated.close_policy == close_policy, "policy update did not take"
    return updated


def get_task(conn: sqlite3.Connection, key: str) -> Task:
    """Fetch a task by key (e.g. 'GRIND-7'); raises TrackerError if absent."""
    assert key, "empty task key"
    row = conn.execute("SELECT * FROM tasks WHERE key = ?", (key.strip().upper(),)).fetchone()
    if row is None:
        raise TrackerError(f"Task {key!r} not found")
    return Task.from_row(row)


def children_of(conn: sqlite3.Connection, task_id: int) -> list[Task]:
    """Direct children ordered by sort_order."""
    assert task_id > 0, f"bad task_id {task_id}"
    rows = conn.execute(
        "SELECT * FROM tasks WHERE parent_id = ? ORDER BY sort_order, id LIMIT ?",
        (task_id, MAX_LIST),
    ).fetchall()
    return [Task.from_row(row) for row in rows]


def _next_sort_order(conn: sqlite3.Connection, project_id: int, parent_id: int | None) -> float:
    """Sort order placing a new task after its last sibling."""
    assert project_id > 0, f"bad project_id {project_id}"
    if parent_id is None:
        row = conn.execute(
            "SELECT MAX(sort_order) FROM tasks WHERE project_id = ? AND parent_id IS NULL",
            (project_id,),
        ).fetchone()
    else:
        row = conn.execute("SELECT MAX(sort_order) FROM tasks WHERE parent_id = ?", (parent_id,)).fetchone()
    highest = row[0] if row[0] is not None else 0.0
    assert isinstance(highest, (int, float)), f"sort_order not numeric: {highest!r}"
    return float(highest) + 1.0


def create_task(
    db: TrackerDB,
    project_key: str,
    title: str,
    description: str = "",
    kind: str = "task",
    parent_key: str | None = None,
    priority: int = 3,
) -> tuple[Task, list[str]]:
    """Create a task; returns (task, auto-applied tag names)."""
    if not title.strip():
        raise TrackerError("Task title must not be empty")
    if not (PRIORITY_MIN <= priority <= PRIORITY_MAX):
        raise TrackerError(f"Priority must be {PRIORITY_MIN}..{PRIORITY_MAX}, got {priority}")
    if not kind.strip():
        raise TrackerError("Task kind must not be empty")
    with db.transaction() as conn:
        project = get_project(conn, project_key)
        parent: Task | None = None
        if parent_key is not None:
            parent = get_task(conn, parent_key)
            if parent.project_id != project.id:
                raise TrackerError(f"Parent {parent.key} belongs to a different project than {project.key}")
            if parent.depth >= MAX_DEPTH:
                raise TrackerError(f"Cannot nest under {parent.key}: would exceed {MAX_DEPTH + 1} levels")
        seq = conn.execute(
            "UPDATE projects SET next_seq = next_seq + 1 WHERE id = ? RETURNING next_seq - 1",
            (project.id,),
        ).fetchone()[0]
        assert seq >= 1, f"sequence must be positive, got {seq}"
        key = f"{project.key}-{seq}"
        now = utc_now_iso()
        conn.execute(
            "INSERT INTO tasks (uid, project_id, key, parent_id, depth, kind, title, description, "
            "priority, sort_order, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                uuid.uuid4().hex,  # global identity for CRDT sync
                project.id,
                key,
                parent.id if parent else None,
                parent.depth + 1 if parent else 0,
                kind.strip().lower(),
                title.strip(),
                description,
                priority,
                _next_sort_order(conn, project.id, parent.id if parent else None),
                now,
                now,
            ),
        )
        task = get_task(conn, key)
        applied = tags_mod.apply_rules(conn, task, parent.kind if parent else None)
    assert task.key == key, "created task key mismatch"
    return task, applied


def update_task(
    db: TrackerDB,
    key: str,
    title: str | None = None,
    description: str | None = None,
    kind: str | None = None,
    priority: int | None = None,
) -> Task:
    """Update mutable task fields (title/description/kind/priority)."""
    if title is None and description is None and kind is None and priority is None:
        raise TrackerError("Nothing to update: pass title, description, kind, or priority")
    if priority is not None and not (PRIORITY_MIN <= priority <= PRIORITY_MAX):
        raise TrackerError(f"Priority must be {PRIORITY_MIN}..{PRIORITY_MAX}, got {priority}")
    if title is not None and not title.strip():
        raise TrackerError("Task title must not be empty")
    with db.transaction() as conn:
        task = get_task(conn, key)
        conn.execute(
            "UPDATE tasks SET title = COALESCE(?, title), description = COALESCE(?, description), "
            "kind = COALESCE(?, kind), priority = COALESCE(?, priority), updated_at = ? "
            "WHERE id = ?",
            (title, description, kind, priority, utc_now_iso(), task.id),
        )
        updated = get_task(conn, key)
    assert updated.id == task.id, "update changed task identity"
    return updated


def _subtree_height(conn: sqlite3.Connection, task_id: int) -> int:
    """Height of a task's subtree (0 = leaf)."""
    assert task_id > 0, f"bad task_id {task_id}"
    row = conn.execute(
        "WITH RECURSIVE sub(id, rel) AS ("
        "  SELECT id, 0 FROM tasks WHERE id = ?"
        "  UNION ALL"
        "  SELECT t.id, s.rel + 1 FROM tasks t JOIN sub s ON t.parent_id = s.id"
        ") SELECT MAX(rel) FROM sub",
        (task_id,),
    ).fetchone()
    height = row[0] if row[0] is not None else 0
    assert 0 <= height <= MAX_DEPTH, f"subtree height {height} out of bounds"
    return height


def _check_no_cycle(conn: sqlite3.Connection, task_id: int, new_parent_id: int) -> None:
    """Raise TrackerError if new_parent is task itself or inside its subtree."""
    assert task_id > 0 and new_parent_id > 0, "ids must be positive"
    if task_id == new_parent_id:
        raise TrackerError("Cannot parent a task to itself")
    current: int | None = new_parent_id
    for _ in range(MAX_DEPTH + 1):  # bounded ancestor walk
        row = conn.execute("SELECT parent_id FROM tasks WHERE id = ?", (current,)).fetchone()
        if row is None or row[0] is None:
            return
        current = row[0]
        if current == task_id:
            raise TrackerError("Move would create a cycle (new parent is a descendant)")
    raise AssertionError("ancestor chain longer than depth bound")


def _rebase_subtree(conn: sqlite3.Connection, task: Task, new_parent: Task | None) -> None:
    """Reparent task and rewrite depth for its whole subtree."""
    new_depth = new_parent.depth + 1 if new_parent else 0
    height = _subtree_height(conn, task.id)
    if new_depth + height > MAX_DEPTH:
        raise TrackerError(
            f"Move rejected: subtree of {task.key} is {height + 1} levels tall and would "
            f"exceed the {MAX_DEPTH + 1}-level bound at its new position"
        )
    if new_parent is not None:
        _check_no_cycle(conn, task.id, new_parent.id)
    conn.execute(
        "UPDATE tasks SET parent_id = ?, updated_at = ? WHERE id = ?",
        (new_parent.id if new_parent else None, utc_now_iso(), task.id),
    )
    conn.execute(
        "WITH RECURSIVE sub(id, rel) AS ("
        "  SELECT id, 0 FROM tasks WHERE id = ?"
        "  UNION ALL"
        "  SELECT t.id, s.rel + 1 FROM tasks t JOIN sub s ON t.parent_id = s.id"
        ") UPDATE tasks SET depth = ? + (SELECT rel FROM sub WHERE sub.id = tasks.id) "
        "WHERE id IN (SELECT id FROM sub)",
        (task.id, new_depth),
    )


def move_task(
    db: TrackerDB,
    key: str,
    new_parent_key: str | None = None,
    to_root: bool = False,
    before_key: str | None = None,
) -> Task:
    """Reparent (new_parent_key or to_root) and/or reorder before a sibling."""
    if new_parent_key is None and not to_root and before_key is None:
        raise TrackerError("Nothing to move: pass parent, to_root, or before")
    if new_parent_key is not None and to_root:
        raise TrackerError("Pass either a new parent or to_root, not both")
    with db.transaction() as conn:
        task = get_task(conn, key)
        if new_parent_key is not None or to_root:
            new_parent = get_task(conn, new_parent_key) if new_parent_key else None
            if new_parent is not None and new_parent.project_id != task.project_id:
                raise TrackerError("Cannot move a task to a parent in another project")
            _rebase_subtree(conn, task, new_parent)
            task = get_task(conn, key)
        if before_key is not None:
            sibling = get_task(conn, before_key)
            if sibling.parent_id != task.parent_id or sibling.project_id != task.project_id:
                raise TrackerError(f"{sibling.key} is not a sibling of {task.key}")
            prev = conn.execute(
                "SELECT MAX(sort_order) FROM tasks WHERE project_id = ? "
                "AND parent_id IS ? AND sort_order < ? AND id != ?",
                (task.project_id, task.parent_id, sibling.sort_order, task.id),
            ).fetchone()[0]
            lower = prev if prev is not None else sibling.sort_order - 2.0
            conn.execute(
                "UPDATE tasks SET sort_order = ?, updated_at = ? WHERE id = ?",
                ((lower + sibling.sort_order) / 2.0, utc_now_iso(), task.id),
            )
        moved = get_task(conn, key)
    assert moved.id == task.id, "move changed task identity"
    assert 0 <= moved.depth <= MAX_DEPTH, "move produced bad depth"
    return moved


def set_status(
    db: TrackerDB,
    key: str,
    status: str,
    override: bool = False,
) -> tuple[Task, list[str]]:
    """Transition a task's status; returns (task, warnings).

    Closing to 'done' evaluates the acceptance gate. Strict projects reject
    (TrackerError) unless override; advisory projects close with warnings.
    """
    if status not in STATUSES:
        raise TrackerError(f"Bad status {status!r}; one of: {', '.join(STATUSES)}")
    warnings: list[str] = []
    with db.transaction() as conn:
        task = get_task(conn, key)
        project = conn.execute("SELECT * FROM projects WHERE id = ?", (task.project_id,)).fetchone()
        assert project is not None, f"task {task.key} has no project row"
        if status == "done":
            unmet, unlinked = criteria_mod.evaluate_gate(conn, task.id)
            for criterion in unmet:
                warnings.append(f"Unmet criterion #{criterion.id}: {criterion.text}")
            for criterion in unlinked:
                warnings.append(f"Criterion #{criterion.id} has no linked test: {criterion.text}")
            if warnings and project["close_policy"] == "strict" and not override:
                raise TrackerError(
                    f"Strict close gate rejected {task.key}:\n- "
                    + "\n- ".join(warnings)
                    + "\n(Pass override=True to force.)"
                )
        closed_at = utc_now_iso() if status in CLOSED_STATUSES else None
        conn.execute(
            "UPDATE tasks SET status = ?, closed_at = ?, updated_at = ? WHERE id = ?",
            (status, closed_at, utc_now_iso(), task.id),
        )
        updated = get_task(conn, key)
    assert updated.status == status, "status update did not take"
    return updated, warnings


def breakdown(
    db: TrackerDB,
    parent_key: str,
    titles: list[str],
    kind: str | None = None,
) -> list[tuple[Task, list[str]]]:
    """Punch-card breakdown: create subtasks under a parent from a list of titles."""
    if not titles:
        raise TrackerError("Breakdown needs at least one subtask title")
    if len(titles) > MAX_BREAKDOWN:
        raise TrackerError(f"Breakdown capped at {MAX_BREAKDOWN} subtasks, got {len(titles)}")
    with db.transaction() as conn:
        parent = get_task(conn, parent_key)
    child_kind = kind if kind is not None else CHILD_KIND.get(parent.kind, "subtask")
    project_key = parent.key.rsplit("-", 1)[0]
    created: list[tuple[Task, list[str]]] = []
    for title in titles:  # bounded by MAX_BREAKDOWN check above
        created.append(create_task(db, project_key, title, kind=child_kind, parent_key=parent.key))
    assert len(created) == len(titles), "breakdown created wrong count"
    return created
