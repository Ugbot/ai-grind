"""Task dependencies and the execution-plan resolver.

An edge (task -> depends_on) means the dependency must reach done/cancelled
before the task can start. The resolver answers "what needs to happen": which
open tasks are ready now, which are blocked and by what, and the topological
layers in which the remaining work can proceed (tasks in one layer are
parallelizable).

Hierarchy interacts with planning in one way: a task with open children is not
"ready" itself — its punch cards are; it shows up as waiting on subtasks.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso
from devtools_mcp.tracker.models import CLOSED_STATUSES, Task
from devtools_mcp.tracker.tasks import get_task

MAX_DEPS_PER_TASK: int = 50
MAX_GRAPH_NODES: int = 10_000
MAX_LAYERS: int = 1_000


def add_dep(db: TrackerDB, task_key: str, depends_on_key: str) -> bool:
    """Add 'task depends on other'. Returns False if the edge already existed."""
    with db.transaction() as conn:
        task = get_task(conn, task_key)
        dep = get_task(conn, depends_on_key)
        if task.id == dep.id:
            raise TrackerError("A task cannot depend on itself")
        if task.project_id != dep.project_id:
            raise TrackerError("Dependencies must stay within one project")
        count = conn.execute("SELECT COUNT(*) FROM task_deps WHERE task_id = ?", (task.id,)).fetchone()[0]
        if count >= MAX_DEPS_PER_TASK:
            raise TrackerError(f"Dependency limit reached ({MAX_DEPS_PER_TASK}) for {task.key}")
        _check_no_dep_cycle(conn, task.id, dep.id)
        cursor = conn.execute(
            "INSERT OR IGNORE INTO task_deps (task_id, depends_on_id, created_at) VALUES (?, ?, ?)",
            (task.id, dep.id, utc_now_iso()),
        )
        inserted = cursor.rowcount
    assert inserted in (0, 1), f"inserted {inserted} rows for one edge"
    return inserted == 1


def remove_dep(db: TrackerDB, task_key: str, depends_on_key: str) -> bool:
    """Remove a dependency edge. Returns True if it existed."""
    with db.transaction() as conn:
        task = get_task(conn, task_key)
        dep = get_task(conn, depends_on_key)
        cursor = conn.execute(
            "DELETE FROM task_deps WHERE task_id = ? AND depends_on_id = ?",
            (task.id, dep.id),
        )
        removed = cursor.rowcount
    assert removed in (0, 1), f"removed {removed} rows for one edge"
    return removed == 1


def deps_of(conn: sqlite3.Connection, task_id: int) -> list[Task]:
    """Tasks this task depends on (its blockers, satisfied or not)."""
    assert task_id > 0, f"bad task_id {task_id}"
    rows = conn.execute(
        "SELECT t.* FROM tasks t JOIN task_deps d ON d.depends_on_id = t.id "
        "WHERE d.task_id = ? ORDER BY t.key LIMIT ?",
        (task_id, MAX_DEPS_PER_TASK),
    ).fetchall()
    return [Task.from_row(row) for row in rows]


def dependents_of(conn: sqlite3.Connection, task_id: int) -> list[Task]:
    """Tasks that depend on this task."""
    assert task_id > 0, f"bad task_id {task_id}"
    rows = conn.execute(
        "SELECT t.* FROM tasks t JOIN task_deps d ON d.task_id = t.id "
        "WHERE d.depends_on_id = ? ORDER BY t.key LIMIT ?",
        (task_id, MAX_GRAPH_NODES),
    ).fetchall()
    return [Task.from_row(row) for row in rows]


def unblocked_by_closing(conn: sqlite3.Connection, task_id: int) -> list[str]:
    """Keys of open dependents whose ONLY unsatisfied dependency is this task.

    Call before (or after) closing `task_id` — the answer is the same because
    the check excludes `task_id` from the unsatisfied set explicitly.
    """
    assert task_id > 0, f"bad task_id {task_id}"
    keys: list[str] = []
    for dependent in dependents_of(conn, task_id):  # bounded by MAX_GRAPH_NODES
        if dependent.status in CLOSED_STATUSES:
            continue
        blockers = [
            dep for dep in deps_of(conn, dependent.id) if dep.status not in CLOSED_STATUSES and dep.id != task_id
        ]
        if not blockers:
            keys.append(dependent.key)
    assert len(keys) <= MAX_GRAPH_NODES, "unblocked list over bound"
    return sorted(keys)


def _check_no_dep_cycle(conn: sqlite3.Connection, task_id: int, new_dep_id: int) -> None:
    """Reject the edge task->new_dep if new_dep already (transitively) depends on task."""
    assert task_id > 0 and new_dep_id > 0, "ids must be positive"
    seen: set[int] = set()
    frontier = [new_dep_id]
    for _ in range(MAX_GRAPH_NODES):  # bounded BFS
        if not frontier:
            return
        current = frontier.pop()
        if current == task_id:
            raise TrackerError(
                "Dependency rejected: it would create a cycle " "(the dependency already depends on this task)"
            )
        if current in seen:
            continue
        seen.add(current)
        rows = conn.execute("SELECT depends_on_id FROM task_deps WHERE task_id = ?", (current,)).fetchall()
        frontier.extend(row[0] for row in rows)
    raise AssertionError(f"dependency graph exceeds {MAX_GRAPH_NODES} nodes")


@dataclass
class Plan:
    """Resolver output: what needs to happen for a project (or a goal task)."""

    project_key: str
    goal_key: str | None
    ready: list[Task] = field(default_factory=list)  # actionable now
    waiting_on_children: list[Task] = field(default_factory=list)
    blocked: list[tuple[Task, list[str]]] = field(default_factory=list)  # task, blocker keys
    layers: list[list[str]] = field(default_factory=list)  # topological key layers

    @property
    def open_count(self) -> int:
        return len(self.ready) + len(self.waiting_on_children) + len(self.blocked)


def _goal_closure(conn: sqlite3.Connection, open_tasks: dict[int, Task], goal: Task) -> set[int]:
    """Open tasks relevant to a goal: its subtree plus transitive deps (and their subtrees)."""
    relevant: set[int] = set()
    frontier = [goal.id]
    for _ in range(MAX_GRAPH_NODES):  # bounded worklist
        if not frontier:
            return relevant
        current = frontier.pop()
        if current in relevant or current not in open_tasks:
            continue
        relevant.add(current)
        children = conn.execute("SELECT id FROM tasks WHERE parent_id = ?", (current,)).fetchall()
        deps = conn.execute("SELECT depends_on_id FROM task_deps WHERE task_id = ?", (current,)).fetchall()
        frontier.extend(row[0] for row in children)
        frontier.extend(row[0] for row in deps)
    raise AssertionError(f"goal closure exceeds {MAX_GRAPH_NODES} nodes")


def resolve_plan(conn: sqlite3.Connection, project_key: str, goal_key: str | None = None) -> Plan:
    """Compute the execution plan over a project's open tasks.

    With goal_key, the plan is restricted to what the goal transitively needs:
    its open subtree plus dependency closure.
    """
    assert project_key, "resolve_plan needs a project key"
    rows = conn.execute(
        "SELECT t.* FROM tasks t JOIN projects p ON p.id = t.project_id "
        "WHERE p.key = ? AND t.status NOT IN ('done', 'cancelled') "
        "ORDER BY t.priority, t.key LIMIT ?",
        (project_key.strip().upper(), MAX_GRAPH_NODES),
    ).fetchall()
    open_tasks = {row["id"]: Task.from_row(row) for row in rows}
    plan = Plan(project_key=project_key.strip().upper(), goal_key=goal_key)
    if goal_key is not None:
        goal = get_task(conn, goal_key)
        if goal.status in CLOSED_STATUSES:
            return plan  # nothing needs to happen
        relevant = _goal_closure(conn, open_tasks, goal)
        open_tasks = {tid: task for tid, task in open_tasks.items() if tid in relevant}

    # Unsatisfied dependency edges among open tasks (deps on closed tasks are done).
    edges: dict[int, set[int]] = {tid: set() for tid in open_tasks}
    for tid in open_tasks:
        for dep in deps_of(conn, tid):
            if dep.id in open_tasks:
                edges[tid].add(dep.id)
    has_open_children = {
        tid for tid in open_tasks if any(child.id in open_tasks for child in _children_ids(conn, tid, open_tasks))
    }

    for tid, task in open_tasks.items():
        blockers = edges[tid]
        if blockers:
            plan.blocked.append((task, sorted(open_tasks[b].key for b in blockers)))
        elif tid in has_open_children:
            plan.waiting_on_children.append(task)
        else:
            plan.ready.append(task)

    plan.layers = _topo_layers(open_tasks, edges)
    assert plan.open_count == len(open_tasks), "plan lost tasks during classification"
    return plan


def _children_ids(conn: sqlite3.Connection, task_id: int, open_tasks: dict[int, Task]) -> list[Task]:
    """Open direct children of a task (helper kept tiny for resolve_plan)."""
    assert task_id > 0, f"bad task_id {task_id}"
    rows = conn.execute("SELECT id FROM tasks WHERE parent_id = ?", (task_id,)).fetchall()
    return [open_tasks[row[0]] for row in rows if row[0] in open_tasks]


def _topo_layers(open_tasks: dict[int, Task], edges: dict[int, set[int]]) -> list[list[str]]:
    """Kahn's algorithm into parallelizable layers of task keys."""
    assert set(edges) == set(open_tasks), "edges must cover exactly the open tasks"
    remaining = {tid: set(deps) for tid, deps in edges.items()}
    layers: list[list[str]] = []
    for _ in range(MAX_LAYERS):  # bounded: each pass removes >=1 node or stops
        if not remaining:
            return layers
        free = sorted(tid for tid, deps in remaining.items() if not deps)
        if not free:
            # Cycle should be impossible (add_dep checks); fail loud, not infinite.
            raise AssertionError(f"dependency cycle among tasks: {sorted(remaining)}")
        layers.append(sorted(open_tasks[tid].key for tid in free))
        for tid in free:
            del remaining[tid]
        for deps in remaining.values():
            deps.difference_update(free)
    raise AssertionError(f"more than {MAX_LAYERS} dependency layers")
