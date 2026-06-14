"""Assemble tracker page data for the visualization terminal.

Bridges the tracker domain layer to the pure renderers: every function takes a
request-scoped TrackerDB, gathers plain dicts, and returns finished HTML (or
None for a 404). No HTML is built here beyond calling render functions.
"""

from __future__ import annotations

from devtools_mcp.tracker import deps as deps_mod
from devtools_mcp.tracker import frames
from devtools_mcp.tracker import tasks as tasks_mod
from devtools_mcp.tracker.db import TrackerDB, TrackerError
from devtools_mcp.viz import render

MAX_BOARD_TASKS: int = 500


def projects_overview(db: TrackerDB) -> list[dict]:
    """All projects with their status rollups."""
    assert db.conn is not None, "overview on closed db"
    out: list[dict] = []
    for project in tasks_mod.list_projects(db.conn):
        rows = db.conn.execute(
            "SELECT status, COUNT(*) FROM tasks WHERE project_id = ? GROUP BY status",
            (project.id,),
        ).fetchall()
        out.append(
            {
                "key": project.key,
                "name": project.name,
                "description": project.description,
                "close_policy": project.close_policy,
                "by_status": {row[0]: row[1] for row in rows},
            }
        )
    assert len(out) <= 10_000, "project overview over bound"
    return out


def _task_rows(db: TrackerDB, project_key: str) -> list[dict]:
    """Task card dicts for a project (bounded), via the shared frame builder."""
    df = frames.tasks_frame(db.conn, project=project_key)
    rows = df.head(MAX_BOARD_TASKS).to_dicts()
    assert len(rows) <= MAX_BOARD_TASKS, "board rows over bound"
    return rows


def board_page(db: TrackerDB, project_key: str) -> str | None:
    """The per-project board + execution plan, or None if no such project."""
    try:
        project = tasks_mod.get_project(db.conn, project_key)
    except TrackerError:
        return None
    rows = _task_rows(db, project.key)
    by_key = {row["key"]: row for row in rows}
    plan = deps_mod.resolve_plan(db.conn, project.key)
    plan_data = {
        "ready": [by_key[t.key] for t in plan.ready if t.key in by_key],
        "blocked": [(by_key[t.key], blockers) for t, blockers in plan.blocked if t.key in by_key],
    }
    return render.tracker_board({"key": project.key, "name": project.name}, rows, plan_data)


def task_detail_page(db: TrackerDB, key: str) -> str | None:
    """One task fully expanded, or None if the key is unknown."""
    try:
        task = tasks_mod.get_task(db.conn, key)
    except TrackerError:
        return None
    project_key = task.key.rsplit("-", 1)[0]
    rows = _task_rows(db, project_key)
    row = next((r for r in rows if r["key"] == task.key), None)
    if row is None:
        return None
    row = dict(
        row, project=project_key, description=task.description, created_at=task.created_at, updated_at=task.updated_at
    )
    criteria_rows = [
        dict(r)
        for r in db.conn.execute(
            "SELECT text, test_ref, last_result FROM acceptance_criteria WHERE task_id = ? " "ORDER BY id LIMIT 200",
            (task.id,),
        ).fetchall()
    ]
    commit_rows = [
        dict(r)
        for r in db.conn.execute(
            "SELECT commit_hash, message_snippet FROM task_commits WHERE task_id = ? " "ORDER BY id DESC LIMIT 10",
            (task.id,),
        ).fetchall()
    ]
    related = {
        "children": [child.key for child in tasks_mod.children_of(db.conn, task.id)],
        "waits on": [dep.key for dep in deps_mod.deps_of(db.conn, task.id)],
        "blocks": [dep.key for dep in deps_mod.dependents_of(db.conn, task.id)],
    }
    assert row["key"] == task.key, "detail row mismatch"
    return render.task_detail(row, criteria_rows, commit_rows, related)
