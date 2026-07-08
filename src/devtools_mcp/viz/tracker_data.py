"""Assemble tracker page data for the visualization terminal."""

from __future__ import annotations

import polars as pl

from devtools_mcp.tracker import deps as deps_mod
from devtools_mcp.tracker import frames
from devtools_mcp.tracker import tasks as tasks_mod
from devtools_mcp.tracker.db import TrackerDB, TrackerError
from devtools_mcp.viz import render

MAX_BOARD_TASKS: int = 500


def projects_overview(db: TrackerDB) -> list[dict]:
    """All projects with status rollups and criteria progress."""
    assert db.conn is not None, "overview on closed db"
    out: list[dict] = []
    for project in tasks_mod.list_projects(db.conn):
        rows = db.conn.execute(
            "SELECT status, COUNT(*) FROM tasks WHERE project_id = ? GROUP BY status",
            (project.id,),
        ).fetchall()
        crit = db.conn.execute(
            "SELECT COUNT(*), SUM(CASE WHEN ac.last_result = 'pass' THEN 1 ELSE 0 END) "
            "FROM acceptance_criteria ac JOIN tasks t ON t.id = ac.task_id "
            "WHERE t.project_id = ?",
            (project.id,),
        ).fetchone()
        n_crit = crit[0] or 0
        n_pass = crit[1] or 0
        out.append(
            {
                "key": project.key,
                "name": project.name,
                "description": project.description,
                "close_policy": project.close_policy,
                "by_status": {row[0]: row[1] for row in rows},
                "criteria_total": n_crit,
                "criteria_passed": n_pass,
            }
        )
    assert len(out) <= 10_000, "project overview over bound"
    return out


def _task_rows(db: TrackerDB, project_key: str, limit: int = MAX_BOARD_TASKS) -> list[dict]:
    df = frames.tasks_frame(db.conn, project=project_key)
    rows = df.head(limit).to_dicts()
    return rows


def _task_row_by_key(db: TrackerDB, key: str) -> dict | None:
    df = frames.tasks_frame(db.conn)
    match = df.filter(pl.col("key") == key.upper())
    if match.is_empty():
        return None
    return match.row(0, named=True)


def board_page(db: TrackerDB, project_key: str, total_tasks: int | None = None) -> str | None:
    try:
        project = tasks_mod.get_project(db.conn, project_key)
    except TrackerError:
        return None
    rows = _task_rows(db, project.key)
    total = total_tasks or len(frames.tasks_frame(db.conn, project=project.key))
    by_key = {row["key"]: row for row in rows}
    plan = deps_mod.resolve_plan(db.conn, project.key)
    plan_data = {
        "ready": [by_key[t.key] for t in plan.ready if t.key in by_key],
        "blocked": [(by_key[t.key], blockers) for t, blockers in plan.blocked if t.key in by_key],
        "waiting_on_children": [by_key[t.key] for t in plan.waiting_on_children if t.key in by_key],
    }
    return render.tracker_board(
        {"key": project.key, "name": project.name},
        rows,
        plan_data,
        total_tasks=total,
        shown=len(rows),
    )


def task_detail_page(db: TrackerDB, key: str, linked_runs: list[str] | None = None) -> str | None:
    try:
        task = tasks_mod.get_task(db.conn, key)
    except TrackerError:
        return None
    project_key = task.key.rsplit("-", 1)[0]
    row = _task_row_by_key(db, task.key)
    if row is None:
        row = {
            "key": task.key,
            "kind": task.kind,
            "status": task.status,
            "priority": task.priority,
            "title": task.title,
            "tags": "",
            "parent": "",
            "depth": task.depth,
            "n_children": 0,
            "n_criteria": 0,
            "n_passed": 0,
        }
    row = dict(
        row,
        project=project_key,
        description=task.description,
        created_at=task.created_at,
        updated_at=task.updated_at,
        closed_at=getattr(task, "closed_at", "") or "",
    )
    parent_key = row.get("parent") or ""
    if not parent_key and task.parent_id:
        prow = db.conn.execute("SELECT key FROM tasks WHERE id = ?", (task.parent_id,)).fetchone()
        parent_key = prow[0] if prow else ""
    row["parent"] = parent_key

    criteria_rows = [
        dict(r)
        for r in db.conn.execute(
            "SELECT id, text, test_ref, last_result, last_run_at FROM acceptance_criteria "
            "WHERE task_id = ? ORDER BY id LIMIT 200",
            (task.id,),
        ).fetchall()
    ]
    commit_rows = [
        dict(r)
        for r in db.conn.execute(
            "SELECT commit_hash, repo_path, message_snippet, linked_at FROM task_commits "
            "WHERE task_id = ? ORDER BY id DESC LIMIT 50",
            (task.id,),
        ).fetchall()
    ]
    issue_rows = [
        dict(r)
        for r in db.conn.execute(
            "SELECT provider, ref_id, repo, url, state, last_synced FROM external_refs " "WHERE task_id = ?",
            (task.id,),
        ).fetchall()
    ]
    related = {
        "children": [child.key for child in tasks_mod.children_of(db.conn, task.id)],
        "waits on": [dep.key for dep in deps_mod.deps_of(db.conn, task.id)],
        "blocks": [dep.key for dep in deps_mod.dependents_of(db.conn, task.id)],
    }
    activity_rows = [
        dict(r)
        for r in db.conn.execute(
            "SELECT session_id, agent_label, file_path, op, ts FROM file_activity "
            "WHERE task_key = ? ORDER BY ts DESC LIMIT 50",
            (task.key,),
        ).fetchall()
    ]
    return render.task_detail(row, criteria_rows, commit_rows, related, issue_rows, linked_runs or [], activity_rows)


def tree_page(db: TrackerDB, project_key: str) -> str | None:
    try:
        project = tasks_mod.get_project(db.conn, project_key)
    except TrackerError:
        return None
    lines = frames.tree_lines(db.conn, project.key)
    return render.tracker_tree(project.key, lines)


def rollup_page(db: TrackerDB, project_key: str) -> str | None:
    try:
        project = tasks_mod.get_project(db.conn, project_key)
    except TrackerError:
        return None
    df = frames.rollup_frame(db.conn, project.key)
    return render.tracker_table_page(project.key, "rollup", render.table_from_df(df))


def criteria_page(db: TrackerDB, project_key: str) -> str | None:
    try:
        project = tasks_mod.get_project(db.conn, project_key)
    except TrackerError:
        return None
    df = frames.criteria_frame(db.conn, project.key)
    return render.tracker_table_page(project.key, "criteria", render.table_from_df(df))


def commits_page(db: TrackerDB, project_key: str) -> str | None:
    try:
        project = tasks_mod.get_project(db.conn, project_key)
    except TrackerError:
        return None
    df = frames.commits_frame(db.conn, project.key)
    return render.tracker_table_page(project.key, "commits", render.table_from_df(df))


def tags_page(db: TrackerDB) -> str:
    df = frames.tags_frame(db.conn)
    return render.tracker_table_page("", "tags", render.table_from_df(df), global_page=True)


def sync_page(db: TrackerDB) -> str:
    from devtools_mcp.tracker import crdt

    status = crdt.status(db)
    return render.tracker_sync(status)


def collab_page(db: TrackerDB) -> str:
    """Local agent collaboration overview: sessions, claims, recent touches."""
    from devtools_mcp.tracker import activity as activity_mod

    sessions = activity_mod.sessions_overview(db.conn)
    claims = [c.model_dump() for c in activity_mod.active_claims(db.conn)]
    recent = [t.model_dump() for t in activity_mod.recent_activity(db.conn, limit=100)]
    holders: dict[tuple[str, str], set[str]] = {}
    for entry in claims + recent:  # bounded: <= LIST_MAX + 100
        path = (entry["repo_root"], entry["file_path"])
        holders.setdefault(path, set()).add(entry["session_id"])
    contested = {path for path, sessions_on in holders.items() if len(sessions_on) > 1}
    return render.collab_page(sessions, claims, recent, contested)
