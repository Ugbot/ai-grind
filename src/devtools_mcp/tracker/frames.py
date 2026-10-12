"""Polars frame builders over tracker SQL, the bounded-query layer.

Mirrors the no-token-flood contract of the profiling tools: tools never dump
raw rows; they build a frame here, filter/slice it, and hand it to the shared
formatters. Frames are built fresh per call (the dataset is small and the DB
is the source of truth).
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Mapping

import polars as pl

from devtools_mcp.tracker.db import TrackerError

FRAME_MAX_ROWS: int = 10_000
TREE_MAX_LINES: int = 500

_TASKS_SQL = """
SELECT
    t.key, p.key AS project, parent.key AS parent, t.depth, t.kind, t.status,
    t.priority, t.title, t.description,
    (SELECT COUNT(*) FROM tasks c WHERE c.parent_id = t.id) AS n_children,
    (SELECT COUNT(*) FROM acceptance_criteria ac WHERE ac.task_id = t.id) AS n_criteria,
    (SELECT COUNT(*) FROM acceptance_criteria ac
      WHERE ac.task_id = t.id AND ac.last_result = 'pass') AS n_passed,
    COALESCE((SELECT GROUP_CONCAT(tg.name, ',') FROM tags tg
      JOIN task_tags tt ON tt.tag_id = tg.id WHERE tt.task_id = t.id), '') AS tags,
    t.created_at, t.updated_at
FROM tasks t
JOIN projects p ON p.id = t.project_id
LEFT JOIN tasks parent ON parent.id = t.parent_id
"""

_TASK_SCHEMA: dict[str, type[pl.DataType]] = {
    "key": pl.String,
    "project": pl.String,
    "parent": pl.String,
    "depth": pl.Int64,
    "kind": pl.String,
    "status": pl.String,
    "priority": pl.Int64,
    "title": pl.String,
    "description": pl.String,
    "n_children": pl.Int64,
    "n_criteria": pl.Int64,
    "n_passed": pl.Int64,
    "tags": pl.String,
    "created_at": pl.String,
    "updated_at": pl.String,
}


def _frame(rows: list[sqlite3.Row], schema: Mapping[str, type[pl.DataType]]) -> pl.DataFrame:
    """Build a typed frame from sqlite rows."""
    assert len(rows) <= FRAME_MAX_ROWS, f"frame over bound: {len(rows)} rows"
    assert len(schema) > 0, "empty frame schema"
    data = {col: [row[i] for row in rows] for i, col in enumerate(schema)}
    return pl.DataFrame(data, schema=schema)


def tasks_frame(
    conn: sqlite3.Connection,
    project: str | None = None,
    status: str | None = None,
    kind: str | None = None,
    tag: str | None = None,
    parent: str | None = None,
    title_pattern: str | None = None,
) -> pl.DataFrame:
    """Task table with rollup columns, filtered."""
    clauses: list[str] = []
    params: list[object] = []
    if project is not None:
        clauses.append("p.key = ?")
        params.append(project.strip().upper())
    if status is not None:
        clauses.append("t.status = ?")
        params.append(status)
    if kind is not None:
        clauses.append("t.kind = ?")
        params.append(kind)
    if parent is not None:
        clauses.append("parent.key = ?")
        params.append(parent.strip().upper())
    if tag is not None:
        clauses.append(
            "EXISTS (SELECT 1 FROM task_tags tt JOIN tags tg ON tg.id = tt.tag_id "
            "WHERE tt.task_id = t.id AND tg.name = ?)"
        )
        params.append(tag.strip().lower())
    sql = _TASKS_SQL + (" WHERE " + " AND ".join(clauses) if clauses else "")
    sql += " ORDER BY p.key, t.depth, t.sort_order, t.id LIMIT ?"
    params.append(FRAME_MAX_ROWS)
    rows = conn.execute(sql, params).fetchall()
    df = _frame(rows, _TASK_SCHEMA)
    if title_pattern is not None:
        try:
            re.compile(title_pattern)
        except re.error as exc:
            raise TrackerError(f"Bad title_pattern {title_pattern!r}: {exc}") from exc
        df = df.filter(pl.col("title").str.contains(title_pattern))
    assert len(df) <= FRAME_MAX_ROWS, "filtered frame over bound"
    return df


def criteria_frame(conn: sqlite3.Connection, project: str | None = None) -> pl.DataFrame:
    """All acceptance criteria with their task keys."""
    schema = {
        "task": pl.String,
        "id": pl.Int64,
        "text": pl.String,
        "test_ref": pl.String,
        "last_result": pl.String,
        "last_run_at": pl.String,
    }
    sql = (
        "SELECT t.key, ac.id, ac.text, ac.test_ref, ac.last_result, ac.last_run_at "
        "FROM acceptance_criteria ac JOIN tasks t ON t.id = ac.task_id "
        "JOIN projects p ON p.id = t.project_id"
    )
    params: list[object] = []
    if project is not None:
        sql += " WHERE p.key = ?"
        params.append(project.strip().upper())
    sql += " ORDER BY t.key, ac.id LIMIT ?"
    params.append(FRAME_MAX_ROWS)
    return _frame(conn.execute(sql, params).fetchall(), schema)


def commits_frame(conn: sqlite3.Connection, project: str | None = None) -> pl.DataFrame:
    """All commit links with their task keys."""
    schema = {
        "task": pl.String,
        "commit": pl.String,
        "repo": pl.String,
        "message": pl.String,
        "linked_at": pl.String,
    }
    sql = (
        "SELECT t.key, substr(tc.commit_hash, 1, 12), tc.repo_path, tc.message_snippet, "
        "tc.linked_at FROM task_commits tc JOIN tasks t ON t.id = tc.task_id "
        "JOIN projects p ON p.id = t.project_id"
    )
    params: list[object] = []
    if project is not None:
        sql += " WHERE p.key = ?"
        params.append(project.strip().upper())
    sql += " ORDER BY tc.id DESC LIMIT ?"
    params.append(FRAME_MAX_ROWS)
    return _frame(conn.execute(sql, params).fetchall(), schema)


def activity_frame(
    conn: sqlite3.Connection,
    repo: str | None = None,
    session: str | None = None,
) -> pl.DataFrame:
    """File-touch activity log, newest first (local agent collaboration)."""
    schema = {
        "session": pl.String,
        "agent": pl.String,
        "task": pl.String,
        "repo": pl.String,
        "file": pl.String,
        "op": pl.String,
        "tool": pl.String,
        "ts": pl.String,
    }
    sql = "SELECT session_id, agent_label, task_key, repo_root, file_path, op, tool_name, ts " "FROM file_activity"
    clauses: list[str] = []
    params: list[object] = []
    if repo is not None:
        clauses.append("repo_root = ?")
        params.append(repo)
    if session is not None:
        clauses.append("session_id = ?")
        params.append(session)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY ts DESC LIMIT ?"
    params.append(FRAME_MAX_ROWS)
    return _frame(conn.execute(sql, params).fetchall(), schema)


def claims_frame(
    conn: sqlite3.Connection,
    repo: str | None = None,
    active_only: bool = True,
) -> pl.DataFrame:
    """File claims (advisory leases), soonest-expiring first."""
    schema = {
        "session": pl.String,
        "agent": pl.String,
        "task": pl.String,
        "repo": pl.String,
        "file": pl.String,
        "claimed_at": pl.String,
        "expires_at": pl.String,
        "released_at": pl.String,
    }
    sql = (
        "SELECT session_id, agent_label, task_key, repo_root, file_path, "
        "claimed_at, expires_at, released_at FROM file_claims"
    )
    clauses: list[str] = []
    params: list[object] = []
    if active_only:
        clauses.append("released_at IS NULL")
    if repo is not None:
        clauses.append("repo_root = ?")
        params.append(repo)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY expires_at LIMIT ?"
    params.append(FRAME_MAX_ROWS)
    return _frame(conn.execute(sql, params).fetchall(), schema)


def tags_frame(conn: sqlite3.Connection, project: str | None = None) -> pl.DataFrame:
    """Tag usage counts."""
    schema = {"tag": pl.String, "tasks": pl.Int64}
    sql = (
        "SELECT tg.name, COUNT(tt.task_id) FROM tags tg "
        "LEFT JOIN task_tags tt ON tt.tag_id = tg.id "
        "LEFT JOIN tasks t ON t.id = tt.task_id "
        "LEFT JOIN projects p ON p.id = t.project_id"
    )
    params: list[object] = []
    if project is not None:
        sql += " WHERE p.key = ?"
        params.append(project.strip().upper())
    sql += " GROUP BY tg.name ORDER BY COUNT(tt.task_id) DESC, tg.name LIMIT ?"
    params.append(FRAME_MAX_ROWS)
    return _frame(conn.execute(sql, params).fetchall(), schema)


def deps_frame(conn: sqlite3.Connection, project: str | None = None) -> pl.DataFrame:
    """Task dependency edges with status of depends_on task."""
    schema = {
        "task": pl.String,
        "depends_on": pl.String,
        "depends_on_status": pl.String,
        "created_at": pl.String,
    }
    sql = (
        "SELECT t.key, d.key, d.status, td.created_at "
        "FROM task_deps td "
        "JOIN tasks t ON t.id = td.task_id "
        "JOIN tasks d ON d.id = td.depends_on_id "
        "JOIN projects p ON p.id = t.project_id"
    )
    params: list[object] = []
    if project is not None:
        sql += " WHERE p.key = ?"
        params.append(project.strip().upper())
    sql += " ORDER BY t.key, d.key LIMIT ?"
    params.append(FRAME_MAX_ROWS)
    return _frame(conn.execute(sql, params).fetchall(), schema)


def issues_frame(conn: sqlite3.Connection, project: str | None = None) -> pl.DataFrame:
    """External issue links (GitHub/GitLab)."""
    schema = {
        "task": pl.String,
        "provider": pl.String,
        "ref_id": pl.String,
        "repo": pl.String,
        "url": pl.String,
        "state": pl.String,
        "last_synced": pl.String,
    }
    sql = (
        "SELECT t.key, er.provider, er.ref_id, er.repo, er.url, er.state, er.last_synced "
        "FROM external_refs er JOIN tasks t ON t.id = er.task_id "
        "JOIN projects p ON p.id = t.project_id"
    )
    params: list[object] = []
    if project is not None:
        sql += " WHERE p.key = ?"
        params.append(project.strip().upper())
    sql += " ORDER BY t.key LIMIT ?"
    params.append(FRAME_MAX_ROWS)
    return _frame(conn.execute(sql, params).fetchall(), schema)


def rollup_frame(conn: sqlite3.Connection, project: str | None = None) -> pl.DataFrame:
    """Per-project, per-kind status rollup with criteria pass rates."""
    schema = {
        "project": pl.String,
        "kind": pl.String,
        "total": pl.Int64,
        "open": pl.Int64,
        "in_progress": pl.Int64,
        "blocked": pl.Int64,
        "done": pl.Int64,
        "cancelled": pl.Int64,
        "criteria": pl.Int64,
        "criteria_passed": pl.Int64,
    }
    sql = """
    SELECT p.key, t.kind, COUNT(*),
        SUM(t.status = 'open'), SUM(t.status = 'in_progress'), SUM(t.status = 'blocked'),
        SUM(t.status = 'done'), SUM(t.status = 'cancelled'),
        (SELECT COUNT(*) FROM acceptance_criteria ac JOIN tasks tt ON tt.id = ac.task_id
          WHERE tt.project_id = p.id AND tt.kind = t.kind),
        (SELECT COUNT(*) FROM acceptance_criteria ac JOIN tasks tt ON tt.id = ac.task_id
          WHERE tt.project_id = p.id AND tt.kind = t.kind AND ac.last_result = 'pass')
    FROM tasks t JOIN projects p ON p.id = t.project_id
    """
    params: list[object] = []
    if project is not None:
        sql += " WHERE p.key = ?"
        params.append(project.strip().upper())
    sql += " GROUP BY p.key, t.kind ORDER BY p.key, t.kind LIMIT ?"
    params.append(FRAME_MAX_ROWS)
    return _frame(conn.execute(sql, params).fetchall(), schema)


def tree_lines(
    conn: sqlite3.Connection,
    project: str,
    root_key: str | None = None,
) -> list[str]:
    """Indented hierarchy rendering, bounded at TREE_MAX_LINES."""
    assert project, "tree_lines requires a project key"
    # sort_order is offset by 1e6 in the path so reorder-induced negative values
    # still sort correctly as fixed-width strings.
    if root_key is None:
        sql = """
        WITH RECURSIVE tree(id, key, kind, status, title, depth, path) AS (
            SELECT t.id, t.key, t.kind, t.status, t.title, 0,
                   printf('%016.4f', t.sort_order + 1000000) || '-' || t.id
            FROM tasks t JOIN projects p ON p.id = t.project_id
            WHERE p.key = ? AND t.parent_id IS NULL
            UNION ALL
            SELECT t.id, t.key, t.kind, t.status, t.title, tree.depth + 1,
                   tree.path || '/' || printf('%016.4f', t.sort_order + 1000000) || '-' || t.id
            FROM tasks t JOIN tree ON t.parent_id = tree.id
        ) SELECT key, kind, status, title, depth FROM tree ORDER BY path LIMIT ?
        """
        params: tuple = (project.strip().upper(), TREE_MAX_LINES + 1)
    else:
        sql = """
        WITH RECURSIVE tree(id, key, kind, status, title, depth, path) AS (
            SELECT t.id, t.key, t.kind, t.status, t.title, 0,
                   printf('%016.4f', t.sort_order + 1000000) || '-' || t.id
            FROM tasks t WHERE t.key = ?
            UNION ALL
            SELECT t.id, t.key, t.kind, t.status, t.title, tree.depth + 1,
                   tree.path || '/' || printf('%016.4f', t.sort_order + 1000000) || '-' || t.id
            FROM tasks t JOIN tree ON t.parent_id = tree.id
        ) SELECT key, kind, status, title, depth FROM tree ORDER BY path LIMIT ?
        """
        params = (root_key.strip().upper(), TREE_MAX_LINES + 1)
    rows = conn.execute(sql, params).fetchall()
    truncated = len(rows) > TREE_MAX_LINES
    marks = {"done": "[x]", "cancelled": "[-]", "blocked": "[!]", "in_progress": "[>]"}
    lines: list[str] = []
    for row in rows[:TREE_MAX_LINES]:  # bounded
        mark = marks.get(row["status"], "[ ]")
        lines.append(f"{'  ' * row['depth']}{mark} {row['key']} ({row['kind']}) {row['title']}")
    if truncated:
        lines.append(f"... truncated at {TREE_MAX_LINES} lines")
    assert len(lines) <= TREE_MAX_LINES + 1, "tree over bound"
    return lines
