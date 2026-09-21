"""Polars frame builders over recipes SQL, the bounded-query layer.

Mirrors tracker/frames.py: tools never dump raw rows; they build a frame here,
filter/slice it, and hand it to the shared formatters. Frames are built fresh
per call (the dataset is small and the DB is the source of truth).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping

import polars as pl

FRAME_MAX_ROWS: int = 10_000


def _frame(rows: list[sqlite3.Row], schema: Mapping[str, type[pl.DataType]]) -> pl.DataFrame:
    """Build a typed frame from sqlite rows."""
    assert len(rows) <= FRAME_MAX_ROWS, f"frame over bound: {len(rows)} rows"
    assert len(schema) > 0, "empty frame schema"
    data = {col: [row[i] for row in rows] for i, col in enumerate(schema)}
    return pl.DataFrame(data, schema=schema)


def recipes_frame(conn: sqlite3.Connection, kind: str | None = None) -> pl.DataFrame:
    """Recipe catalog with step counts and last-run status."""
    schema: dict[str, type[pl.DataType]] = {
        "key": pl.String,
        "name": pl.String,
        "kind": pl.String,
        "summary": pl.String,
        "steps": pl.Int64,
        "runs": pl.Int64,
        "last_status": pl.String,
        "spec_hash": pl.String,
        "updated_at": pl.String,
    }
    sql = """
    SELECT r.key, r.name, r.kind, r.summary,
        json_array_length(r.steps) AS n_steps,
        (SELECT COUNT(*) FROM recipe_runs rr WHERE rr.recipe_id = r.id) AS n_runs,
        COALESCE((SELECT rr.status FROM recipe_runs rr WHERE rr.recipe_id = r.id
                  ORDER BY rr.id DESC LIMIT 1), '') AS last_status,
        substr(r.spec_hash, 1, 12) AS spec_hash,
        r.updated_at
    FROM recipes r
    """
    params: list[object] = []
    if kind is not None:
        sql += " WHERE r.kind = ?"
        params.append(kind)
    sql += " ORDER BY r.updated_at DESC, r.id DESC LIMIT ?"
    params.append(FRAME_MAX_ROWS)
    return _frame(conn.execute(sql, params).fetchall(), schema)


def runs_frame(conn: sqlite3.Connection, key: str | None = None) -> pl.DataFrame:
    """Recipe runs, newest first, with their recipe key and step tallies."""
    schema: dict[str, type[pl.DataType]] = {
        "run_id": pl.Int64,
        "recipe": pl.String,
        "status": pl.String,
        "exit_code": pl.Int64,
        "steps": pl.Int64,
        "passed": pl.Int64,
        "failed": pl.Int64,
        "skipped": pl.Int64,
        "started_at": pl.String,
        "finished_at": pl.String,
    }
    sql = """
    SELECT rr.id, r.key, rr.status, rr.exit_code,
        (SELECT COUNT(*) FROM run_steps s WHERE s.run_id = rr.id) AS n_steps,
        (SELECT COUNT(*) FROM run_steps s WHERE s.run_id = rr.id AND s.status = 'passed') AS n_passed,
        (SELECT COUNT(*) FROM run_steps s WHERE s.run_id = rr.id AND s.status = 'failed') AS n_failed,
        (SELECT COUNT(*) FROM run_steps s WHERE s.run_id = rr.id AND s.status = 'skipped') AS n_skipped,
        rr.started_at, rr.finished_at
    FROM recipe_runs rr JOIN recipes r ON r.id = rr.recipe_id
    """
    params: list[object] = []
    if key is not None:
        sql += " WHERE r.key = ?"
        params.append(key.strip())
    sql += " ORDER BY rr.id DESC LIMIT ?"
    params.append(FRAME_MAX_ROWS)
    return _frame(conn.execute(sql, params).fetchall(), schema)


def steps_frame(conn: sqlite3.Connection, run_id: int) -> pl.DataFrame:
    """Ordered step outcomes for one run."""
    schema: dict[str, type[pl.DataType]] = {
        "ordinal": pl.Int64,
        "label": pl.String,
        "command": pl.String,
        "status": pl.String,
        "exit_code": pl.Int64,
        "duration_ms": pl.Int64,
        "tail": pl.String,
    }
    rows = conn.execute(
        "SELECT ordinal, label, command, status, exit_code, duration_ms, tail "
        "FROM run_steps WHERE run_id = ? ORDER BY ordinal LIMIT ?",
        (run_id, FRAME_MAX_ROWS),
    ).fetchall()
    return _frame(rows, schema)
