"""Acceptance criteria: CRUD, test linkage, result recording, and the close gate.

A criterion is "met" when its last recorded result is 'pass', and "linked"
when it references a concrete test (e.g. 'tests/test_x.py::test_name').
The close gate reports both lists; policy enforcement lives in tasks.set_status.
"""

from __future__ import annotations

import sqlite3
import uuid

from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso
from devtools_mcp.tracker.models import Criterion

MAX_CRITERIA_PER_TASK: int = 200
RESULTS: tuple[str, ...] = ("pass", "fail")


def add_criterion(db: TrackerDB, task_id: int, text: str, test_ref: str | None = None) -> Criterion:
    """Add an acceptance criterion to a task."""
    assert task_id > 0, f"bad task_id {task_id}"
    if not text.strip():
        raise TrackerError("Criterion text must not be empty")
    with db.transaction() as conn:
        count = conn.execute("SELECT COUNT(*) FROM acceptance_criteria WHERE task_id = ?", (task_id,)).fetchone()[0]
        if count >= MAX_CRITERIA_PER_TASK:
            raise TrackerError(f"Criteria limit reached ({MAX_CRITERIA_PER_TASK}) for task")
        cursor = conn.execute(
            "INSERT INTO acceptance_criteria (uid, task_id, text, test_ref, created_at) " "VALUES (?, ?, ?, ?, ?)",
            (uuid.uuid4().hex, task_id, text.strip(), test_ref, utc_now_iso()),
        )
        new_id = cursor.lastrowid
        assert new_id is not None and new_id > 0, "insert returned no rowid"
        criterion = get_criterion(conn, new_id)
    assert criterion.task_id == task_id, "criterion attached to wrong task"
    return criterion


def get_criterion(conn: sqlite3.Connection, criterion_id: int) -> Criterion:
    """Fetch one criterion; raises TrackerError if absent."""
    assert criterion_id is not None and criterion_id > 0, f"bad criterion_id {criterion_id}"
    row = conn.execute("SELECT * FROM acceptance_criteria WHERE id = ?", (criterion_id,)).fetchone()
    if row is None:
        raise TrackerError(f"Criterion #{criterion_id} not found")
    return Criterion.from_row(row)


def update_criterion(
    db: TrackerDB,
    criterion_id: int,
    text: str | None = None,
    test_ref: str | None = None,
) -> Criterion:
    """Update a criterion's text and/or test reference."""
    if text is None and test_ref is None:
        raise TrackerError("Nothing to update: pass text or test_ref")
    if text is not None and not text.strip():
        raise TrackerError("Criterion text must not be empty")
    with db.transaction() as conn:
        existing = get_criterion(conn, criterion_id)
        conn.execute(
            "UPDATE acceptance_criteria SET text = COALESCE(?, text), " "test_ref = COALESCE(?, test_ref) WHERE id = ?",
            (text, test_ref, existing.id),
        )
        updated = get_criterion(conn, criterion_id)
    assert updated.id == existing.id, "update changed criterion identity"
    return updated


def record_result(db: TrackerDB, criterion_id: int, result: str) -> Criterion:
    """Record a pass/fail outcome for a criterion (stamps last_run_at)."""
    if result not in RESULTS:
        raise TrackerError(f"Bad result {result!r}; one of: {', '.join(RESULTS)}")
    with db.transaction() as conn:
        existing = get_criterion(conn, criterion_id)
        conn.execute(
            "UPDATE acceptance_criteria SET last_result = ?, last_run_at = ? WHERE id = ?",
            (result, utc_now_iso(), existing.id),
        )
        updated = get_criterion(conn, criterion_id)
    assert updated.last_result == result, "result update did not take"
    assert updated.last_run_at is not None, "last_run_at not stamped"
    return updated


def remove_criterion(db: TrackerDB, criterion_id: int) -> bool:
    """Delete a criterion. Returns True if it existed."""
    assert criterion_id > 0, f"bad criterion_id {criterion_id}"
    with db.transaction() as conn:
        cursor = conn.execute("DELETE FROM acceptance_criteria WHERE id = ?", (criterion_id,))
        removed = cursor.rowcount
    assert removed in (0, 1), f"removed {removed} criteria for one id"
    return removed == 1


def list_criteria(conn: sqlite3.Connection, task_id: int) -> list[Criterion]:
    """All criteria on a task, oldest first."""
    assert task_id > 0, f"bad task_id {task_id}"
    rows = conn.execute(
        "SELECT * FROM acceptance_criteria WHERE task_id = ? ORDER BY id LIMIT ?",
        (task_id, MAX_CRITERIA_PER_TASK),
    ).fetchall()
    criteria = [Criterion.from_row(row) for row in rows]
    assert len(criteria) <= MAX_CRITERIA_PER_TASK, "criteria list over bound"
    return criteria


def evaluate_gate(conn: sqlite3.Connection, task_id: int) -> tuple[list[Criterion], list[Criterion]]:
    """Close-gate check: returns (unmet, unlinked) criteria for a task.

    Unmet = last_result is not 'pass'. Unlinked = no test_ref. A criterion can
    appear in both lists.
    """
    criteria = list_criteria(conn, task_id)
    unmet = [criterion for criterion in criteria if not criterion.is_met]
    unlinked = [criterion for criterion in criteria if not criterion.is_linked]
    assert len(unmet) <= len(criteria), "unmet exceeds total"
    assert len(unlinked) <= len(criteria), "unlinked exceeds total"
    return unmet, unlinked
