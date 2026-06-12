"""Tags, task-tag assignment, and the auto-tagging rules engine.

Rules match on task kind, a regex over title+description, and/or the parent
task's kind. NULL fields match anything. Rules are validated at insertion
(regex must compile); a rule whose stored regex no longer compiles is skipped
at apply time (runtime condition, not an invariant).

Low-level helpers take a live connection so they compose inside a caller's
transaction; public mutators own their transaction via TrackerDB.transaction().
"""

from __future__ import annotations

import re
import sqlite3

from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso
from devtools_mcp.tracker.models import KNOWN_KINDS, TagRule, Task

MAX_RULES: int = 500
MAX_TAGS_PER_TASK: int = 100
TAG_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,49}$")


def normalize_tag(name: str) -> str:
    """Normalize a tag name to lowercase; reject malformed names."""
    assert isinstance(name, str), f"tag name must be str, got {type(name)}"
    normalized = name.strip().lower().replace(" ", "-")
    if not TAG_NAME_RE.match(normalized):
        raise TrackerError(f"Bad tag name {name!r}: must be 1-50 chars of [a-z0-9_.-], starting alphanumeric")
    assert normalized == normalized.lower(), "normalization failed"
    return normalized


def ensure_tag(conn: sqlite3.Connection, name: str) -> int:
    """Get-or-create a tag row; returns tag id. Must run inside a transaction."""
    normalized = normalize_tag(name)
    conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (normalized,))
    row = conn.execute("SELECT id FROM tags WHERE name = ?", (normalized,)).fetchone()
    assert row is not None, f"tag {normalized!r} missing after insert"
    assert row[0] > 0, "tag id must be positive"
    return row[0]


def add_tag(db: TrackerDB, task_id: int, name: str) -> str:
    """Attach a tag to a task. Returns the normalized name."""
    assert task_id > 0, f"bad task_id {task_id}"
    with db.transaction() as conn:
        tag_id = ensure_tag(conn, name)
        conn.execute(
            "INSERT OR IGNORE INTO task_tags (task_id, tag_id) VALUES (?, ?)",
            (task_id, tag_id),
        )
        normalized = conn.execute("SELECT name FROM tags WHERE id = ?", (tag_id,)).fetchone()[0]
    assert normalized, "tag name empty after add"
    return normalized


def remove_tag(db: TrackerDB, task_id: int, name: str) -> bool:
    """Detach a tag from a task. Returns True if a link was removed."""
    assert task_id > 0, f"bad task_id {task_id}"
    normalized = normalize_tag(name)
    with db.transaction() as conn:
        cursor = conn.execute(
            "DELETE FROM task_tags WHERE task_id = ? AND " "tag_id = (SELECT id FROM tags WHERE name = ?)",
            (task_id, normalized),
        )
        removed = cursor.rowcount
    assert removed in (0, 1), f"removed {removed} links for one (task, tag) pair"
    return removed == 1


def tags_for_task(conn: sqlite3.Connection, task_id: int) -> list[str]:
    """All tag names on a task, sorted."""
    assert task_id > 0, f"bad task_id {task_id}"
    rows = conn.execute(
        "SELECT t.name FROM tags t JOIN task_tags tt ON tt.tag_id = t.id "
        "WHERE tt.task_id = ? ORDER BY t.name LIMIT ?",
        (task_id, MAX_TAGS_PER_TASK),
    ).fetchall()
    names = [row[0] for row in rows]
    assert len(names) <= MAX_TAGS_PER_TASK, "tag list over bound"
    return names


def add_rule(
    db: TrackerDB,
    tag_name: str,
    project_id: int | None = None,
    match_kind: str | None = None,
    match_regex: str | None = None,
    match_parent_kind: str | None = None,
) -> int:
    """Create an auto-tagging rule. Returns the rule id."""
    if match_kind is None and match_regex is None and match_parent_kind is None:
        raise TrackerError("A tag rule needs at least one condition (kind/regex/parent kind)")
    if match_regex is not None:
        try:
            re.compile(match_regex)
        except re.error as exc:
            raise TrackerError(f"Bad rule regex {match_regex!r}: {exc}") from exc
    for kind in (match_kind, match_parent_kind):
        if kind is not None and kind not in KNOWN_KINDS:
            raise TrackerError(f"Unknown kind {kind!r}; known: {', '.join(KNOWN_KINDS)}")
    with db.transaction() as conn:
        count = conn.execute("SELECT COUNT(*) FROM tag_rules").fetchone()[0]
        if count >= MAX_RULES:
            raise TrackerError(f"Rule limit reached ({MAX_RULES})")
        tag_id = ensure_tag(conn, tag_name)
        cursor = conn.execute(
            "INSERT INTO tag_rules (project_id, tag_id, match_kind, match_regex, "
            "match_parent_kind, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (project_id, tag_id, match_kind, match_regex, match_parent_kind, utc_now_iso()),
        )
        rule_id = cursor.lastrowid
    assert rule_id is not None and rule_id > 0, "rule insert returned no id"
    return rule_id


def list_rules(conn: sqlite3.Connection, project_id: int | None = None) -> list[dict]:
    """Rules visible to a project (its own + global), or all rules if project_id is None."""
    if project_id is None:
        rows = conn.execute(
            "SELECT r.*, t.name AS tag_name FROM tag_rules r JOIN tags t ON t.id = r.tag_id " "ORDER BY r.id LIMIT ?",
            (MAX_RULES,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT r.*, t.name AS tag_name FROM tag_rules r JOIN tags t ON t.id = r.tag_id "
            "WHERE r.project_id IS NULL OR r.project_id = ? ORDER BY r.id LIMIT ?",
            (project_id, MAX_RULES),
        ).fetchall()
    assert len(rows) <= MAX_RULES, "rule list over bound"
    return [dict(row) for row in rows]


def remove_rule(db: TrackerDB, rule_id: int) -> bool:
    """Delete a rule. Returns True if it existed."""
    assert rule_id > 0, f"bad rule_id {rule_id}"
    with db.transaction() as conn:
        cursor = conn.execute("DELETE FROM tag_rules WHERE id = ?", (rule_id,))
        removed = cursor.rowcount
    assert removed in (0, 1), f"removed {removed} rules for one id"
    return removed == 1


def _rule_matches(rule: TagRule, task: Task, parent_kind: str | None) -> bool:
    """Evaluate one rule against a task. All non-NULL conditions must hold."""
    assert rule.enabled == 1, "disabled rule reached matcher"
    assert task.id > 0, "task must be persisted before rule matching"
    if rule.match_kind is not None and task.kind != rule.match_kind:
        return False
    if rule.match_parent_kind is not None and parent_kind != rule.match_parent_kind:
        return False
    if rule.match_regex is not None:
        try:
            pattern = re.compile(rule.match_regex)
        except re.error:
            return False  # stored regex went bad; skip, never crash creation
        if not pattern.search(task.title + "\n" + task.description):
            return False
    return True


def apply_rules(conn: sqlite3.Connection, task: Task, parent_kind: str | None) -> list[str]:
    """Apply matching enabled rules to a freshly created task. Returns tag names applied.

    Runs inside the caller's create-task transaction.
    """
    assert task.id > 0, "apply_rules needs a persisted task"
    rows = conn.execute(
        "SELECT r.*, t.name AS tag_name FROM tag_rules r JOIN tags t ON t.id = r.tag_id "
        "WHERE r.enabled = 1 AND (r.project_id IS NULL OR r.project_id = ?) "
        "ORDER BY r.id LIMIT ?",
        (task.project_id, MAX_RULES),
    ).fetchall()
    applied: list[str] = []
    for row in rows:  # bounded by MAX_RULES via LIMIT above
        rule = TagRule.from_row(row)
        if not _rule_matches(rule, task, parent_kind):
            continue
        conn.execute(
            "INSERT OR IGNORE INTO task_tags (task_id, tag_id) VALUES (?, ?)",
            (task.id, rule.tag_id),
        )
        applied.append(row["tag_name"])
    assert len(applied) <= len(rows), "applied more tags than rules"
    return sorted(set(applied))
