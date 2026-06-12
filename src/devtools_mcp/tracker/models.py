"""Pydantic models for tracker rows.

Each model mirrors one table; `from_row` builds a model from a sqlite3.Row.
Validation constants (statuses, kinds, policies) live here so the domain
layer and the MCP tools share one vocabulary.
"""

from __future__ import annotations

import sqlite3

from pydantic import BaseModel

STATUSES: tuple[str, ...] = ("open", "in_progress", "blocked", "done", "cancelled")
CLOSED_STATUSES: tuple[str, ...] = ("done", "cancelled")
POLICIES: tuple[str, ...] = ("advisory", "strict")
KNOWN_KINDS: tuple[str, ...] = ("epic", "story", "task", "subtask", "spike", "test")
MAX_DEPTH: int = 5  # 6 levels, 0-indexed
PRIORITY_MIN: int = 1
PRIORITY_MAX: int = 5


class Project(BaseModel):
    id: int
    key: str
    name: str
    description: str = ""
    close_policy: str = "advisory"
    next_seq: int = 1
    created_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Project:
        assert row is not None, "Project.from_row given None"
        project = cls(**dict(row))
        assert project.close_policy in POLICIES, f"bad policy in db: {project.close_policy}"
        return project


class Task(BaseModel):
    id: int
    project_id: int
    key: str
    parent_id: int | None = None
    depth: int = 0
    kind: str = "task"
    title: str
    description: str = ""
    status: str = "open"
    priority: int = 3
    sort_order: float = 0.0
    created_at: str
    updated_at: str
    closed_at: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Task:
        assert row is not None, "Task.from_row given None"
        task = cls(**dict(row))
        assert task.status in STATUSES, f"bad status in db: {task.status}"
        assert 0 <= task.depth <= MAX_DEPTH, f"bad depth in db: {task.depth}"
        return task


class Criterion(BaseModel):
    id: int
    task_id: int
    text: str
    test_ref: str | None = None
    last_result: str | None = None
    last_run_at: str | None = None
    created_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Criterion:
        assert row is not None, "Criterion.from_row given None"
        criterion = cls(**dict(row))
        assert criterion.last_result in (None, "pass", "fail"), f"bad result in db: {criterion.last_result}"
        return criterion

    @property
    def is_met(self) -> bool:
        return self.last_result == "pass"

    @property
    def is_linked(self) -> bool:
        return bool(self.test_ref)


class CommitLink(BaseModel):
    id: int
    task_id: int
    commit_hash: str
    repo_path: str
    message_snippet: str = ""
    linked_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> CommitLink:
        assert row is not None, "CommitLink.from_row given None"
        link = cls(**dict(row))
        assert link.commit_hash, "empty commit hash in db"
        return link


class TagRule(BaseModel):
    id: int
    project_id: int | None = None
    tag_id: int
    match_kind: str | None = None
    match_regex: str | None = None
    match_parent_kind: str | None = None
    enabled: int = 1
    created_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> TagRule:
        assert row is not None, "TagRule.from_row given None"
        rule = cls(**dict(row))
        assert rule.enabled in (0, 1), f"bad enabled flag in db: {rule.enabled}"
        return rule


class ExternalRef(BaseModel):
    task_id: int
    provider: str
    ref_id: str
    repo: str
    url: str
    state: str | None = None
    last_synced: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> ExternalRef:
        assert row is not None, "ExternalRef.from_row given None"
        ref = cls(**dict(row))
        assert ref.provider, "empty provider in db"
        return ref
