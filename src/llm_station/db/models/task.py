from datetime import UTC, datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from ..base import Base


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    key: Mapped[str] = mapped_column(index=True)  # e.g. GRIND-42
    parent_id: Mapped[Optional[str]] = mapped_column(ForeignKey("tasks.id"), nullable=True, index=True)
    depth: Mapped[int] = mapped_column(default=0)  # 0-5, bounded at 6 levels
    kind: Mapped[str] = mapped_column(default="task")  # epic|story|task|subtask|spike|test
    title: Mapped[str]
    description: Mapped[Optional[str]] = mapped_column(nullable=True)
    status: Mapped[str] = mapped_column(default="open")  # open|in_progress|blocked|done|cancelled
    priority: Mapped[Optional[int]] = mapped_column(nullable=True)  # 1-5
    sort_order: Mapped[int] = mapped_column(default=0)
    milestone_id: Mapped[Optional[str]] = mapped_column(ForeignKey("milestones.id"), nullable=True)
    sprint_id: Mapped[Optional[str]] = mapped_column(ForeignKey("sprints.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )
    closed_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)


class TaskAssignee(Base):
    __tablename__ = "task_assignees"

    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"), primary_key=True)
    member_id: Mapped[str] = mapped_column(ForeignKey("members.id"), primary_key=True)
    assigned_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    assigned_by: Mapped[Optional[str]] = mapped_column(ForeignKey("members.id"), nullable=True)


class AcceptanceCriteria(Base):
    __tablename__ = "acceptance_criteria"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"), index=True)
    text: Mapped[str]
    test_ref: Mapped[Optional[str]] = mapped_column(nullable=True)
    last_result: Mapped[Optional[str]] = mapped_column(nullable=True)  # pass|fail
    last_run_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))


class TaskDep(Base):
    __tablename__ = "task_deps"

    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"), primary_key=True)
    depends_on_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))


class Tag(Base):
    __tablename__ = "tags"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id"), index=True)
    name: Mapped[str]


class TaskTag(Base):
    __tablename__ = "task_tags"

    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"), primary_key=True)
    tag_id: Mapped[str] = mapped_column(ForeignKey("tags.id"), primary_key=True)


class TaskCommit(Base):
    __tablename__ = "task_commits"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"), index=True)
    commit_hash: Mapped[str]
    repo_id: Mapped[Optional[str]] = mapped_column(ForeignKey("repos.id"), nullable=True)
    repo_path: Mapped[Optional[str]] = mapped_column(nullable=True)  # fallback if repo not registered
    message_snippet: Mapped[Optional[str]] = mapped_column(nullable=True)
    linked_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
