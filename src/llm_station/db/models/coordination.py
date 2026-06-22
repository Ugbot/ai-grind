from datetime import UTC, datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column

from ..base import Base


class WorkSession(Base):
    # Named WorkSession to avoid shadowing sqlalchemy's Session
    __tablename__ = "work_sessions"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    member_id: Mapped[str] = mapped_column(ForeignKey("members.id"), index=True)
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id"), index=True)
    project_id: Mapped[Optional[str]] = mapped_column(ForeignKey("projects.id"), nullable=True)
    task_key: Mapped[Optional[str]] = mapped_column(nullable=True)
    status: Mapped[str] = mapped_column(default="active")  # active|paused|completed|handed_off
    summary: Mapped[Optional[str]] = mapped_column(nullable=True)
    context: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    started_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    ended_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)


class Handoff(Base):
    __tablename__ = "handoffs"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    from_member_id: Mapped[str] = mapped_column(ForeignKey("members.id"), index=True)
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id"), index=True)
    project_id: Mapped[Optional[str]] = mapped_column(ForeignKey("projects.id"), nullable=True)
    task_key: Mapped[Optional[str]] = mapped_column(nullable=True)
    context: Mapped[str]  # markdown: what was tried, what was found
    next_steps: Mapped[str]  # markdown: what should happen next
    artifact_run_ids: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)
    status: Mapped[str] = mapped_column(default="pending")  # pending|accepted|declined|expired
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    accepted_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)


class HandoffRecipient(Base):
    __tablename__ = "handoff_recipients"

    handoff_id: Mapped[str] = mapped_column(ForeignKey("handoffs.id"), primary_key=True)
    member_id: Mapped[Optional[str]] = mapped_column(ForeignKey("members.id"), nullable=True)
    role: Mapped[Optional[str]] = mapped_column(nullable=True)  # role-targeted fallback
    # Constraint: exactly one of member_id or role must be set — enforced at service layer


class Event(Base):
    __tablename__ = "events"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id"), index=True)
    project_id: Mapped[Optional[str]] = mapped_column(ForeignKey("projects.id"), nullable=True, index=True)
    actor_id: Mapped[Optional[str]] = mapped_column(ForeignKey("members.id"), nullable=True)
    event_type: Mapped[str] = mapped_column(index=True)  # task.created, handoff.sent, ...
    target_type: Mapped[Optional[str]] = mapped_column(nullable=True)
    target_id: Mapped[Optional[str]] = mapped_column(nullable=True)
    summary: Mapped[str]
    detail: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    ts: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC), index=True)


class Comment(Base):
    __tablename__ = "comments"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id"), index=True)
    target_type: Mapped[str]  # task|run|handoff|work_session
    target_id: Mapped[str]
    member_id: Mapped[str] = mapped_column(ForeignKey("members.id"))
    body: Mapped[str]
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    edited_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)


class Checkout(Base):
    __tablename__ = "checkouts"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id"), index=True)
    repo_id: Mapped[str] = mapped_column(ForeignKey("repos.id"), index=True)
    member_id: Mapped[str] = mapped_column(ForeignKey("members.id"), index=True)
    session_id: Mapped[Optional[str]] = mapped_column(ForeignKey("work_sessions.id"), nullable=True)
    task_key: Mapped[Optional[str]] = mapped_column(nullable=True)
    path: Mapped[str]  # repo-relative path
    path_type: Mapped[str] = mapped_column(default="file")  # file|directory|glob
    line_start: Mapped[Optional[int]] = mapped_column(nullable=True)
    line_end: Mapped[Optional[int]] = mapped_column(nullable=True)
    mode: Mapped[str] = mapped_column(default="exclusive")  # exclusive|shared
    intent: Mapped[Optional[str]] = mapped_column(nullable=True)
    checked_out_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    heartbeat_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    released_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    transferred_from: Mapped[Optional[str]] = mapped_column(ForeignKey("members.id"), nullable=True)
