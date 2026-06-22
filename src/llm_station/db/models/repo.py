from datetime import UTC, datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column

from ..base import Base


class Repo(Base):
    __tablename__ = "repos"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    remote_url: Mapped[str] = mapped_column(unique=True)  # canonical identity
    name: Mapped[str]
    stack: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)  # ["python","powershell"]
    description: Mapped[Optional[str]] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))


class RepoPath(Base):
    """Machine-local path for a repo — one repo at different paths on different machines."""
    __tablename__ = "repo_paths"

    repo_id: Mapped[str] = mapped_column(ForeignKey("repos.id"), primary_key=True)
    machine_id: Mapped[str] = mapped_column(ForeignKey("members.id"), primary_key=True)
    path: Mapped[str]


class ProjectRepo(Base):
    __tablename__ = "project_repos"

    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), primary_key=True)
    repo_id: Mapped[str] = mapped_column(ForeignKey("repos.id"), primary_key=True)
    added_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    added_by: Mapped[Optional[str]] = mapped_column(ForeignKey("members.id"), nullable=True)
