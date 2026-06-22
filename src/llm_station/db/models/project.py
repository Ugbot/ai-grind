from datetime import UTC, datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from ..base import Base


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id"), index=True)
    key: Mapped[str]  # e.g. GRIND — unique within org enforced at service layer
    name: Mapped[str]
    description: Mapped[Optional[str]] = mapped_column(nullable=True)
    close_policy: Mapped[str] = mapped_column(default="advisory")  # advisory|strict
    next_seq: Mapped[int] = mapped_column(default=1)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))


class ProjectMember(Base):
    __tablename__ = "project_members"

    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), primary_key=True)
    member_id: Mapped[str] = mapped_column(ForeignKey("members.id"), primary_key=True)
    role: Mapped[str]  # owner|admin|contributor|observer
    granted_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    granted_by: Mapped[Optional[str]] = mapped_column(ForeignKey("members.id"), nullable=True)
