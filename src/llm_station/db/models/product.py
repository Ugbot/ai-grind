from datetime import UTC, datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from ..base import Base


class Milestone(Base):
    __tablename__ = "milestones"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    title: Mapped[str]
    target_date: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    description: Mapped[Optional[str]] = mapped_column(nullable=True)
    closed: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))


class Sprint(Base):
    __tablename__ = "sprints"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), index=True)
    title: Mapped[str]
    start_date: Mapped[datetime]
    end_date: Mapped[datetime]
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
