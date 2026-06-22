from datetime import UTC, datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column

from ..base import Base


class Skill(Base):
    __tablename__ = "skills"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    org_id: Mapped[Optional[str]] = mapped_column(ForeignKey("orgs.id"), nullable=True, index=True)
    # NULL org_id = global platform skill; SET = org-private
    name: Mapped[str] = mapped_column(index=True)
    type: Mapped[str]  # skill|command|agent|archive
    category: Mapped[Optional[str]] = mapped_column(nullable=True)
    description: Mapped[Optional[str]] = mapped_column(nullable=True)
    body: Mapped[Optional[str]] = mapped_column(nullable=True)  # full SKILL.md content
    platforms: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)  # ["win32","linux"]
    requires_tools: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)  # ["valgrind"]
    project_types: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)  # ["python","cpp"]
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )
