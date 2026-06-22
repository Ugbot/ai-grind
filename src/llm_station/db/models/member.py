from datetime import UTC, datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from ..base import Base


class Member(Base):
    __tablename__ = "members"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str]
    type: Mapped[str]  # human|agent|machine
    email: Mapped[Optional[str]] = mapped_column(nullable=True)
    agent_model: Mapped[Optional[str]] = mapped_column(nullable=True)
    hostname: Mapped[Optional[str]] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))


class AgentSponsor(Base):
    __tablename__ = "agent_sponsors"

    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id"), primary_key=True)
    agent_id: Mapped[str] = mapped_column(ForeignKey("members.id"), primary_key=True)
    sponsor_id: Mapped[str] = mapped_column(ForeignKey("members.id"), primary_key=True)
    granted_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    granted_by: Mapped[Optional[str]] = mapped_column(ForeignKey("members.id"), nullable=True)
