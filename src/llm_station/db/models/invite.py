from datetime import UTC, datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from ..base import Base


class Invite(Base):
    __tablename__ = "invites"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    # id doubles as the invite token shown to the recipient
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id"), index=True)
    email: Mapped[Optional[str]] = mapped_column(nullable=True)
    agent_model: Mapped[Optional[str]] = mapped_column(nullable=True)
    hostname: Mapped[Optional[str]] = mapped_column(nullable=True)
    invited_by: Mapped[str] = mapped_column(ForeignKey("members.id"))
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    expires_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    accepted_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    accepted_by: Mapped[Optional[str]] = mapped_column(ForeignKey("members.id"), nullable=True)


class InviteProjectRole(Base):
    __tablename__ = "invite_project_roles"

    invite_id: Mapped[str] = mapped_column(ForeignKey("invites.id"), primary_key=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"), primary_key=True)
    role: Mapped[str]  # owner|admin|contributor|observer
