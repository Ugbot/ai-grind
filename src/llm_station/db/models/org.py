from datetime import UTC, datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from ..base import Base


class Org(Base):
    __tablename__ = "orgs"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    name: Mapped[str]
    slug: Mapped[str] = mapped_column(unique=True)
    plan: Mapped[str] = mapped_column(default="free")
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))


class OrgMember(Base):
    __tablename__ = "org_members"

    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id"), primary_key=True)
    member_id: Mapped[str] = mapped_column(ForeignKey("members.id"), primary_key=True)
    role: Mapped[str]  # owner|admin|contributor|observer
    joined_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    left_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    invited_by: Mapped[Optional[str]] = mapped_column(ForeignKey("members.id"), nullable=True)
