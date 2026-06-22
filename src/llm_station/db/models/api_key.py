from datetime import UTC, datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column

from ..base import Base


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid4()))
    member_id: Mapped[str] = mapped_column(ForeignKey("members.id"), index=True)
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id"), index=True)
    key_hash: Mapped[str] = mapped_column(unique=True)  # SHA-256; raw key shown once on creation
    name: Mapped[Optional[str]] = mapped_column(nullable=True)
    scopes: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)  # ["tasks:write","runs:read"]
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    last_used_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    revoked_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
