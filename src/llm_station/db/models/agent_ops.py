from datetime import UTC, datetime

from sqlalchemy import ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column

from ..base import Base


class AgentOp(Base):
    """CRDT op-log hub: local agents push mutations here; peers pull to converge."""
    __tablename__ = "agent_ops"

    seq: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    org_id: Mapped[str] = mapped_column(ForeignKey("orgs.id"), index=True)
    site_id: Mapped[str] = mapped_column(index=True)  # machine UUID from local_site table
    hlc: Mapped[str]  # "siteUUID:wallMs:counter" — hybrid logical clock for causality
    tbl: Mapped[str]  # local cache table name: "my_checkouts" | "my_session"
    row_id: Mapped[str]
    op: Mapped[str]  # "upsert" | "delete"
    payload: Mapped[dict] = mapped_column(JSON)
    ts: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC), index=True)
