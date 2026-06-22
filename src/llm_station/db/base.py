from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


def make_engine(database_url: str) -> AsyncEngine:
    assert database_url, "database_url must be non-empty"
    return create_async_engine(database_url, echo=False, pool_pre_ping=True)
