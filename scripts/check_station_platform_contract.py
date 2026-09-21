"""Exercise StationClient against the real platform router and database service.

Run with llm-station-remote's Python and both repositories' src on PYTHONPATH.
Uses a temporary in-memory database and test identity; never contacts a server.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import llm_station.db.models  # noqa: F401
from devtools_mcp.station.client import StationClient, StationError
from llm_station.auth.deps import AuthContext, get_current_auth
from llm_station.db.base import Base
from llm_station.db.models.org import Org
from llm_station.db.session import get_db
from llm_station.routers.code_graph import router


@asynccontextmanager
async def lifespan(app):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session:
        session.add(Org(id="contract-org", name="Contract", slug="contract", plan="pro"))
        await session.commit()

    async def database():
        async with sessions() as session:
            yield session

    app.dependency_overrides[get_db] = database
    try:
        yield
    finally:
        await engine.dispose()


def main():
    app = FastAPI(lifespan=lifespan)
    app.include_router(router)
    app.dependency_overrides[get_current_auth] = lambda: AuthContext("test", "contract-org", "test-key")
    with TestClient(app) as http:
        client = StationClient("http://testserver", "lls_contract", "contract-org", client=http)
        body = '{"nodes":[],"edges":[]}'
        first = client.code_graph_upload({"project": "project-a", "body": body})
        second = client.code_graph_upload({"project": "project-b", "body": body})
        updated = client.code_graph_upload({"project": "project-a", "body": body, "node_count": 2})
        assert first["id"] != second["id"]
        assert updated["id"] == first["id"] and updated["node_count"] == 2
        client.org_id = "wrong-org"
        try:
            client.code_graph_upload({"project": "project-a", "body": body})
        except StationError as exc:
            assert exc.status_code == 403
        else:
            raise AssertionError("Cross-org graph upload accepted")
    print("PASS: real platform graph create/update/project isolation/org guard")


if __name__ == "__main__":
    main()
