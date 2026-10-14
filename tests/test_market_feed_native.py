"""Opt-in maintained real-native acceptance through the registered MCP callable.

Set DEVTOOLS_MARKET_FEED_BINARY to the built chukonu_market_feed executable.
This test does not build or regenerate market events in Python.
"""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

pytestmark = pytest.mark.skipif(
    not os.environ.get("DEVTOOLS_MARKET_FEED_BINARY"),
    reason="set DEVTOOLS_MARKET_FEED_BINARY for real-native acceptance",
)


@pytest.fixture
def native_session(monkeypatch):
    from devtools_mcp import server
    from devtools_mcp.tools import market_feed_tools as tools

    binary = Path(os.environ["DEVTOOLS_MARKET_FEED_BINARY"])
    assert binary.is_absolute() and binary.is_file(), "configured native binary must already be built"
    monkeypatch.setattr(tools, "_manager", None)
    monkeypatch.setenv("DEVTOOLS_MARKET_FEED_MAX_MEMORY_MIB", "1024")

    class Context:
        registry = None
        debug_manager = None

        def cleanup_all(self):
            pass

    class Registry:
        async def detect_all(self):
            return None

    # Exercise the real registered MCP dispatch/lifespan without starting DBOS,
    # profiling discovery or a dashboard unrelated to this developer harness.
    monkeypatch.setattr(server, "_EAGER_CTX", None)
    monkeypatch.setattr(server, "_bootstrap_ctx", Context)
    monkeypatch.setattr(server, "ToolRegistry", Registry)
    monkeypatch.setattr(server, "_launch_dbos", lambda: None)
    monkeypatch.setattr(server, "_maybe_start_dashboard", lambda _: None)

    @asynccontextmanager
    async def connect():
        async with create_connected_server_and_client_session(server.mcp, raise_exceptions=True) as session:
            inventory = await session.list_tools()
            assert "devtools_market_feed" in {tool.name for tool in inventory.tools}
            yield session
        assert tools._manager is None  # Established lifespan closed and reset it.

    return connect


async def call(session, action: str, **params):
    result = await asyncio.wait_for(session.call_tool("devtools_market_feed", {"action": action, **params}), 15)
    assert not result.isError
    data = result.structuredContent
    if data is None:
        data = json.loads(result.content[0].text)
    assert isinstance(data, dict) and "status" in data
    return data


async def payload_at_revision(session, run_id: str, kind: str, revision: int):
    for _ in range(300):  # 3s bounded telemetry wait, never generation/pacing.
        result = await call(session, "status", run_id=run_id)
        assert result["status"] == "ok", result
        snapshot = result["data"]
        assert snapshot["state"] != "failed", snapshot
        payload = snapshot[kind]
        if payload is not None and payload["revision"] >= revision:
            return payload
        await asyncio.sleep(0.01)
    pytest.fail(f"native {kind} did not reach command revision{revision}")


async def refresh(session, run_id: str):
    reply = await call(session, "control", run_id=run_id, command="status")
    assert reply["status"] == "ok", reply
    return await payload_at_revision(session, run_id, "telemetry", reply["receipt"]["revision"])


async def test_registered_real_native_paused_rate_ramp_unlimited_book_stop(native_session):
    async with native_session() as session:
        result = await call(
            session,
            "start",
            config={
                "symbols": 48,
                "depth": 4,
                "rate": 0,
                "seed": 18446744073709551615,
                "seconds": 15,
                "memory_mib": 256,
            },
        )
        assert result["status"] == "ok", result
        run_id = result["data"]["run_id"]
        try:
            initial = result["data"]["telemetry"]
            assert initial["paused"] and initial["emitted_messages"] == 0
            definition = result["data"]["definition"]
            assert definition["symbols"] == 48 and len(definition["families"]) == 12
            assert definition["seed"] == 18446744073709551615
            assert definition["kinds"] == ["future", "calendar", "call", "put"]
            previous_revision = 0
            for command in ("rate 6000", "ramp 12000 200", "unlimited"):
                reply = await call(session, "control", run_id=run_id, command=command)
                assert reply["status"] == "ok", reply
                assert reply["receipt"]["command"] == command
                assert reply["receipt"]["revision"] > previous_revision
                previous_revision = reply["receipt"]["revision"]
            telemetry = await refresh(session, run_id)
            assert telemetry["unlimited"] and not telemetry["paused"]
            assert telemetry["processed_rows"] == telemetry["source_rows"] == telemetry["emitted_messages"]
            assert telemetry["achieved_messages_per_second"] >= 0
            # Wait for all initial replacement events, then inspect the last active
            # symbol (a synthetic put) at the native ACK's exact telemetry revision.
            for _ in range(100):
                if telemetry["book_events"] >= 48:
                    break
                await asyncio.sleep(0.01)
                telemetry = await refresh(session, run_id)
            assert telemetry["book_events"] >= 48
            reply = await call(session, "control", run_id=run_id, command="book 48")
            assert reply["status"] == "ok", reply
            book = await payload_at_revision(session, run_id, "book", reply["receipt"]["revision"])
            assert book["symbol_id"] == 48 and book["kind"] == "put" and book["valid"]
            assert book["price_scale"] > 0 and book["quantity_scale"] > 0
            assert book["bid"] and book["ask"]
            snapshot = await call(session, "status", run_id=run_id)
            assert snapshot["data"]["mode"] == "synthetic_unpersisted_native_local"
            assert snapshot["data"]["durable"] is False and snapshot["data"]["platform_job"] is False
        finally:
            stopped = await call(session, "stop", run_id=run_id)
        assert stopped["data"]["state"] == "finished", stopped
        assert stopped["data"]["exit_code"] == 0
        assert stopped["data"]["final"]["processed_rows"] == stopped["data"]["final"]["source_rows"]


async def test_registered_real_native_many_symbols_memory_admission_and_refusal(native_session):
    async with native_session() as session:
        admitted = await call(
            session, "start", config={"symbols": 100_000, "depth": 1, "rate": 0, "seconds": 15, "memory_mib": 512}
        )
        assert admitted["status"] == "ok", admitted
        run_id = admitted["data"]["run_id"]
        try:
            definition = admitted["data"]["definition"]
            assert definition["symbols"] == 100_000
            assert definition["startup_storage_bytes"] <= definition["memory_budget_bytes"]
            reply = await call(session, "control", run_id=run_id, command="book 100000")
            assert reply["status"] == "ok", reply
            book = await payload_at_revision(session, run_id, "book", reply["receipt"]["revision"])
            assert book["symbol_id"] == 100_000 and book["valid"] is False  # Still paused, no snapshot fabricated.
        finally:
            stopped = await call(session, "stop", run_id=run_id)
        assert stopped["data"]["state"] == "finished", stopped
        refused = await call(
            session, "start", config={"symbols": 100_000, "depth": 64, "rate": 0, "seconds": 15, "memory_mib": 1}
        )
        assert refused["status"] == "error" and refused["code"] == "startup_failed", refused
        assert refused["data"]["exit_code"] is not None
        assert "native_error" in refused["data"]["failure"]
        oversized_batch = await call(
            session, "start", config={"batch": 0xFFFFFFFF, "rate": 0, "seconds": 15, "memory_mib": 512}
        )
        assert oversized_batch["status"] == "error" and oversized_batch["code"] == "startup_failed", oversized_batch
        assert oversized_batch["data"]["exit_code"] is not None
        assert "native_error" in oversized_batch["data"]["failure"]
        inventory = await call(session, "list")
        assert all(run["state"] in {"finished", "failed"} for run in inventory["runs"])
