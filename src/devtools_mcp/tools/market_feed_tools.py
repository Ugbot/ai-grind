"""One callable for a configured local native synthetic-feed developer harness."""

from __future__ import annotations

import os
import re
from typing import Any, Literal

from devtools_mcp.market_feed import MarketFeedManager
from devtools_mcp.market_feed.manager import error
from devtools_mcp.server import mcp

_manager: MarketFeedManager | None = None


def _harness() -> MarketFeedManager:
    global _manager
    if _manager is None:
        _manager = MarketFeedManager(os.environ.get("DEVTOOLS_MARKET_FEED_BINARY"))
    return _manager


async def close_market_feeds() -> None:
    """Called by the established MCP lifespan shutdown hook."""
    global _manager
    current = _manager
    if current is not None:
        await current.close()
        if _manager is current:
            _manager = None


@mcp.tool()
async def devtools_market_feed(
    action: Literal["start", "control", "status", "stop", "list"],
    run_id: str = "",
    config: dict[str, Any] | None = None,
    command: str = "",
) -> dict[str, Any]:
    """Control chukonu_market_feed locally; synthetic/unpersisted, not a platform job.

    Configure the exact absolute executable once via DEVTOOLS_MARKET_FEED_BINARY.
    start accepts symbols/depth/seed/rate/batch/burst/seconds/max_messages/memory_mib/
    unlimited. Every run is finite; named server ceilings default to3600s/512MiB. Controls: rate N, ramp N MS,
    burst N, pause, resume, unlimited, book ID, status, stop. No shell or per-row IPC;
    Python never generates/decode/computes market events. Four active runs, sixteen
    retained summaries, bounded NDJSON telemetry and eight command receipts/run.
    status reads cached native counters; control command="status" requests refresh.
    Command timeout is uncertain: wait for its receipt, never blindly resend.
    stop/EOF asks graceful shutdown then bounded terminate/kill. MCP shutdown also
    cleans up owned producers. Local developer harness only, no durable ACK claim.
    """
    if action not in {"start", "control", "status", "stop", "list"}:
        return error("invalid_action", "unsupported harness action")
    if action in {"start", "list"}:
        if run_id or command or (action == "list" and config is not None):
            return error("invalid_input", "unexpected parameters for start/list")
    else:
        if not isinstance(run_id, str) or re.fullmatch(r"[0-9a-f]{32}", run_id) is None or config is not None:
            return error("invalid_input", "owned run ID required; config only applies to start")
        if action != "control" and command:
            return error("invalid_input", "command only applies to control")
    try:
        manager = _harness()
    except ValueError as exc:
        return error("invalid_server_config", str(exc))
    if action == "start":
        return await manager.start(config)
    if action == "list":
        return manager.list()
    if action == "status":
        return manager.status(run_id)
    if action == "stop":
        return await manager.stop(run_id)
    return await manager.control(run_id, command)
