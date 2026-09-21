"""Explicit local Station MCP adapter. Never retries capture or changes engines."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from pydantic import BaseModel, Field

from devtools_mcp.models import StackSample
from devtools_mcp.node.models import NodeResult
from devtools_mcp.store.paths import data_root

MAX_ARTIFACT_BYTES = 64 * 1024 * 1024


class CoreEnvelope(BaseModel):
    protocol_version: Literal[1]
    engine: Literal["llm-station"]
    run_id: str = Field(pattern=r"^station-[A-Za-z0-9_-]+$")
    suite: Literal["node"]
    tool: Literal["cpu"]
    binary: str
    args: list[str]
    workspace_id: str
    duration_seconds: float = Field(ge=0)
    exit_code: int
    raw_path: str
    stack_samples: list[StackSample] | None = None
    stack_samples_path: str = ""
    total_weight: int = Field(ge=0)
    timestamp_ms: int = Field(ge=0)
    tool_version: str = ""
    git_commit: str = ""
    git_branch: str = ""
    git_dirty: bool = False


def _artifact(path: str, root: Path) -> Path:
    resolved = Path(path).resolve()
    if not resolved.is_relative_to(root.resolve()) or not resolved.is_file():
        raise ValueError("Station artifact is missing or outside its configured data directory")
    if resolved.stat().st_size > MAX_ARTIFACT_BYTES:
        raise ValueError("Station artifact exceeds the 64 MiB import limit")
    return resolved


def import_envelope(payload: dict, root: Path) -> tuple[NodeResult, str]:
    """Validate provenance and immutable local artifacts before adopting a run."""
    envelope = CoreEnvelope.model_validate(payload)
    raw = _artifact(envelope.raw_path, root)
    samples = envelope.stack_samples
    if samples is None:
        path = _artifact(envelope.stack_samples_path, root)
        with path.open("rb") as source:
            encoded = source.read(MAX_ARTIFACT_BYTES + 1)
        if len(encoded) > MAX_ARTIFACT_BYTES:
            raise ValueError("Station stack artifact exceeds the import limit")
        samples = [StackSample.model_validate(item) for item in json.loads(encoded)]
    if any(sample.weight < 0 or not sample.frames for sample in samples):
        raise ValueError("Station returned invalid weighted stacks")
    if sum(sample.weight for sample in samples) != envelope.total_weight:
        raise ValueError("Station sample total does not match its manifest")
    result = NodeResult(
        run_id=envelope.run_id,
        timestamp=datetime.fromtimestamp(envelope.timestamp_ms / 1000, tz=UTC),
        tool_version=envelope.tool_version,
        git_commit=envelope.git_commit,
        git_branch=envelope.git_branch,
        git_dirty=envelope.git_dirty,
        binary=envelope.binary,
        tool="cpu",
        args=envelope.args,
        exit_code=envelope.exit_code,
        duration_seconds=envelope.duration_seconds,
        cwd=envelope.workspace_id,
        stack_samples=samples,
        total_weight=envelope.total_weight,
        profile_path=str(raw),
        stack_format="v8-full-v1",
        engine="llm-station",
        engine_protocol=1,
        engine_workspace=envelope.workspace_id,
    )
    return result, str(raw)


async def run_station_node(
    tool: str,
    binary: str,
    args: list[str] | None,
    extra_args: list[str] | None,
    timeout: int,
    working_dir: str = "",
    env: dict[str, str] | None = None,
) -> tuple[str | None, NodeResult | None, str]:
    if tool != "cpu":
        return "Station core supports node:cpu only; explicitly select python for other verbs.", None, ""
    executable = os.environ.get("DEVTOOLS_MCP_STATION_CORE", "").strip()
    if not executable:
        return "Set DEVTOOLS_MCP_STATION_CORE to the station-core executable.", None, ""
    if not 1 <= timeout <= 3600:
        return "timeout must be between 1 and 3600 seconds", None, ""
    script = Path(binary).resolve()
    cwd = Path(working_dir).resolve() if working_dir else script.parent
    root = (data_root() / "station-core").resolve()
    params = StdioServerParameters(
        command=executable,
        args=["--data-dir", str(root), "--workspace", str(cwd)],
        env=dict(os.environ),
    )
    try:
        async with (
            stdio_client(params) as (reader, writer),
            ClientSession(reader, writer, read_timeout_seconds=timedelta(seconds=timeout + 15)) as session,
        ):
            await session.initialize()
            caps = await session.call_tool("core_capabilities", {})
            capability = caps.structuredContent or {}
            if caps.isError or capability.get("protocol_version") != 1 or capability.get("engine") != "llm-station":
                raise ValueError("incompatible Station core; protocol 1 is required")
            if "node.cpu" not in capability.get("supported", []):
                raise ValueError("Station core does not advertise node.cpu support")
            reply = await session.call_tool(
                "core_node_capture",
                {
                    "protocol_version": 1,
                    "script": str(script),
                    "args": args or [],
                    "node_args": extra_args or [],
                    "cwd": str(cwd),
                    "env": env or {},
                    "timeout_seconds": timeout,
                },
            )
            if reply.isError:
                message = " ".join(getattr(item, "text", "") for item in reply.content)
                raise RuntimeError(message[:1000] or "Station capture failed")
            result, raw = import_envelope(reply.structuredContent or {}, root)
            return None, result, raw
    except Exception as exc:
        return f"Station capture failed; not retried: {exc}", None, ""
