"""Read-only bounded Instruments thread policy evidence tool."""

from __future__ import annotations

from functools import partial
from typing import Annotated, Any

import anyio
from mcp.types import ToolAnnotations
from pydantic import Field

from devtools_mcp.evidence.artifacts import ArtifactError
from devtools_mcp.evidence.benchmark import EvidenceParseError
from devtools_mcp.evidence.instruments_threads import analyze_instruments_thread_policy
from devtools_mcp.server import mcp

PositiveInt = Annotated[int, Field(strict=True, gt=0, le=1 << 40)]
UInt64 = Annotated[int, Field(strict=True, ge=0, le=(1 << 64) - 1)]


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=False))
async def devtools_instruments_threads(
    thread_info_path: str,
    qos_path: str,
    priority_path: str,
    *,
    pid: UInt64,
    window_start_ns: UInt64,
    window_end_ns: UInt64,
    max_bytes: PositiveInt = 64 << 20,
    max_rows: PositiveInt = 100000,
    max_nodes: PositiveInt = 500000,
    max_threads: PositiveInt = 64,
    top_n: PositiveInt = 20,
) -> dict[str, Any]:
    """Validate caller-associated xctrace XML and summarize observed thread policy.

    The explicit half-open window uses trace-relative integer nanoseconds. Gaps
    stay unknown; sparse policy coverage returns PASS/PARTIAL, never a default
    QoS. Raw exporter enum codes and formatted labels are preserved, not mapped
    to OS QoS constants. Combined byte/row/node caps and target-thread/detail caps
    fail closed; XML references and identities are checked before PID filtering.
    PID/TID identity cannot authenticate a shared capture/execution. No native
    clock mapping, latency/off-CPU causality, or performance qualification follows.
    Does not launch processes, change policy or write artifacts.
    """
    analyze = partial(
        analyze_instruments_thread_policy,
        thread_info_path,
        qos_path,
        priority_path,
        pid=pid,
        window_start_ns=window_start_ns,
        window_end_ns=window_end_ns,
        max_bytes=max_bytes,
        max_rows=max_rows,
        max_nodes=max_nodes,
        max_threads=max_threads,
        top_n=top_n,
    )
    try:
        return await anyio.to_thread.run_sync(analyze)
    except (EvidenceParseError, ArtifactError, OSError, ValueError) as exc:
        return {
            "analysis_validation": "FAIL",
            "performance_validation": "UNVERIFIED",
            "error": {"code": "invalid_evidence", "message": str(exc)},
        }
