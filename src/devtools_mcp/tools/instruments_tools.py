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


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=False))
async def devtools_instruments_ring(
    native_report_path: str,
    csv_path: str,
    signpost_path: str,
    thread_state_path: str,
    thread_info_path: str,
    *,
    pid: UInt64,
    assume_constant_offset: Annotated[bool, Field(strict=True)] = False,
    max_bytes: PositiveInt = 128 << 20,
    max_rows: PositiveInt = 500000,
    max_nodes: PositiveInt = 3000000,
    max_threads: PositiveInt = 64,
    top_n: PositiveInt = 5,
    state_details: PositiveInt = 20,
) -> dict[str, Any]:
    """Validate ring CSV/native metrics, collected finish marker and scheduler XML.

    Default returns validated evidence with clock join UNVERIFIED and no joined
    events. Explicit assume_constant_offset enables CONDITIONAL single-anchor
    extrapolation with drift UNMEASURED, not performance/causality qualification.
    Possible envelopes and guaranteed cores intersect marker-main-thread states;
    no producer role is inferred. Coverage and omitted detail counts/durations
    remain explicit. CSV stamps have no independent DTrace boundary proof here.
    Budgets combine all files; XML/reference depth64 and native JSON1MiB caps
    additionally apply. Files are caller-associated, not capture-UUID authenticated.
    Does not launch processes, change policy or write artifacts.
    """
    from devtools_mcp.evidence.instruments_ring import analyze_instruments_ring_arrival

    analyze = partial(
        analyze_instruments_ring_arrival,
        native_report_path,
        csv_path,
        signpost_path,
        thread_state_path,
        thread_info_path,
        pid=pid,
        assume_constant_offset=assume_constant_offset,
        max_bytes=max_bytes,
        max_rows=max_rows,
        max_nodes=max_nodes,
        max_threads=max_threads,
        top_n=top_n,
        state_details=state_details,
    )
    try:
        return await anyio.to_thread.run_sync(analyze)
    except (EvidenceParseError, ArtifactError, OSError, ValueError) as exc:
        return {
            "analysis_validation": "FAIL",
            "performance_validation": "UNVERIFIED",
            "clock_join_validation": "UNVERIFIED",
            "error": {"code": "invalid_evidence", "message": str(exc)},
        }
