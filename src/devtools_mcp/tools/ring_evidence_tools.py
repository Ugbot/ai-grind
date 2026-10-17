"""Bounded offline attribution for the owned native ring benchmark."""

from __future__ import annotations

from functools import partial
from typing import Annotated, Any

import anyio
from pydantic import Field

from devtools_mcp.evidence import EvidenceParseError
from devtools_mcp.evidence.ring_arrival import analyze_ring_arrival_boundaries
from devtools_mcp.server import mcp

PositiveInt = Annotated[int, Field(strict=True, gt=0)]
NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]


@mcp.tool()
async def devtools_ring_arrival(
    boundary_path: str,
    arrival_csv_path: str,
    benchmark: dict[str, Any],
    max_events: PositiveInt = 1_000_000,
    max_bytes: PositiveInt = 256 << 20,
    top_n: PositiveInt = 20,
    preceding_events: NonNegativeInt = 0,
) -> dict[str, Any]:
    """Validate and attribute one same-execution native ring diagnostic.

    Reads dedicated DTrace B records and native ordered arrival CSV, with the
    exact native benchmark JSON object. Separate DTrace output with -o: mixed
    child stdout is refused. Checks every event, thread, phase, due equation,
    clock-domain bracket and native percentile before reporting bounded phase
    summaries and worst events. Caps apply to input events/bytes and report rows;
    max_bytes is an input-byte budget, not an aggregate resident-memory budget.
    Uses integer nanoseconds; publication-to-reader times remain bounds.
    Dedicated records must uniformly have seven columns or append an eighth
    DTrace vtimestamp_ns column. CPU attribution validates same-thread logical
    order and reports signed wall-minus-CPU residuals, including negatives.
    Those residuals include probe/accounting differences and are not pure
    off-CPU time, preemption evidence or an algorithmic-cause classification.
    preceding_events independently caps measured chronological predecessors of
    the single worst event (0..max_events); warm events are never returned.
    An analysis PASS never qualifies instrumented p99 or uninstrumented speed.
    Does not launch a process, change scheduling, write artifacts or score CI.
    """
    analyze = partial(
        analyze_ring_arrival_boundaries,
        boundary_path,
        arrival_csv_path,
        benchmark,
        max_events=max_events,
        max_bytes=max_bytes,
        top_n=top_n,
        preceding_events=preceding_events,
    )
    try:
        return await anyio.to_thread.run_sync(analyze)
    except (EvidenceParseError, OSError, ValueError) as exc:
        return {
            "analysis_validation": "FAIL",
            "performance_validation": "UNVERIFIED",
            "error": {"code": "invalid_evidence", "message": str(exc)},
        }
