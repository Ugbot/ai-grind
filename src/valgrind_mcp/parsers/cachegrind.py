"""Parser for cachegrind output format.

Cachegrind produces output in a subset of the callgrind format.
We parse both the raw file (for summary) and optionally run cg_annotate
for per-line data.
"""

from __future__ import annotations

import re
from pathlib import Path

from valgrind_mcp.models import (
    CachegrindLine,
    CachegrindResult,
    ValgrindRun,
)

# Standard cachegrind events in order
CACHEGRIND_EVENTS = ["Ir", "I1mr", "ILmr", "Dr", "D1mr", "DLmr", "Dw", "D1mw", "DLmw"]


def parse_cachegrind(file_path: str, run_base: ValgrindRun) -> CachegrindResult:
    """Parse a cachegrind.out file into structured data.

    Parses the raw format which is a subset of callgrind format.
    """
    path = Path(file_path)
    if not path.exists():
        return CachegrindResult(
            **run_base.model_dump(),
            events=[],
            summary={},
            lines=[],
        )

    text = path.read_text(errors="replace")
    file_lines = text.splitlines()

    events: list[str] = []
    summary: dict[str, int] = {}
    data_lines: list[CachegrindLine] = []

    current_file = "???"
    current_fn = "???"

    for raw_line in file_lines:
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        # Events header
        if line.startswith("events:") or line.startswith("Events:"):
            events = line.split(":", 1)[1].strip().split()
            continue

        # Summary line
        if line.startswith("summary:") or line.startswith("Summary:"):
            vals = line.split(":", 1)[1].strip().split()
            for idx, val in enumerate(vals):
                if idx < len(events):
                    summary[events[idx]] = int(val)
            continue

        # File spec
        if line.startswith("fl=") or line.startswith("fi=") or line.startswith("fe="):
            current_file = line.split("=", 1)[1].strip()
            continue

        # Function spec
        if line.startswith("fn="):
            current_fn = line.split("=", 1)[1].strip()
            continue

        # Skip other headers
        if "=" in line and not line[0].isdigit():
            continue

        # Cost line: line_number cost1 cost2 ...
        if line[0].isdigit():
            parts = line.split()
            if len(parts) < 2:
                continue

            line_num = int(parts[0])
            costs = [int(v) for v in parts[1:]]

            # Map costs to event names
            event_costs = {}
            for idx, val in enumerate(costs):
                if idx < len(events):
                    event_costs[events[idx]] = val

            data_lines.append(CachegrindLine(
                file=current_file,
                function=current_fn,
                line=line_num,
                ir=event_costs.get("Ir", 0),
                i1mr=event_costs.get("I1mr", 0),
                ilmr=event_costs.get("ILmr", 0),
                dr=event_costs.get("Dr", 0),
                d1mr=event_costs.get("D1mr", 0),
                dlmr=event_costs.get("DLmr", 0),
                dw=event_costs.get("Dw", 0),
                d1mw=event_costs.get("D1mw", 0),
                dlmw=event_costs.get("DLmw", 0),
            ))

    return CachegrindResult(
        **run_base.model_dump(),
        events=events,
        summary=summary,
        lines=data_lines,
    )
