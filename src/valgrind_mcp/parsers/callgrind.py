"""Parser for callgrind output format.

The callgrind format is a line-oriented text format used by both callgrind
and cachegrind (cachegrind is a subset). It supports name compression via
integer ID mappings.

Reference: https://valgrind.org/docs/manual/cl-format.html
"""

from __future__ import annotations

import re
from pathlib import Path

from valgrind_mcp.models import (
    CallgrindCall,
    CallgrindFunction,
    CallgrindResult,
    ValgrindRun,
)


def parse_callgrind(file_path: str, run_base: ValgrindRun) -> CallgrindResult:
    """Parse a callgrind output file into structured data.

    Handles the callgrind format including name compression (integer ID maps),
    cost lines, and call specifications.
    """
    path = Path(file_path)
    if not path.exists():
        return CallgrindResult(
            **run_base.model_dump(),
            events=[],
            functions=[],
            totals={},
        )

    text = path.read_text(errors="replace")
    lines = text.splitlines()

    events: list[str] = []
    totals: dict[str, int] = {}
    functions_map: dict[str, CallgrindFunction] = {}  # keyed by "file:function"

    # Name compression maps
    file_map: dict[str, str] = {}  # "(N)" -> name
    fn_map: dict[str, str] = {}
    obj_map: dict[str, str] = {}

    current_file: str = "???"
    current_fn: str = "???"
    current_obj: str | None = None
    call_target_fn: str | None = None
    call_target_file: str | None = None
    call_count: int = 0

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        i += 1

        if not line or line.startswith("#"):
            continue

        # Header fields
        if line.startswith("events:") or line.startswith("Events:"):
            events = line.split(":", 1)[1].strip().split()
            continue

        if line.startswith("summary:") or line.startswith("Summary:"):
            vals = line.split(":", 1)[1].strip().split()
            for idx, val in enumerate(vals):
                if idx < len(events):
                    totals[events[idx]] = int(val)
            continue

        if line.startswith("totals:") or line.startswith("Totals:"):
            vals = line.split(":", 1)[1].strip().split()
            for idx, val in enumerate(vals):
                if idx < len(events):
                    totals[events[idx]] = int(val)
            continue

        # Skip other header fields
        if ":" in line and not line[0].isdigit() and not line.startswith("cfn=") and not line.startswith("calls="):
            key, _, value = line.partition(":")
            key = key.strip().lower()

            # Name compression definitions
            if key.startswith("fl") or key.startswith("fi") or key.startswith("fe"):
                name = _resolve_compression(value.strip(), file_map, key[:2])
                current_file = name
                continue
            if key.startswith("fn"):
                name = _resolve_compression(value.strip(), fn_map, "fn")
                current_fn = name
                call_target_fn = None  # Reset call context
                continue
            if key.startswith("ob"):
                name = _resolve_compression(value.strip(), obj_map, "ob")
                current_obj = name
                continue
            if key.startswith("cfn"):
                call_target_fn = _resolve_compression(value.strip(), fn_map, "fn")
                continue
            if key.startswith("cfl") or key.startswith("cfi"):
                call_target_file = _resolve_compression(value.strip(), file_map, key[:3])
                continue
            # Skip other headers
            continue

        # Assignments without colon prefix (fl=, fn=, etc. at start of line)
        if line.startswith("fl=") or line.startswith("fi=") or line.startswith("fe="):
            current_file = _resolve_compression(line.split("=", 1)[1], file_map, line[:2])
            continue
        if line.startswith("fn="):
            current_fn = _resolve_compression(line.split("=", 1)[1], fn_map, "fn")
            call_target_fn = None
            continue
        if line.startswith("ob="):
            current_obj = _resolve_compression(line.split("=", 1)[1], obj_map, "ob")
            continue
        if line.startswith("cfn="):
            call_target_fn = _resolve_compression(line.split("=", 1)[1], fn_map, "fn")
            continue
        if line.startswith("cfl=") or line.startswith("cfi="):
            call_target_file = _resolve_compression(line.split("=", 1)[1], file_map, line[:3])
            continue

        # Calls specification
        if line.startswith("calls="):
            parts = line.split("=", 1)[1].strip().split()
            if parts:
                call_count = int(parts[0])
            continue

        # Cost line: starts with digits (position followed by costs)
        if line[0].isdigit() or line[0] == "+":
            parts = line.split()
            if len(parts) < 2:
                continue

            # Parse costs (skip position)
            costs = {}
            cost_values = parts[1:]  # Skip line number
            for idx, val in enumerate(cost_values):
                if idx < len(events):
                    costs[events[idx]] = int(val)

            func_key = f"{current_file}:{current_fn}"

            if call_target_fn is not None and call_count > 0:
                # This is a call cost line
                if func_key in functions_map:
                    functions_map[func_key].callees.append(
                        CallgrindCall(
                            target=call_target_fn,
                            target_file=call_target_file,
                            count=call_count,
                            cost=costs,
                        )
                    )
                call_target_fn = None
                call_target_file = None
                call_count = 0
            else:
                # Self cost line
                if func_key not in functions_map:
                    functions_map[func_key] = CallgrindFunction(
                        name=current_fn,
                        file=current_file if current_file != "???" else None,
                        object=current_obj,
                        self_cost={},
                        inclusive_cost={},
                    )
                fn_entry = functions_map[func_key]
                for event_name, cost_val in costs.items():
                    fn_entry.self_cost[event_name] = fn_entry.self_cost.get(event_name, 0) + cost_val

    # Compute inclusive costs (self + callee costs)
    for fn_entry in functions_map.values():
        fn_entry.inclusive_cost = dict(fn_entry.self_cost)
        for callee in fn_entry.callees:
            for event_name, cost_val in callee.cost.items():
                fn_entry.inclusive_cost[event_name] = fn_entry.inclusive_cost.get(event_name, 0) + cost_val

    return CallgrindResult(
        **run_base.model_dump(),
        events=events,
        functions=list(functions_map.values()),
        totals=totals,
    )


def _resolve_compression(value: str, name_map: dict[str, str], prefix: str) -> str:
    """Resolve name compression. Format: '(N) name' defines, '(N)' references."""
    value = value.strip()
    match = re.match(r"\((\d+)\)\s*(.*)", value)
    if match:
        idx = match.group(1)
        name = match.group(2).strip()
        if name:
            # Definition: (N) name
            name_map[idx] = name
            return name
        else:
            # Reference: (N)
            return name_map.get(idx, f"({idx})")
    return value
