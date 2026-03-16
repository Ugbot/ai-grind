"""Parser for massif output format.

Massif produces a structured text format with snapshot data
including heap usage over time and allocation trees.

Reference: massif output is designed to be both human and machine readable.
"""

from __future__ import annotations

import re
from pathlib import Path

from valgrind_mcp.models import (
    MassifAllocation,
    MassifResult,
    MassifSnapshot,
    ValgrindRun,
)


def parse_massif(file_path: str, run_base: ValgrindRun) -> MassifResult:
    """Parse a massif.out file into structured data."""
    path = Path(file_path)
    if not path.exists():
        return MassifResult(
            **run_base.model_dump(),
            snapshots=[],
            peak_snapshot_index=-1,
            command="",
            time_unit="i",
        )

    text = path.read_text(errors="replace")
    lines = text.splitlines()

    command = ""
    time_unit = "i"
    snapshots: list[MassifSnapshot] = []
    peak_index = -1

    # Header parsing
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        i += 1

        if line.startswith("desc:"):
            continue
        if line.startswith("cmd:"):
            command = line.split(":", 1)[1].strip()
            continue
        if line.startswith("time_unit:"):
            time_unit = line.split(":", 1)[1].strip()
            continue
        if line.startswith("#-"):
            # Separator line, start of snapshot data
            break

    # Snapshot parsing
    # After the separator, snapshots follow this pattern:
    # snapshot=N
    # #-----------
    # time=N
    # mem_heap_B=N
    # mem_heap_extra_B=N
    # mem_stacks_B=N
    # heap_tree=empty|detailed|peak
    # [optional tree data]

    current_snapshot: dict = {}

    while i < len(lines):
        line = lines[i].strip()
        i += 1

        if not line or line.startswith("#-"):
            # Save previous snapshot if we have one
            if current_snapshot and "index" in current_snapshot:
                snapshot = _build_snapshot(current_snapshot)
                snapshots.append(snapshot)
                if snapshot.is_peak:
                    peak_index = snapshot.index
                current_snapshot = {}
            continue

        if line.startswith("snapshot="):
            current_snapshot["index"] = int(line.split("=", 1)[1])
            continue

        if line.startswith("time="):
            current_snapshot["time"] = int(line.split("=", 1)[1])
            continue

        if line.startswith("mem_heap_B="):
            current_snapshot["heap_bytes"] = int(line.split("=", 1)[1])
            continue

        if line.startswith("mem_heap_extra_B="):
            current_snapshot["heap_extra_bytes"] = int(line.split("=", 1)[1])
            continue

        if line.startswith("mem_stacks_B="):
            current_snapshot["stacks_bytes"] = int(line.split("=", 1)[1])
            continue

        if line.startswith("heap_tree="):
            tree_type = line.split("=", 1)[1].strip()
            current_snapshot["tree_type"] = tree_type
            if tree_type in ("detailed", "peak"):
                # Parse the heap tree
                tree_lines: list[str] = []
                while i < len(lines):
                    tline = lines[i]
                    # Tree lines start with spaces and 'n' or are continuation
                    if tline.strip().startswith("n") or (tline and tline[0] == " "):
                        tree_lines.append(tline)
                        i += 1
                    else:
                        break
                if tree_lines:
                    current_snapshot["heap_tree"] = _parse_heap_tree(tree_lines)
            continue

    # Don't forget the last snapshot
    if current_snapshot and "index" in current_snapshot:
        snapshot = _build_snapshot(current_snapshot)
        snapshots.append(snapshot)
        if snapshot.is_peak:
            peak_index = snapshot.index

    return MassifResult(
        **run_base.model_dump(),
        snapshots=snapshots,
        peak_snapshot_index=peak_index,
        command=command,
        time_unit=time_unit,
    )


def _build_snapshot(data: dict) -> MassifSnapshot:
    """Build a MassifSnapshot from parsed data dict."""
    tree_type = data.get("tree_type", "empty")
    return MassifSnapshot(
        index=data.get("index", 0),
        time=data.get("time", 0),
        heap_bytes=data.get("heap_bytes", 0),
        heap_extra_bytes=data.get("heap_extra_bytes", 0),
        stacks_bytes=data.get("stacks_bytes", 0),
        is_peak=(tree_type == "peak"),
        is_detailed=(tree_type in ("detailed", "peak")),
        heap_tree=data.get("heap_tree"),
    )


def _parse_heap_tree(lines: list[str]) -> MassifAllocation | None:
    """Parse a massif heap tree.

    Tree format:
    n<count>: <bytes> <description>
     n<count>: <bytes> <description>
      ...

    Indentation indicates parent-child relationships.
    """
    if not lines:
        return None

    # Build a stack-based tree parser
    root: MassifAllocation | None = None
    stack: list[tuple[int, MassifAllocation]] = []  # (indent_level, node)

    for line in lines:
        # Count leading spaces for indent level
        stripped = line.lstrip()
        indent = len(line) - len(stripped)

        match = re.match(r"n(\d+):\s+(\d+)\s+(.*)", stripped)
        if not match:
            continue

        _child_count = int(match.group(1))
        alloc_bytes = int(match.group(2))
        description = match.group(3).strip()

        # Parse description for function/file info
        fn, file_name, line_num = _parse_alloc_description(description)

        node = MassifAllocation(
            bytes=alloc_bytes,
            function=fn,
            file=file_name,
            line=line_num,
        )

        if root is None:
            root = node
            stack = [(indent, node)]
        else:
            # Find parent: last node with indent < current
            while stack and stack[-1][0] >= indent:
                stack.pop()

            if stack:
                parent = stack[-1][1]
                parent.children.append(node)

            stack.append((indent, node))

    return root


def _parse_alloc_description(desc: str) -> tuple[str | None, str | None, int | None]:
    """Parse a massif allocation description into function, file, line.

    Examples:
        'in 42 places, all below massif's threshold (1.00%)'
        '0x1234: malloc (vg_replace_malloc.c:299)'
        '0x1234: main (example.c:10)'
    """
    # Pattern: address: function (file:line)
    match = re.match(r"0x[0-9A-Fa-f]+:\s+(\S+)\s+\(([^:)]+):(\d+)\)", desc)
    if match:
        return match.group(1), match.group(2), int(match.group(3))

    # Pattern: address: function (in object)
    match = re.match(r"0x[0-9A-Fa-f]+:\s+(\S+)\s+\(in\s+(.+)\)", desc)
    if match:
        return match.group(1), None, None

    # Pattern: just address and function
    match = re.match(r"0x[0-9A-Fa-f]+:\s+(\S+)", desc)
    if match:
        return match.group(1), None, None

    return None, None, None
