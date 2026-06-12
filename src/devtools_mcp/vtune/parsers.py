"""Parsers for VTune CSV reports (`vtune -report ... -format csv`).

Two shapes matter:

- function reports (`-report hotspots -group-by function`): flat rows,
  "Function" + per-analysis metric columns ("CPU Time", "Loads", ...).
- top-down (`-report top-down`): a "Function Stack" column where depth is
  encoded as leading spaces — that tree becomes folded stacks for flame graphs.

Values arrive as "1.234", "1.234s", "12.5%", or "" — `_to_float` normalizes.
"""

from __future__ import annotations

import csv
import io
import re

from devtools_mcp.models import StackSample
from devtools_mcp.vtune.models import VtuneFunction

MAX_ROWS = 500_000  # bound: refuse a pathologically huge CSV
MAX_STACK_DEPTH = 512

_NON_METRIC_COLUMNS = {
    "function",
    "function stack",
    "function (full)",
    "module",
    "source file",
    "start address",
    "process",
    "thread",
}


def _to_float(raw: str) -> float | None:
    """Parse a VTune metric cell ('1.234s', '12.5%', '1,234', '')."""
    assert isinstance(raw, str), "metric cell must be str"
    cleaned = raw.strip().rstrip("s%").replace(",", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _norm_key(column: str) -> str:
    """'CPU Time:Effective Time' -> 'cpu_time_effective_time'."""
    key = re.sub(r"[^a-z0-9]+", "_", column.strip().lower()).strip("_")
    assert key or not column.strip(), f"column {column!r} normalized to nothing"
    return key


def parse_function_csv(text: str) -> list[VtuneFunction]:
    """Parse a flat function-grouped VTune CSV report."""
    assert isinstance(text, str), "csv text must be str"
    reader = csv.DictReader(io.StringIO(text))
    fields = reader.fieldnames or []
    metric_columns = [c for c in fields if c and c.strip().lower() not in _NON_METRIC_COLUMNS]
    functions: list[VtuneFunction] = []
    for i, row in enumerate(reader):
        if i >= MAX_ROWS:
            break
        name = (row.get("Function") or row.get("Function (Full)") or "").strip()
        if not name:
            continue
        metrics: dict[str, float] = {}
        for column in metric_columns:
            value = _to_float(row.get(column) or "")
            if value is not None:
                metrics[_norm_key(column)] = value
        functions.append(
            VtuneFunction(
                function=name,
                module=(row.get("Module") or "").strip(),
                source_file=(row.get("Source File") or "").strip(),
                metrics=metrics,
                primary=next(iter(metrics.values()), 0.0),
            )
        )
    assert len(functions) <= MAX_ROWS, "parsed more rows than the bound"
    return functions


def _stack_depth(cell: str) -> int:
    """Depth of a 'Function Stack' cell — one leading space per level."""
    depth = len(cell) - len(cell.lstrip(" "))
    assert depth >= 0, "negative indentation"
    return depth


def parse_topdown_csv(text: str) -> list[StackSample]:
    """Fold a top-down report's indented Function Stack into StackSamples.

    Weight is the row's self time in milliseconds; rows with zero self time
    still shape the tree but emit no sample of their own.
    """
    assert isinstance(text, str), "csv text must be str"
    reader = csv.DictReader(io.StringIO(text))
    fields = reader.fieldnames or []
    stack_column = next((c for c in fields if c.strip().lower() == "function stack"), None)
    if stack_column is None:
        return []
    self_column = next(
        (c for c in fields if _norm_key(c) in ("cpu_time_self", "cpu_time_self_time", "self_time")),
        None,
    )
    samples: list[StackSample] = []
    path: list[str] = []
    for i, row in enumerate(reader):
        if i >= MAX_ROWS:
            break
        cell = row.get(stack_column) or ""
        name = cell.strip()
        if not name:
            continue
        depth = _stack_depth(cell)
        if depth > MAX_STACK_DEPTH:
            continue
        del path[depth:]  # pop back to the parent level
        path.append(name)
        assert len(path) == depth + 1, "stack path out of sync with indentation"
        self_seconds = _to_float(row.get(self_column) or "") if self_column else None
        if self_seconds is not None and self_seconds > 0:
            weight_ms = int(round(self_seconds * 1000))
            if weight_ms > 0:
                frames = [f for f in path if f.lower() != "total"]
                if frames:
                    samples.append(StackSample(frames=list(frames), weight=weight_ms))
    assert all(s.weight > 0 for s in samples), "zero-weight sample emitted"
    return samples
