"""Massif heap profiling analysis with Polars DataFrames."""

from __future__ import annotations

import polars as pl

from devtools_mcp.valgrind.models import MassifAllocation, MassifResult


def massif_timeline_df(result: MassifResult) -> pl.DataFrame:
    """Convert massif snapshots into a time-series DataFrame."""
    rows = []
    for snap in result.snapshots:
        rows.append(
            {
                "snapshot": snap.index,
                "time": snap.time,
                "heap_bytes": snap.heap_bytes,
                "heap_extra_bytes": snap.heap_extra_bytes,
                "stacks_bytes": snap.stacks_bytes,
                "total_bytes": snap.heap_bytes + snap.heap_extra_bytes + snap.stacks_bytes,
                "is_peak": snap.is_peak,
                "is_detailed": snap.is_detailed,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "snapshot": pl.Int64,
                "time": pl.Int64,
                "heap_bytes": pl.Int64,
                "heap_extra_bytes": pl.Int64,
                "stacks_bytes": pl.Int64,
                "total_bytes": pl.Int64,
                "is_peak": pl.Boolean,
                "is_detailed": pl.Boolean,
            }
        )
    return pl.DataFrame(rows)


def peak_allocations(result: MassifResult) -> list[dict[str, int | str | None]]:
    """Extract allocation tree at peak snapshot."""
    if result.peak_snapshot_index < 0:
        return []

    peak = None
    for snap in result.snapshots:
        if snap.index == result.peak_snapshot_index:
            peak = snap
            break

    if not peak or not peak.heap_tree:
        return []

    return _flatten_alloc_tree(peak.heap_tree)


def _flatten_alloc_tree(
    alloc: MassifAllocation,
    depth: int = 0,
) -> list[dict[str, int | str | None]]:
    """Flatten allocation tree into a list of dicts with depth info."""
    result: list[dict[str, int | str | None]] = [
        {
            "depth": depth,
            "bytes": alloc.bytes,
            "function": alloc.function,
            "file": alloc.file,
            "line": alloc.line,
        }
    ]
    for child in alloc.children:
        result.extend(_flatten_alloc_tree(child, depth + 1))
    return result
