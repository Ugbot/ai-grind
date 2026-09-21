"""RenderDoc analysis, Polars frames per verb + GPU-time flame-graph stacks."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from devtools_mcp.models import RunBase, StackSample
from devtools_mcp.renderdoc.models import (
    RenderdocCaptureResult,
    RenderdocReplayResult,
    RenderdocThumbResult,
)

MAX_STACK_DEPTH = 64

_ACTIONS_SCHEMA = {
    "event_id": pl.Int64,
    "action_id": pl.Int64,
    "parent_event_id": pl.Int64,
    "depth": pl.Int64,
    "function": pl.Utf8,  # action name, aliased for function_pattern filters
    "flags": pl.Utf8,
    "indices": pl.Int64,
    "instances": pl.Int64,
    "dispatch_x": pl.Int64,
    "dispatch_y": pl.Int64,
    "dispatch_z": pl.Int64,
    "duration_us": pl.Float64,
    "value": pl.Float64,
}


def rdoc_actions_df(result: RunBase) -> pl.DataFrame:
    """The frame's action tree, `value` is duration_us when counters ran, else indices."""
    assert isinstance(result, RenderdocReplayResult), f"expected replay result, got {type(result)}"
    rows = []
    for a in result.actions:
        dispatch = (a.dispatch + [0, 0, 0])[:3]
        value = a.duration_us if a.duration_us is not None else float(a.num_indices)
        rows.append(
            {
                "event_id": a.event_id,
                "action_id": a.action_id,
                "parent_event_id": a.parent_event_id,
                "depth": a.depth,
                "function": a.name,
                "flags": a.flags,
                "indices": a.num_indices,
                "instances": a.num_instances,
                "dispatch_x": dispatch[0],
                "dispatch_y": dispatch[1],
                "dispatch_z": dispatch[2],
                "duration_us": a.duration_us,
                "value": value,
            }
        )
    if not rows:
        return pl.DataFrame(schema=_ACTIONS_SCHEMA)
    return pl.DataFrame(rows, schema_overrides={"duration_us": pl.Float64, "value": pl.Float64})


def rdoc_resources_df(result: RunBase) -> pl.DataFrame:
    """GPU resources referenced by the capture, `value` is byte size."""
    assert isinstance(result, RenderdocReplayResult), f"expected replay result, got {type(result)}"
    rows = [
        {
            "resource_id": r.resource_id,
            "function": r.name,  # aliased for function_pattern filters
            "type": r.type,
            "width": r.width,
            "height": r.height,
            "depth": r.depth,
            "mips": r.mips,
            "format": r.format,
            "bytes": r.bytes,
            "value": float(r.bytes),
        }
        for r in result.resources
    ]
    if not rows:
        return pl.DataFrame(
            schema={
                "resource_id": pl.Utf8,
                "function": pl.Utf8,
                "type": pl.Utf8,
                "width": pl.Int64,
                "height": pl.Int64,
                "depth": pl.Int64,
                "mips": pl.Int64,
                "format": pl.Utf8,
                "bytes": pl.Int64,
                "value": pl.Float64,
            }
        )
    return pl.DataFrame(rows)


def rdoc_counters_df(result: RunBase) -> pl.DataFrame:
    """Raw GPU counter samples, one row per (event, counter)."""
    assert isinstance(result, RenderdocReplayResult), f"expected replay result, got {type(result)}"
    event_names = {a.event_id: a.name for a in result.actions}
    rows = [
        {
            "event_id": c.event_id,
            "function": event_names.get(c.event_id, ""),
            "counter": c.counter,
            "unit": c.unit,
            "value": c.value,
        }
        for c in result.counters
    ]
    if not rows:
        return pl.DataFrame(
            schema={
                "event_id": pl.Int64,
                "function": pl.Utf8,
                "counter": pl.Utf8,
                "unit": pl.Utf8,
                "value": pl.Float64,
            }
        )
    return pl.DataFrame(rows)


def rdoc_capture_df(result: RunBase) -> pl.DataFrame:
    """Captures produced by a capture run. One row per .rdc."""
    assert isinstance(result, RenderdocCaptureResult), f"expected capture result, got {type(result)}"
    rows = []
    for path in result.rdc_paths:
        p = Path(path)
        size = p.stat().st_size if p.is_file() else 0
        rows.append(
            {
                "rdc": path,
                "bytes": size,
                "frame": result.frame_captured if result.frame_captured is not None else -1,
                "value": float(size),
            }
        )
    if not rows:
        return pl.DataFrame(schema={"rdc": pl.Utf8, "bytes": pl.Int64, "frame": pl.Int64, "value": pl.Float64})
    return pl.DataFrame(rows)


def rdoc_thumb_df(result: RunBase) -> pl.DataFrame:
    """Thumbnail output, a single-row frame."""
    assert isinstance(result, RenderdocThumbResult), f"expected thumb result, got {type(result)}"
    row = {
        "rdc": result.rdc_path,
        "thumb": result.thumb_path,
        "width": result.width,
        "height": result.height,
        "value": float(result.width * result.height),
    }
    return pl.DataFrame([row])


def rdoc_stack_samples(result: RunBase) -> list[StackSample]:
    """Marker-region hierarchy as flame-graph stacks, one sample per leaf action.

    Weight is GPU duration in whole microseconds when counters were fetched
    (a GPU-time flame graph of the frame), else 1 (a structural flame graph).
    """
    if not isinstance(result, RenderdocReplayResult):
        return []
    by_event = {a.event_id: a for a in result.actions}
    child_counts: dict[int, int] = {}
    for action in result.actions:
        child_counts[action.parent_event_id] = child_counts.get(action.parent_event_id, 0) + 1
    samples: list[StackSample] = []
    for action in result.actions:
        if child_counts.get(action.event_id, 0) > 0:
            continue  # not a leaf
        frames = [action.name]
        cursor = action
        for _ in range(MAX_STACK_DEPTH):
            parent = by_event.get(cursor.parent_event_id)
            if parent is None or parent is cursor:
                break
            frames.append(parent.name)
            cursor = parent
        assert len(frames) <= MAX_STACK_DEPTH + 1, "stack walk exceeded bound"
        weight = int(action.duration_us) if action.duration_us is not None else 1
        samples.append(StackSample(frames=list(reversed(frames)), weight=max(weight, 1)))
    return samples
