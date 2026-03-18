"""Convert LLDB snapshots into Polars DataFrames for filtering/analysis."""

from __future__ import annotations

import polars as pl

from devtools_mcp.lldb.models import LldbSnapshot


def lldb_frames_df(snapshot: LldbSnapshot) -> pl.DataFrame:
    """All stack frames across all threads — filterable by function, file, module."""
    rows = []
    for thread in snapshot.threads:
        for frame in thread.frames:
            rows.append(
                {
                    "thread_id": thread.thread_id,
                    "thread_name": thread.name,
                    "stop_reason": thread.stop_reason,
                    "frame_index": frame.index,
                    "address": frame.address,
                    "module": frame.module,
                    "function": frame.function,
                    "file": frame.file,
                    "line": frame.line,
                }
            )
    if not rows:
        return pl.DataFrame(
            schema={
                "thread_id": pl.Int64,
                "thread_name": pl.Utf8,
                "stop_reason": pl.Utf8,
                "frame_index": pl.Int64,
                "address": pl.Utf8,
                "module": pl.Utf8,
                "function": pl.Utf8,
                "file": pl.Utf8,
                "line": pl.Int64,
            }
        )
    return pl.DataFrame(rows)


def lldb_threads_df(snapshot: LldbSnapshot) -> pl.DataFrame:
    """Thread summary — one row per thread with stop reason and frame count."""
    rows = []
    for thread in snapshot.threads:
        top_frame = thread.frames[0] if thread.frames else None
        rows.append(
            {
                "thread_id": thread.thread_id,
                "name": thread.name,
                "queue": thread.queue,
                "stop_reason": thread.stop_reason,
                "frame_count": len(thread.frames),
                "top_function": top_frame.function if top_frame else None,
                "top_file": top_frame.file if top_frame else None,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "thread_id": pl.Int64,
                "name": pl.Utf8,
                "queue": pl.Utf8,
                "stop_reason": pl.Utf8,
                "frame_count": pl.Int64,
                "top_function": pl.Utf8,
                "top_file": pl.Utf8,
            }
        )
    return pl.DataFrame(rows)


def lldb_variables_df(snapshot: LldbSnapshot) -> pl.DataFrame:
    """Variables in current frame."""
    rows = []
    for var in snapshot.variables:
        rows.append(
            {
                "name": var.name,
                "type": var.type,
                "value": var.value,
                "summary": var.summary,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "name": pl.Utf8,
                "type": pl.Utf8,
                "value": pl.Utf8,
                "summary": pl.Utf8,
            }
        )
    return pl.DataFrame(rows)


def lldb_breakpoints_df(snapshot: LldbSnapshot) -> pl.DataFrame:
    """All breakpoints with hit counts."""
    rows = []
    for bp in snapshot.breakpoints:
        rows.append(
            {
                "id": bp.id,
                "name": bp.name,
                "file": bp.file,
                "line": bp.line,
                "address": bp.address,
                "hit_count": bp.hit_count,
                "enabled": bp.enabled,
                "condition": bp.condition,
                "resolved": bp.resolved,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "id": pl.Int64,
                "name": pl.Utf8,
                "file": pl.Utf8,
                "line": pl.Int64,
                "address": pl.Utf8,
                "hit_count": pl.Int64,
                "enabled": pl.Boolean,
                "condition": pl.Utf8,
                "resolved": pl.Boolean,
            }
        )
    return pl.DataFrame(rows)
