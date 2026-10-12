"""Convert DebugSnapshots into Polars DataFrames for querying."""

from __future__ import annotations

import polars as pl

from devtools_mcp.debug.models import DebugSnapshot

_FRAME_SCHEMA = {
    "stop_seq": pl.Int64,
    "thread_id": pl.Int64,
    "thread_name": pl.Utf8,
    "stop_reason": pl.Utf8,
    "frame_index": pl.Int64,
    "function": pl.Utf8,
    "file": pl.Utf8,
    "line": pl.Int64,
    "module": pl.Utf8,
}

_VARIABLE_SCHEMA = {
    "stop_seq": pl.Int64,
    "scope": pl.Utf8,
    "path": pl.Utf8,
    "name": pl.Utf8,
    "type": pl.Utf8,
    "value": pl.Utf8,
    "depth": pl.Int64,
    "expandable": pl.Boolean,
}


def debug_frames_df(snapshot: DebugSnapshot) -> pl.DataFrame:
    """All captured stack frames across threads."""
    rows = []
    for thread in snapshot.threads:
        for frame in thread.frames:
            rows.append(
                {
                    "stop_seq": snapshot.stop_seq,
                    "thread_id": thread.thread_id,
                    "thread_name": thread.name,
                    "stop_reason": thread.stop_reason,
                    "frame_index": frame.index,
                    "function": frame.function,
                    "file": frame.file,
                    "line": frame.line,
                    "module": frame.module,
                }
            )
    return pl.DataFrame(rows, schema=_FRAME_SCHEMA) if rows else pl.DataFrame(schema=_FRAME_SCHEMA)


def debug_variables_df(snapshot: DebugSnapshot) -> pl.DataFrame:
    """Flattened variables (+ watches as scope='watch'), the diff/query surface."""
    rows = []
    for var in snapshot.variables:
        rows.append(
            {
                "stop_seq": snapshot.stop_seq,
                "scope": var.scope,
                "path": var.path,
                "name": var.name,
                "type": var.type,
                "value": var.value,
                "depth": var.depth,
                "expandable": var.ref > 0,
            }
        )
    for watch in snapshot.watches:
        rows.append(
            {
                "stop_seq": snapshot.stop_seq,
                "scope": "watch",
                "path": watch.expression,
                "name": watch.expression,
                "type": watch.type,
                "value": watch.error or watch.value,
                "depth": 0,
                "expandable": False,
            }
        )
    return pl.DataFrame(rows, schema=_VARIABLE_SCHEMA) if rows else pl.DataFrame(schema=_VARIABLE_SCHEMA)


def debug_threads_df(snapshot: DebugSnapshot) -> pl.DataFrame:
    schema = {
        "thread_id": pl.Int64,
        "name": pl.Utf8,
        "stopped": pl.Boolean,
        "stop_reason": pl.Utf8,
        "frame_count": pl.Int64,
        "top_function": pl.Utf8,
        "top_file": pl.Utf8,
    }
    rows = []
    for thread in snapshot.threads:
        top = thread.frames[0] if thread.frames else None
        rows.append(
            {
                "thread_id": thread.thread_id,
                "name": thread.name,
                "stopped": thread.stopped,
                "stop_reason": thread.stop_reason,
                "frame_count": len(thread.frames),
                "top_function": top.function if top else None,
                "top_file": top.file if top else None,
            }
        )
    return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)


def debug_breakpoints_df(snapshot: DebugSnapshot) -> pl.DataFrame:
    schema = {
        "id": pl.Int64,
        "verified": pl.Boolean,
        "source": pl.Utf8,
        "line": pl.Int64,
        "function": pl.Utf8,
        "condition": pl.Utf8,
        "hit_condition": pl.Utf8,
        "log_message": pl.Utf8,
        "message": pl.Utf8,
    }
    rows = [
        {
            "id": bp.id,
            "verified": bp.verified,
            "source": bp.source,
            "line": bp.line,
            "function": bp.function,
            "condition": bp.condition,
            "hit_condition": bp.hit_condition,
            "log_message": bp.log_message,
            "message": bp.message,
        }
        for bp in snapshot.breakpoints
    ]
    return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)


def debug_changes_df(snapshot: DebugSnapshot) -> pl.DataFrame:
    schema = {"stop_seq": pl.Int64, "path": pl.Utf8, "kind": pl.Utf8, "old": pl.Utf8, "new": pl.Utf8}
    rows = [
        {
            "stop_seq": snapshot.stop_seq,
            "path": change.path,
            "kind": change.kind,
            "old": change.old,
            "new": change.new,
        }
        for change in snapshot.changes
    ]
    return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)


def debug_output_df(snapshot: DebugSnapshot) -> pl.DataFrame:
    lines = (snapshot.raw_output or "").splitlines()
    if not lines:
        return pl.DataFrame(schema={"line_no": pl.Int64, "text": pl.Utf8})
    return pl.DataFrame({"line_no": list(range(len(lines))), "text": lines})


# tool name (RunBase.tool) -> builder. "stop" snapshots default to the
# variables frame, the most queried surface; other views have their own
# inspect tools that store runs with the matching tool name.
DF_BUILDERS = {
    "stop": debug_variables_df,
    "variables": debug_variables_df,
    "stack": debug_frames_df,
    "threads": debug_threads_df,
    "breakpoints": debug_breakpoints_df,
    "diff": debug_changes_df,
    "output": debug_output_df,
    "watches": debug_variables_df,
    "expression": debug_variables_df,
    "memory": debug_output_df,
    "disassemble": debug_output_df,
    "registers": debug_variables_df,
    "plan": debug_changes_df,
    "session": debug_frames_df,
    "_default": debug_variables_df,
}
