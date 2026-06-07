"""Python analysis — Polars DataFrames + flame-graph stacks."""

from __future__ import annotations

import polars as pl

from devtools_mcp.hotspots import hotspots_df
from devtools_mcp.models import StackSample
from devtools_mcp.py.models import PyResult


def py_hotspots_df(result: PyResult) -> pl.DataFrame:
    """Per-function hotspots from py-spy sampling stacks."""
    return hotspots_df(result.stack_samples)


def py_funcstats_df(result: PyResult) -> pl.DataFrame:
    """cProfile rows: ncalls / tottime / cumtime, sorted by cumulative time."""
    rows = [
        {
            "function": s.function,
            "ncalls": s.ncalls,
            "tottime": s.tottime,
            "cumtime": s.cumtime,
            "percall": s.percall_cum,
            "value": s.cumtime,  # alias for the unified index
        }
        for s in result.func_stats
    ]
    if not rows:
        return pl.DataFrame(
            schema={"function": pl.Utf8, "ncalls": pl.Int64, "tottime": pl.Float64,
                    "cumtime": pl.Float64, "percall": pl.Float64, "value": pl.Float64}
        )
    return pl.DataFrame(rows).sort("cumtime", descending=True)


def py_threads_df(result: PyResult) -> pl.DataFrame:
    """py-spy dump threads: state, depth, top frame."""
    rows = [
        {
            "function": t.frames[0] if t.frames else "",
            "thread": t.name or t.tid,
            "state": t.state,
            "frame_count": len(t.frames),
            "value": float(len(t.frames)),
        }
        for t in result.threads
    ]
    if not rows:
        return pl.DataFrame(
            schema={"function": pl.Utf8, "thread": pl.Utf8, "state": pl.Utf8,
                    "frame_count": pl.Int64, "value": pl.Float64}
        )
    return pl.DataFrame(rows)


def py_stack_samples(result: PyResult) -> list[StackSample]:
    """Flame-graph stacks (py-spy record / memray)."""
    return list(result.stack_samples)
