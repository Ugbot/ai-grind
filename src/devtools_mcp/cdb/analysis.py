"""CDB analysis — Polars DataFrame + flame-graph stacks."""

from __future__ import annotations

import polars as pl

from devtools_mcp.cdb.models import CdbSnapshot
from devtools_mcp.models import StackSample


def cdb_frames_df(snapshot: CdbSnapshot) -> pl.DataFrame:
    """Every stack frame across all threads — `function` aliased for search."""
    rows = []
    for t in snapshot.threads:
        for f in t.frames:
            rows.append(
                {
                    "thread": t.index,
                    "tid": t.tid,
                    "frame_index": f.index,
                    "function": f.symbol,
                    "module": f.module,
                    "file": f.file,
                    "line": f.line,
                    "value": float(f.index),
                }
            )
    if not rows:
        return pl.DataFrame(
            schema={
                "thread": pl.Int64,
                "tid": pl.Utf8,
                "frame_index": pl.Int64,
                "function": pl.Utf8,
                "module": pl.Utf8,
                "file": pl.Utf8,
                "line": pl.Int64,
                "value": pl.Float64,
            }
        )
    return pl.DataFrame(rows)


def cdb_stack_samples(snapshot: CdbSnapshot) -> list[StackSample]:
    """Each thread's stack as a flame-graph sample (leaf-first → root-first)."""
    samples: list[StackSample] = []
    for t in snapshot.threads:
        if not t.frames:
            continue
        frames = [f.symbol for f in reversed(t.frames)]  # frame 00 is innermost
        samples.append(StackSample(frames=frames, weight=1))
    return samples
