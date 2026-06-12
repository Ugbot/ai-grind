"""JVM analysis — Polars DataFrames + flame-graph stacks."""

from __future__ import annotations

import polars as pl

from devtools_mcp.flamegraph.tree import build_call_tree, function_stats
from devtools_mcp.jvm.models import JvmResult
from devtools_mcp.models import StackSample


def jvm_hotspots_df(result: JvmResult) -> pl.DataFrame:
    """Per-method exclusive/inclusive sample counts from JFR/async-profiler stacks."""
    tree = build_call_tree(result.stack_samples)
    total = tree.total_weight or 1
    stats = function_stats(tree)
    rows = [
        {
            "function": name,
            "exclusive": exc,
            "inclusive": inc,
            "exc_pct": 100.0 * exc / total,
            "value": 100.0 * exc / total,  # alias for the unified index
        }
        for name, (exc, inc) in stats.items()
    ]
    if not rows:
        return pl.DataFrame(
            schema={
                "function": pl.Utf8,
                "exclusive": pl.Int64,
                "inclusive": pl.Int64,
                "exc_pct": pl.Float64,
                "value": pl.Float64,
            }
        )
    return pl.DataFrame(rows).sort("exclusive", descending=True)


def jvm_threads_df(result: JvmResult) -> pl.DataFrame:
    """Threads as rows: state, daemon, depth, top frame."""
    rows = []
    for t in result.threads:
        rows.append(
            {
                "function": t.frames[0] if t.frames else "",  # top frame, aliased
                "thread": t.name,
                "state": t.state,
                "daemon": t.daemon,
                "frame_count": len(t.frames),
                "value": float(len(t.frames)),
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "function": pl.Utf8,
                "thread": pl.Utf8,
                "state": pl.Utf8,
                "daemon": pl.Boolean,
                "frame_count": pl.Int64,
                "value": pl.Float64,
            }
        )
    return pl.DataFrame(rows)


def jvm_heap_df(result: JvmResult) -> pl.DataFrame:
    """Heap histogram classes by retained bytes."""
    rows = []
    for c in result.heap_classes:
        rows.append(
            {
                "function": c.class_name,  # aliased so search finds it
                "instances": c.instances,
                "bytes": c.bytes,
                "value": float(c.bytes),
            }
        )
    if not rows:
        return pl.DataFrame(schema={"function": pl.Utf8, "instances": pl.Int64, "bytes": pl.Int64, "value": pl.Float64})
    return pl.DataFrame(rows).sort("bytes", descending=True)


def jvm_stack_samples(result: JvmResult) -> list[StackSample]:
    """Flame-graph stacks (JFR / async-profiler)."""
    return list(result.stack_samples)
