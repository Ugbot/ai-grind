"""DTrace analysis — convert results to Polars DataFrames."""

from __future__ import annotations

import polars as pl

from devtools_mcp.dtrace.models import DTraceResult
from devtools_mcp.models import StackSample


def dtrace_stack_samples(result: DTraceResult) -> list[StackSample]:
    """Map DTrace stacks to flame-graph StackSamples (leaf-last → root-first)."""
    samples: list[StackSample] = []
    for stack in result.stacks:
        if not stack.frames:
            continue
        # DTrace prints stacks leaf-first; flame graphs want root-first.
        samples.append(StackSample(frames=list(reversed(stack.frames)), weight=stack.count))
    return samples


def dtrace_aggregation_df(result: DTraceResult) -> pl.DataFrame:
    """Aggregations as rows with key columns + value."""
    rows = []
    for agg in result.aggregations:
        row: dict[str, str | int] = {"value": agg.value, "agg_type": agg.agg_type}
        for i, key in enumerate(agg.keys):
            row[f"key_{i}"] = key
        # Also put first key as "function" for cross-tool compatibility
        if agg.keys:
            row["function"] = agg.keys[0]
        rows.append(row)
    if not rows:
        return pl.DataFrame(
            schema={
                "function": pl.Utf8,
                "value": pl.Int64,
                "agg_type": pl.Utf8,
            }
        )
    return pl.DataFrame(rows)


def dtrace_stacks_df(result: DTraceResult) -> pl.DataFrame:
    """Stack traces with count, top function, frame count."""
    rows = []
    for stack in result.stacks:
        top_fn = stack.frames[0] if stack.frames else None
        # Extract function name from "module`function+offset"
        function = None
        module = None
        if top_fn and "`" in top_fn:
            parts = top_fn.split("`", 1)
            module = parts[0]
            function = parts[1].split("+")[0] if "+" in parts[1] else parts[1]
        elif top_fn:
            function = top_fn

        rows.append(
            {
                "function": function,
                "module": module,
                "count": stack.count,
                "frame_count": len(stack.frames),
                "top_frame": top_fn,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "function": pl.Utf8,
                "module": pl.Utf8,
                "count": pl.Int64,
                "frame_count": pl.Int64,
                "top_frame": pl.Utf8,
            }
        )
    return pl.DataFrame(rows)
