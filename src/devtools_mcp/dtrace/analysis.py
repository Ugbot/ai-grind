"""DTrace analysis — convert results to Polars DataFrames."""

from __future__ import annotations

import polars as pl

from devtools_mcp.dtrace.models import DTraceResult


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


def dtrace_quantize_df(result: DTraceResult) -> pl.DataFrame:
    """Quantization buckets across all distributions."""
    rows = []
    for quant in result.quantizations:
        for bucket in quant.buckets:
            rows.append(
                {
                    "key": quant.key,
                    "low": bucket.low,
                    "high": bucket.high,
                    "count": bucket.count,
                    "total": quant.total,
                }
            )
    if not rows:
        return pl.DataFrame(
            schema={
                "key": pl.Utf8,
                "low": pl.Int64,
                "high": pl.Int64,
                "count": pl.Int64,
                "total": pl.Int64,
            }
        )
    return pl.DataFrame(rows)


def dtrace_probe_hits_df(result: DTraceResult) -> pl.DataFrame:
    """Probe hits as rows."""
    rows = []
    for hit in result.probe_hits:
        rows.append(
            {
                "probe": hit.probe,
                "cpu": hit.cpu,
                "pid": hit.pid,
                "execname": hit.execname,
                "args": hit.args,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "probe": pl.Utf8,
                "cpu": pl.Int64,
                "pid": pl.Int64,
                "execname": pl.Utf8,
                "args": pl.Utf8,
            }
        )
    return pl.DataFrame(rows)
