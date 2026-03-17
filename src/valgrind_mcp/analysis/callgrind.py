"""Callgrind profiling analysis with Polars DataFrames."""

from __future__ import annotations

import polars as pl

from valgrind_mcp.models import CallgrindResult


def callgrind_df(result: CallgrindResult) -> pl.DataFrame:
    """Convert callgrind functions into a Polars DataFrame."""
    rows = []
    for fn in result.functions:
        row: dict[str, str | int | None] = {
            "function": fn.name,
            "file": fn.file,
            "object": fn.object,
        }
        for event in result.events:
            row[f"self_{event}"] = fn.self_cost.get(event, 0)
            row[f"inclusive_{event}"] = fn.inclusive_cost.get(event, 0)
        row["num_callees"] = len(fn.callees)
        rows.append(row)
    if not rows:
        schema: dict[str, type[pl.DataType]] = {
            "function": pl.Utf8,
            "file": pl.Utf8,
            "object": pl.Utf8,
            "num_callees": pl.Int64,
        }
        for event in result.events:
            schema[f"self_{event}"] = pl.Int64
            schema[f"inclusive_{event}"] = pl.Int64
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows)


def hotspots(result: CallgrindResult, event: str = "Ir", top_n: int = 20) -> pl.DataFrame:
    """Find top functions by a given event cost."""
    df = callgrind_df(result)
    if df.is_empty():
        return df
    self_col = f"self_{event}"
    incl_col = f"inclusive_{event}"
    if self_col not in df.columns:
        return df.head(0)

    total = result.totals.get(event, 1)
    return (
        df.select("function", "file", self_col, incl_col)
        .with_columns(
            (pl.col(self_col) / total * 100).round(2).alias("self_pct"),
            (pl.col(incl_col) / total * 100).round(2).alias("inclusive_pct"),
        )
        .sort(self_col, descending=True)
        .head(top_n)
    )


def call_graph_summary(result: CallgrindResult) -> pl.DataFrame:
    """Summarize caller-callee relationships with costs."""
    rows = []
    for fn in result.functions:
        for callee in fn.callees:
            row: dict[str, str | int] = {
                "caller": fn.name,
                "callee": callee.target,
                "call_count": callee.count,
            }
            for event in result.events:
                row[f"cost_{event}"] = callee.cost.get(event, 0)
            rows.append(row)
    if not rows:
        return pl.DataFrame(schema={"caller": pl.Utf8, "callee": pl.Utf8, "call_count": pl.Int64})
    return pl.DataFrame(rows)
