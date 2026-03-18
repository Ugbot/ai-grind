"""Thread error analysis (Helgrind/DRD) with Polars DataFrames."""

from __future__ import annotations

import polars as pl

from devtools_mcp.valgrind.models import ThreadCheckResult


def threadcheck_errors_df(result: ThreadCheckResult) -> pl.DataFrame:
    """Convert thread errors into a Polars DataFrame."""
    rows = []
    for err in result.errors:
        top_frame = err.stack[0] if err.stack else None
        rows.append(
            {
                "error_id": err.unique_id,
                "kind": err.kind,
                "what": err.what,
                "thread_id": err.thread_id,
                "top_function": top_frame.fn if top_frame else None,
                "top_file": top_frame.file if top_frame else None,
                "top_line": top_frame.line if top_frame else None,
                "stack_depth": len(err.stack),
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "error_id": pl.Utf8,
                "kind": pl.Utf8,
                "what": pl.Utf8,
                "thread_id": pl.Int64,
                "top_function": pl.Utf8,
                "top_file": pl.Utf8,
                "top_line": pl.Int64,
                "stack_depth": pl.Int64,
            }
        )
    return pl.DataFrame(rows)


def thread_errors_by_kind(result: ThreadCheckResult) -> pl.DataFrame:
    """Aggregate thread errors by kind."""
    df = threadcheck_errors_df(result)
    if df.is_empty():
        return df
    return df.group_by("kind").agg(pl.len().alias("count")).sort("count", descending=True)


def thread_errors_by_function(result: ThreadCheckResult, top_n: int = 20) -> pl.DataFrame:
    """Aggregate thread errors by function."""
    df = threadcheck_errors_df(result)
    if df.is_empty():
        return df
    return (
        df.filter(pl.col("top_function").is_not_null())
        .group_by("top_function")
        .agg(
            pl.len().alias("count"),
            pl.col("kind").n_unique().alias("unique_kinds"),
        )
        .sort("count", descending=True)
        .head(top_n)
    )
