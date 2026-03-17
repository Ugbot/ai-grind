"""Memcheck error analysis with Polars DataFrames."""

from __future__ import annotations

import polars as pl

from valgrind_mcp.models import MemcheckResult


def memcheck_errors_df(result: MemcheckResult) -> pl.DataFrame:
    """Convert memcheck errors into a Polars DataFrame."""
    rows = []
    for err in result.errors:
        top_frame = err.stack[0] if err.stack else None
        rows.append(
            {
                "error_id": err.unique_id,
                "kind": err.kind,
                "what": err.what,
                "bytes_leaked": err.bytes_leaked or 0,
                "blocks_leaked": err.blocks_leaked or 0,
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
                "bytes_leaked": pl.Int64,
                "blocks_leaked": pl.Int64,
                "top_function": pl.Utf8,
                "top_file": pl.Utf8,
                "top_line": pl.Int64,
                "stack_depth": pl.Int64,
            }
        )
    return pl.DataFrame(rows)


def errors_by_kind(result: MemcheckResult) -> pl.DataFrame:
    """Aggregate memcheck errors by kind."""
    df = memcheck_errors_df(result)
    if df.is_empty():
        return df
    return (
        df.group_by("kind")
        .agg(
            pl.len().alias("count"),
            pl.col("bytes_leaked").sum().alias("total_bytes_leaked"),
            pl.col("blocks_leaked").sum().alias("total_blocks_leaked"),
        )
        .sort("count", descending=True)
    )


def errors_by_function(result: MemcheckResult, top_n: int = 20) -> pl.DataFrame:
    """Aggregate memcheck errors by top function in stack."""
    df = memcheck_errors_df(result)
    if df.is_empty():
        return df
    return (
        df.filter(pl.col("top_function").is_not_null())
        .group_by("top_function")
        .agg(
            pl.len().alias("count"),
            pl.col("kind").n_unique().alias("unique_kinds"),
            pl.col("bytes_leaked").sum().alias("total_bytes_leaked"),
        )
        .sort("count", descending=True)
        .head(top_n)
    )


def errors_by_file(result: MemcheckResult, top_n: int = 20) -> pl.DataFrame:
    """Aggregate memcheck errors by source file."""
    df = memcheck_errors_df(result)
    if df.is_empty():
        return df
    return (
        df.filter(pl.col("top_file").is_not_null())
        .group_by("top_file")
        .agg(
            pl.len().alias("count"),
            pl.col("kind").n_unique().alias("unique_kinds"),
            pl.col("bytes_leaked").sum().alias("total_bytes_leaked"),
        )
        .sort("count", descending=True)
        .head(top_n)
    )
