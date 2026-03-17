"""Cachegrind cache profiling analysis with Polars DataFrames."""

from __future__ import annotations

import polars as pl

from valgrind_mcp.models import CachegrindResult


def cachegrind_df(result: CachegrindResult) -> pl.DataFrame:
    """Convert cachegrind lines into a Polars DataFrame with miss rates."""
    rows = []
    for cline in result.lines:
        row = cline.model_dump()
        row["i1_miss_rate"] = (cline.i1mr / cline.ir * 100) if cline.ir > 0 else 0.0
        row["d1_read_miss_rate"] = (cline.d1mr / cline.dr * 100) if cline.dr > 0 else 0.0
        row["d1_write_miss_rate"] = (cline.d1mw / cline.dw * 100) if cline.dw > 0 else 0.0
        row["ll_miss_rate"] = (cline.ilmr + cline.dlmr + cline.dlmw) / max(cline.ir + cline.dr + cline.dw, 1) * 100
        rows.append(row)
    if not rows:
        return pl.DataFrame(
            schema={
                "file": pl.Utf8,
                "function": pl.Utf8,
                "line": pl.Int64,
                "ir": pl.Int64,
                "i1mr": pl.Int64,
                "ilmr": pl.Int64,
                "dr": pl.Int64,
                "d1mr": pl.Int64,
                "dlmr": pl.Int64,
                "dw": pl.Int64,
                "d1mw": pl.Int64,
                "dlmw": pl.Int64,
                "i1_miss_rate": pl.Float64,
                "d1_read_miss_rate": pl.Float64,
                "d1_write_miss_rate": pl.Float64,
                "ll_miss_rate": pl.Float64,
            }
        )
    return pl.DataFrame(rows)


def cache_miss_rates(result: CachegrindResult, top_n: int = 20) -> pl.DataFrame:
    """Find functions with worst cache miss rates."""
    df = cachegrind_df(result)
    if df.is_empty():
        return df
    return (
        df.group_by("function")
        .agg(
            pl.col("ir").sum().alias("total_ir"),
            pl.col("i1mr").sum().alias("total_i1mr"),
            pl.col("dr").sum().alias("total_dr"),
            pl.col("d1mr").sum().alias("total_d1mr"),
            pl.col("dw").sum().alias("total_dw"),
            pl.col("d1mw").sum().alias("total_d1mw"),
        )
        .with_columns(
            (pl.col("total_i1mr") / pl.col("total_ir").cast(pl.Float64) * 100).fill_nan(0.0).alias("i1_miss_pct"),
            (pl.col("total_d1mr") / pl.col("total_dr").cast(pl.Float64) * 100).fill_nan(0.0).alias("d1_read_miss_pct"),
            (pl.col("total_d1mw") / pl.col("total_dw").cast(pl.Float64) * 100).fill_nan(0.0).alias("d1_write_miss_pct"),
        )
        .sort("total_ir", descending=True)
        .head(top_n)
    )
