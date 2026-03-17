"""Cross-tool comparison analysis with Polars DataFrames."""

from __future__ import annotations

import polars as pl

from valgrind_mcp.analysis.callgrind import callgrind_df
from valgrind_mcp.analysis.memcheck import errors_by_kind
from valgrind_mcp.models import CallgrindResult, MassifResult, MemcheckResult


def compare_memcheck(a: MemcheckResult, b: MemcheckResult) -> pl.DataFrame:
    """Compare two memcheck runs by error kind."""
    df_a = errors_by_kind(a).rename({"count": "count_a", "total_bytes_leaked": "bytes_a"})
    df_b = errors_by_kind(b).rename({"count": "count_b", "total_bytes_leaked": "bytes_b"})

    if df_a.is_empty() and df_b.is_empty():
        return pl.DataFrame(schema={"kind": pl.Utf8})

    a_cols = [c for c in ["kind", "count_a", "bytes_a"] if c in df_a.columns]
    b_cols = [c for c in ["kind", "count_b", "bytes_b"] if c in df_b.columns]

    joined = (
        df_a.select(a_cols)
        .join(
            df_b.select(b_cols),
            on="kind",
            how="full",
            coalesce=True,
        )
        .fill_null(0)
    )

    if "count_a" in joined.columns and "count_b" in joined.columns:
        joined = joined.with_columns(
            (pl.col("count_b") - pl.col("count_a")).alias("count_delta"),
        )

    return joined


def compare_callgrind(
    a: CallgrindResult,
    b: CallgrindResult,
    event: str = "Ir",
) -> pl.DataFrame:
    """Compare two callgrind runs by function cost."""
    self_col = f"self_{event}"
    df_a = callgrind_df(a).select("function", self_col).rename({self_col: "cost_a"})
    df_b = callgrind_df(b).select("function", self_col).rename({self_col: "cost_b"})

    joined = df_a.join(df_b, on="function", how="full", coalesce=True).fill_null(0)
    return joined.with_columns(
        (pl.col("cost_b") - pl.col("cost_a")).alias("cost_delta"),
        ((pl.col("cost_b") - pl.col("cost_a")) / pl.col("cost_a").cast(pl.Float64) * 100)
        .fill_nan(0.0)
        .alias("pct_change"),
    ).sort("cost_delta", descending=True)


def compare_massif(a: MassifResult, b: MassifResult) -> dict[str, int | float]:
    """Compare two massif runs: peak memory and snapshot counts."""

    def _peak_bytes(r: MassifResult) -> int:
        for snap in r.snapshots:
            if snap.is_peak:
                return snap.heap_bytes + snap.heap_extra_bytes + snap.stacks_bytes
        if r.snapshots:
            return max(s.heap_bytes + s.heap_extra_bytes + s.stacks_bytes for s in r.snapshots)
        return 0

    peak_a = _peak_bytes(a)
    peak_b = _peak_bytes(b)

    return {
        "peak_a_bytes": peak_a,
        "peak_b_bytes": peak_b,
        "peak_delta_bytes": peak_b - peak_a,
        "peak_delta_pct": ((peak_b - peak_a) / max(peak_a, 1)) * 100,
        "snapshots_a": len(a.snapshots),
        "snapshots_b": len(b.snapshots),
    }
