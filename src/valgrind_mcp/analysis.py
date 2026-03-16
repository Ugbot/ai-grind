"""Polars-based analysis functions for all Valgrind tool outputs."""

from __future__ import annotations

import polars as pl

from valgrind_mcp.models import (
    CallgrindResult,
    CachegrindResult,
    MassifResult,
    MemcheckResult,
    ThreadCheckResult,
    ValgrindResult,
)


# ============================================================
# Memcheck Analysis
# ============================================================


def memcheck_errors_df(result: MemcheckResult) -> pl.DataFrame:
    """Convert memcheck errors into a Polars DataFrame."""
    rows = []
    for err in result.errors:
        top_frame = err.stack[0] if err.stack else None
        rows.append({
            "error_id": err.unique_id,
            "kind": err.kind,
            "what": err.what,
            "bytes_leaked": err.bytes_leaked or 0,
            "blocks_leaked": err.blocks_leaked or 0,
            "top_function": top_frame.fn if top_frame else None,
            "top_file": top_frame.file if top_frame else None,
            "top_line": top_frame.line if top_frame else None,
            "stack_depth": len(err.stack),
        })
    if not rows:
        return pl.DataFrame(schema={
            "error_id": pl.Utf8, "kind": pl.Utf8, "what": pl.Utf8,
            "bytes_leaked": pl.Int64, "blocks_leaked": pl.Int64,
            "top_function": pl.Utf8, "top_file": pl.Utf8,
            "top_line": pl.Int64, "stack_depth": pl.Int64,
        })
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


# ============================================================
# Thread Check Analysis (Helgrind / DRD)
# ============================================================


def threadcheck_errors_df(result: ThreadCheckResult) -> pl.DataFrame:
    """Convert thread errors into a Polars DataFrame."""
    rows = []
    for err in result.errors:
        top_frame = err.stack[0] if err.stack else None
        rows.append({
            "error_id": err.unique_id,
            "kind": err.kind,
            "what": err.what,
            "thread_id": err.thread_id,
            "top_function": top_frame.fn if top_frame else None,
            "top_file": top_frame.file if top_frame else None,
            "top_line": top_frame.line if top_frame else None,
            "stack_depth": len(err.stack),
        })
    if not rows:
        return pl.DataFrame(schema={
            "error_id": pl.Utf8, "kind": pl.Utf8, "what": pl.Utf8,
            "thread_id": pl.Int64, "top_function": pl.Utf8,
            "top_file": pl.Utf8, "top_line": pl.Int64, "stack_depth": pl.Int64,
        })
    return pl.DataFrame(rows)


def thread_errors_by_kind(result: ThreadCheckResult) -> pl.DataFrame:
    """Aggregate thread errors by kind."""
    df = threadcheck_errors_df(result)
    if df.is_empty():
        return df
    return (
        df.group_by("kind")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )


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


# ============================================================
# Callgrind Analysis
# ============================================================


def callgrind_df(result: CallgrindResult) -> pl.DataFrame:
    """Convert callgrind functions into a Polars DataFrame."""
    rows = []
    for fn in result.functions:
        row: dict = {
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
        schema: dict = {"function": pl.Utf8, "file": pl.Utf8, "object": pl.Utf8, "num_callees": pl.Int64}
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
            row = {
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


# ============================================================
# Cachegrind Analysis
# ============================================================


def cachegrind_df(result: CachegrindResult) -> pl.DataFrame:
    """Convert cachegrind lines into a Polars DataFrame with miss rates."""
    rows = []
    for cline in result.lines:
        row = cline.model_dump()
        # Compute miss rates
        row["i1_miss_rate"] = (cline.i1mr / cline.ir * 100) if cline.ir > 0 else 0.0
        row["d1_read_miss_rate"] = (cline.d1mr / cline.dr * 100) if cline.dr > 0 else 0.0
        row["d1_write_miss_rate"] = (cline.d1mw / cline.dw * 100) if cline.dw > 0 else 0.0
        row["ll_miss_rate"] = ((cline.ilmr + cline.dlmr + cline.dlmw) /
                               max(cline.ir + cline.dr + cline.dw, 1) * 100)
        rows.append(row)
    if not rows:
        return pl.DataFrame(schema={
            "file": pl.Utf8, "function": pl.Utf8, "line": pl.Int64,
            "ir": pl.Int64, "i1mr": pl.Int64, "ilmr": pl.Int64,
            "dr": pl.Int64, "d1mr": pl.Int64, "dlmr": pl.Int64,
            "dw": pl.Int64, "d1mw": pl.Int64, "dlmw": pl.Int64,
            "i1_miss_rate": pl.Float64, "d1_read_miss_rate": pl.Float64,
            "d1_write_miss_rate": pl.Float64, "ll_miss_rate": pl.Float64,
        })
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
            (pl.col("total_i1mr") / pl.col("total_ir").cast(pl.Float64) * 100)
            .fill_nan(0.0).alias("i1_miss_pct"),
            (pl.col("total_d1mr") / pl.col("total_dr").cast(pl.Float64) * 100)
            .fill_nan(0.0).alias("d1_read_miss_pct"),
            (pl.col("total_d1mw") / pl.col("total_dw").cast(pl.Float64) * 100)
            .fill_nan(0.0).alias("d1_write_miss_pct"),
        )
        .sort("total_ir", descending=True)
        .head(top_n)
    )


# ============================================================
# Massif Analysis
# ============================================================


def massif_timeline_df(result: MassifResult) -> pl.DataFrame:
    """Convert massif snapshots into a time-series DataFrame."""
    rows = []
    for snap in result.snapshots:
        rows.append({
            "snapshot": snap.index,
            "time": snap.time,
            "heap_bytes": snap.heap_bytes,
            "heap_extra_bytes": snap.heap_extra_bytes,
            "stacks_bytes": snap.stacks_bytes,
            "total_bytes": snap.heap_bytes + snap.heap_extra_bytes + snap.stacks_bytes,
            "is_peak": snap.is_peak,
            "is_detailed": snap.is_detailed,
        })
    if not rows:
        return pl.DataFrame(schema={
            "snapshot": pl.Int64, "time": pl.Int64,
            "heap_bytes": pl.Int64, "heap_extra_bytes": pl.Int64,
            "stacks_bytes": pl.Int64, "total_bytes": pl.Int64,
            "is_peak": pl.Boolean, "is_detailed": pl.Boolean,
        })
    return pl.DataFrame(rows)


def peak_allocations(result: MassifResult) -> list[dict]:
    """Extract allocation tree at peak snapshot."""
    if result.peak_snapshot_index < 0:
        return []

    peak = None
    for snap in result.snapshots:
        if snap.index == result.peak_snapshot_index:
            peak = snap
            break

    if not peak or not peak.heap_tree:
        return []

    return _flatten_alloc_tree(peak.heap_tree)


def _flatten_alloc_tree(alloc: "MassifAllocation", depth: int = 0) -> list[dict]:
    """Flatten allocation tree into a list of dicts with depth info."""
    from valgrind_mcp.models import MassifAllocation
    result = [{
        "depth": depth,
        "bytes": alloc.bytes,
        "function": alloc.function,
        "file": alloc.file,
        "line": alloc.line,
    }]
    for child in alloc.children:
        result.extend(_flatten_alloc_tree(child, depth + 1))
    return result


# ============================================================
# Cross-tool Comparison
# ============================================================


def compare_memcheck(a: MemcheckResult, b: MemcheckResult) -> pl.DataFrame:
    """Compare two memcheck runs by error kind."""
    df_a = errors_by_kind(a).rename({"count": "count_a", "total_bytes_leaked": "bytes_a"})
    df_b = errors_by_kind(b).rename({"count": "count_b", "total_bytes_leaked": "bytes_b"})

    if df_a.is_empty() and df_b.is_empty():
        return pl.DataFrame(schema={"kind": pl.Utf8})

    # Select only columns that exist
    a_cols = ["kind", "count_a"]
    if "bytes_a" in df_a.columns:
        a_cols.append("bytes_a")
    b_cols = ["kind", "count_b"]
    if "bytes_b" in df_b.columns:
        b_cols.append("bytes_b")

    joined = df_a.select([c for c in a_cols if c in df_a.columns]).join(
        df_b.select([c for c in b_cols if c in df_b.columns]),
        on="kind", how="full", coalesce=True,
    ).fill_null(0)

    if "count_a" in joined.columns and "count_b" in joined.columns:
        joined = joined.with_columns(
            (pl.col("count_b") - pl.col("count_a")).alias("count_delta"),
        )

    return joined


def compare_callgrind(a: CallgrindResult, b: CallgrindResult, event: str = "Ir") -> pl.DataFrame:
    """Compare two callgrind runs by function cost."""
    df_a = callgrind_df(a).select("function", f"self_{event}").rename({f"self_{event}": "cost_a"})
    df_b = callgrind_df(b).select("function", f"self_{event}").rename({f"self_{event}": "cost_b"})

    joined = df_a.join(df_b, on="function", how="full", coalesce=True).fill_null(0)
    return joined.with_columns(
        (pl.col("cost_b") - pl.col("cost_a")).alias("cost_delta"),
        ((pl.col("cost_b") - pl.col("cost_a")) / pl.col("cost_a").cast(pl.Float64) * 100)
        .fill_nan(0.0).alias("pct_change"),
    ).sort("cost_delta", descending=True)


def compare_massif(a: MassifResult, b: MassifResult) -> dict:
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
