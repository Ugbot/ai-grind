"""perf analysis — convert results to Polars DataFrames."""

from __future__ import annotations

import polars as pl

from devtools_mcp.perf.models import PerfAnnotationResult, PerfRecordResult, PerfStatResult


def perf_counters_df(result: PerfStatResult) -> pl.DataFrame:
    """Hardware counters as rows."""
    rows = []
    for counter in result.counters:
        rows.append(
            {
                "event": counter.event,
                "value": counter.value,
                "unit": counter.unit,
                "variance_pct": counter.variance_pct,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "event": pl.Utf8,
                "value": pl.Float64,
                "unit": pl.Utf8,
                "variance_pct": pl.Float64,
            }
        )
    return pl.DataFrame(rows)


def perf_hotspots_df(result: PerfRecordResult) -> pl.DataFrame:
    """Sampling hotspots — symbol, shared object, overhead."""
    rows = []
    for sample in result.samples:
        rows.append(
            {
                "symbol": sample.symbol,
                "function": sample.symbol,  # alias for cross-tool compatibility
                "shared_object": sample.shared_object,
                "command": sample.command,
                "overhead_pct": sample.overhead_pct,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "function": pl.Utf8,
                "shared_object": pl.Utf8,
                "command": pl.Utf8,
                "overhead_pct": pl.Float64,
            }
        )
    return pl.DataFrame(rows)


def perf_annotation_df(result: PerfAnnotationResult) -> pl.DataFrame:
    """Per-instruction annotation with hot percentages."""
    rows = []
    for line in result.lines:
        rows.append(
            {
                "address": line.address,
                "instruction": line.instruction,
                "percent": line.percent,
                "file": line.file,
                "line": line.line_number,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "address": pl.Utf8,
                "instruction": pl.Utf8,
                "percent": pl.Float64,
                "file": pl.Utf8,
                "line": pl.Int64,
            }
        )
    return pl.DataFrame(rows)
