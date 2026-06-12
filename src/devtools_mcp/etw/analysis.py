"""ETW analysis — Polars DataFrame + flame-graph stacks."""

from __future__ import annotations

import polars as pl

from devtools_mcp.etw.models import EtwResult
from devtools_mcp.etw.parsers import is_synthetic
from devtools_mcp.models import StackSample


def etw_hotspots_df(result: EtwResult) -> pl.DataFrame:
    """CPU nodes as queryable rows (Exc/Inc); `function`/`value` aliased for search."""
    rows = []
    for s in result.samples:
        if is_synthetic(s.name):
            continue
        rows.append(
            {
                "function": s.function,
                "module": s.module,
                "exc_pct": s.exc_pct,
                "inc_pct": s.inc_pct,
                "exc": s.exc,
                "inc": s.inc,
                "value": s.exc_pct,  # alias for the unified index
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "function": pl.Utf8,
                "module": pl.Utf8,
                "exc_pct": pl.Float64,
                "inc_pct": pl.Float64,
                "exc": pl.Float64,
                "inc": pl.Float64,
                "value": pl.Float64,
            }
        )
    return pl.DataFrame(rows)


def etw_stack_samples(result: EtwResult) -> list[StackSample]:
    """Folded stacks for the flame graph (present only if a folded export fed in)."""
    return list(result.stack_samples)
