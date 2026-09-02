"""VTune analysis, Polars DataFrame + flame-graph stacks."""

from __future__ import annotations

import polars as pl

from devtools_mcp.models import StackSample
from devtools_mcp.vtune.models import VtuneResult

_BASE_SCHEMA: dict[str, type[pl.DataType]] = {
    "function": pl.String,
    "module": pl.String,
    "file": pl.String,
    "value": pl.Float64,
}


def vtune_functions_df(result: VtuneResult) -> pl.DataFrame:
    """Function rows as a queryable frame; metric columns vary by analysis type.

    `value` aliases the primary metric so the unified index and sorts work the
    same as every other suite.
    """
    assert isinstance(result, VtuneResult), f"expected VtuneResult, got {type(result)}"
    metric_keys: list[str] = []
    for fn in result.functions:  # bounded by parser MAX_ROWS
        for key in fn.metrics:
            if key not in metric_keys:
                metric_keys.append(key)
    if not result.functions:
        return pl.DataFrame(schema=_BASE_SCHEMA)
    rows = []
    for fn in result.functions:
        row: dict[str, object] = {
            "function": fn.function,
            "module": fn.module,
            "file": fn.source_file,
            "value": fn.primary,
        }
        for key in metric_keys:
            row[key] = fn.metrics.get(key)
        rows.append(row)
    df = pl.DataFrame(rows)
    assert len(df) == len(result.functions), "frame lost function rows"
    return df


def vtune_stack_samples(result: VtuneResult) -> list[StackSample]:
    """Folded stacks from the top-down report (drives devtools_flamegraph)."""
    assert isinstance(result, VtuneResult), f"expected VtuneResult, got {type(result)}"
    return list(result.stack_samples)
