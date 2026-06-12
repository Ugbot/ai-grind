"""Shared: turn flame-graph StackSamples into a queryable hotspots DataFrame.

Reused by sampling backends (jvm/py/node) so per-function Exc/Inc tables are
consistent and searchable (`function`/`value` aliases for the unified index).
"""

from __future__ import annotations

import polars as pl

from devtools_mcp.flamegraph.tree import build_call_tree, function_stats
from devtools_mcp.models import StackSample

_SCHEMA = {
    "function": pl.Utf8,
    "exclusive": pl.Int64,
    "inclusive": pl.Int64,
    "exc_pct": pl.Float64,
    "inc_pct": pl.Float64,
    "value": pl.Float64,
}


def hotspots_df(samples: list[StackSample]) -> pl.DataFrame:
    """Per-function exclusive/inclusive weights + percentages, sorted by Exc."""
    assert isinstance(samples, list), "samples must be a list"
    tree = build_call_tree(samples)
    total = tree.total_weight or 1
    rows = [
        {
            "function": name,
            "exclusive": exc,
            "inclusive": inc,
            "exc_pct": 100.0 * exc / total,
            "inc_pct": 100.0 * inc / total,
            "value": 100.0 * exc / total,  # alias for the unified index
        }
        for name, (exc, inc) in function_stats(tree).items()
    ]
    if not rows:
        return pl.DataFrame(schema=_SCHEMA)
    return pl.DataFrame(rows).sort("exclusive", descending=True)
