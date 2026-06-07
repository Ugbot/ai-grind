"""Node analysis — hotspots DataFrame + flame-graph stacks."""

from __future__ import annotations

import polars as pl

from devtools_mcp.hotspots import hotspots_df
from devtools_mcp.models import StackSample
from devtools_mcp.node.models import NodeResult


def node_hotspots_df(result: NodeResult) -> pl.DataFrame:
    """Per-function hotspots from the V8 profile stacks."""
    return hotspots_df(result.stack_samples)


def node_stack_samples(result: NodeResult) -> list[StackSample]:
    """Flame-graph stacks (CPU samples or heap allocation bytes)."""
    return list(result.stack_samples)
