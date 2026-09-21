"""Shared flame-graph engine: folded stacks -> call tree -> text tree.

Every sampling backend (perf, dtrace profile, ETW, JFR, async-profiler, CDB
thread stacks) produces `models.StackSample`s. This package turns those into a
call tree and renders it as a bounded text flame-tree for agents.

The SVG renderer (`render_svg`) still exists in `flamegraph.render_svg` and is
used by the browser visualization terminal (_flame route in viz/server.py), but
it is not part of the MCP tool API, the MCP tool returns text or folded-stack
JSON (for GUI / ImPlot consumers).
"""

from devtools_mcp.flamegraph.fold import emit_folded, parse_folded
from devtools_mcp.flamegraph.render_text import render_text_tree, top_table
from devtools_mcp.flamegraph.sample_filter import StackFilter, filter_samples
from devtools_mcp.flamegraph.tree import CallNode, build_call_tree, focus

__all__ = [
    "CallNode",
    "StackFilter",
    "build_call_tree",
    "emit_folded",
    "filter_samples",
    "focus",
    "parse_folded",
    "render_text_tree",
    "top_table",
]
