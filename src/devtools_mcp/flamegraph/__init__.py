"""Shared flame-graph engine: folded stacks -> call tree -> SVG + text tree.

Every sampling backend (perf, dtrace profile, ETW, JFR, async-profiler, CDB
thread stacks) produces `models.StackSample`s. This package turns those into a
call tree and renders it two ways: an interactive SVG saved to the workspace for
the human, and a bounded text flame-tree for the LLM. Nothing here floods the
caller — rendering is bounded by min-percent and max-depth.
"""

from devtools_mcp.flamegraph.fold import emit_folded, parse_folded
from devtools_mcp.flamegraph.render_svg import render_svg
from devtools_mcp.flamegraph.render_text import render_text_tree, top_table
from devtools_mcp.flamegraph.tree import CallNode, build_call_tree, focus

__all__ = [
    "CallNode",
    "build_call_tree",
    "emit_folded",
    "focus",
    "parse_folded",
    "render_svg",
    "render_text_tree",
    "top_table",
]
