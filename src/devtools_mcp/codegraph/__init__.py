"""Thin consumer of the native code property graph.

LLM Station builds the graph natively (MarbleDB) and emits `knowledge-graph.json`
via its `graph_export` tool. This package only *reads* that export and renders a
server-rendered SVG graph view for the dashboard — no parsing, no graph
construction happens in Python (that is the native engine's job).
"""

from devtools_mcp.codegraph.load import CodeGraph, load_graph
from devtools_mcp.codegraph.render_graph_svg import render_graph_svg

__all__ = ["CodeGraph", "load_graph", "render_graph_svg"]
