"""Server-rendered SVG of a code property graph. No JS graph lib, no CDN.

Ego-centric, click-to-focus: the focus node sits in the centre column, its
dependencies fan right, its dependents fan left, and every node is a link that
re-roots the view (`?focus=<id>`), the same interaction model as the flamegraph
(`flamegraph/render_svg.py`), which is the only precedent for a drawn graphic in
this strictly server-rendered dashboard.
"""

from __future__ import annotations

import html
import urllib.parse

from devtools_mcp.codegraph.load import CodeGraph

_COL_W = 300
_ROW_H = 46
_NODE_R = 8
_MARGIN = 28
_LABEL_MAX = 34

# node type -> fill colour (dashboard dark theme accents)
_TYPE_COLOR = {
    "function": "#58a6ff",
    "class": "#bc8cff",
    "file": "#e3b341",
    "module": "#3fb950",
    "service": "#f85149",
    "table": "#d29922",
    "endpoint": "#39c5cf",
}
_DEFAULT_COLOR = "#8b949e"


def _color(node_type: str) -> str:
    return _TYPE_COLOR.get(node_type, _DEFAULT_COLOR)


def _label(name: str) -> str:
    name = name or "?"
    return name if len(name) <= _LABEL_MAX else name[: _LABEL_MAX - 1] + "…"


def render_graph_svg(graph: CodeGraph, focus: str | None = None, hops: int = 1, href_base: str = "/graph") -> str:
    """Render the neighbourhood of `focus` (or the highest-degree node) as SVG."""
    if not graph.nodes:
        return "<p class='note'>Empty graph. Run <code>graph_build</code> then <code>graph_export</code>.</p>"
    if focus is None or focus not in graph.nodes:
        focus = graph.top_node()
    assert focus is not None
    placement, edges = graph.ego(focus, hops=hops)

    # Bucket nodes by column (signed BFS level); assign a row within each column.
    columns: dict[int, list[str]] = {}
    for node_id, col in placement.items():
        columns.setdefault(col, []).append(node_id)
    for ids in columns.values():
        ids.sort(key=lambda i: str(graph.nodes[i].get("name", i)))

    ordered_cols = sorted(columns)
    col_index = {col: idx for idx, col in enumerate(ordered_cols)}
    max_rows = max((len(ids) for ids in columns.values()), default=1)

    # Node id -> (x, y) centre.
    pos: dict[str, tuple[float, float]] = {}
    for col, ids in columns.items():
        cx = _MARGIN + col_index[col] * _COL_W + _NODE_R
        for row, node_id in enumerate(ids):
            cy = _MARGIN + row * _ROW_H + _NODE_R
            pos[node_id] = (cx, cy)

    width = _MARGIN * 2 + (len(ordered_cols) - 1) * _COL_W + _COL_W
    height = _MARGIN * 2 + max_rows * _ROW_H

    parts: list[str] = [
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{int(width)}' height='{int(height)}' "
        f"viewBox='0 0 {int(width)} {int(height)}' font-family='ui-monospace,monospace' font-size='11'>"
    ]
    # Edges first (under nodes).
    for e in edges:
        s, t = str(e.get("source", "")), str(e.get("target", ""))
        if s not in pos or t not in pos:
            continue
        x1, y1 = pos[s]
        x2, y2 = pos[t]
        parts.append(
            f"<line x1='{x1:.0f}' y1='{y1:.0f}' x2='{x2:.0f}' y2='{y2:.0f}' "
            f"stroke='#30363d' stroke-width='1'><title>{html.escape(str(e.get('type', '')))}</title></line>"
        )
    # Nodes (each a focus link).
    for node_id, (cx, cy) in pos.items():
        node = graph.nodes[node_id]
        ntype = str(node.get("type", ""))
        is_focus = node_id == focus
        fill = _color(ntype)
        r = _NODE_R + 2 if is_focus else _NODE_R
        stroke = "#e6edf3" if is_focus else "none"
        href = f"{href_base}?focus={urllib.parse.quote(node_id, safe='')}"
        tip = html.escape(f"{node_id}  [{ntype}]")
        label = html.escape(_label(str(node.get("name", node_id))))
        parts.append(
            f"<a xlink:href='{html.escape(href)}' href='{html.escape(href)}'>"
            f"<circle cx='{cx:.0f}' cy='{cy:.0f}' r='{r}' fill='{fill}' stroke='{stroke}' "
            f"stroke-width='2'><title>{tip}</title></circle>"
            f"<text x='{cx + r + 4:.0f}' y='{cy + 4:.0f}' fill='#e6edf3'>{label}</text></a>"
        )
    parts.append("</svg>")
    return "".join(parts)
