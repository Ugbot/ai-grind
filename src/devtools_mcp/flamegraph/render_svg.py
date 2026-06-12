"""Pure-Python SVG flame-graph (icicle layout, root at top). No dependencies.

Width of each frame is proportional to its inclusive weight; depth grows
downward. Hover shows a tooltip with the full frame name, percentage, and sample
count. The SVG is written to a workspace file and only its path is returned to
the caller — it is never inlined into the LLM response.
"""

from __future__ import annotations

from urllib.parse import quote

from devtools_mcp.flamegraph.tree import CallNode

FRAME_H = 16
PAD = 4
HEADER_H = 34
MIN_PX = 0.4  # frames narrower than this are dropped (bounds output size)
MAX_RECTS = 30_000  # hard cap on emitted rectangles
_PALETTE = ["#eb5a3c", "#f07849", "#f59256", "#f9aa63", "#fbc171", "#fdd884"]


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _color(name: str) -> str:
    return _PALETTE[hash(name) % len(_PALETTE)]


def _layout(root: CallNode, width: int) -> tuple[list[tuple[float, int, float, CallNode]], int]:
    """Return (rects, max_depth). Each rect = (x_px, depth, w_px, node)."""
    total = root.total_weight
    assert total > 0, "cannot lay out an empty tree"
    scale = width / total
    rects: list[tuple[float, int, float, CallNode]] = []
    max_depth = 0
    # DFS preserving left-to-right order; widest child first.
    stack: list[tuple[CallNode, int, float]] = [(root, 0, 0.0)]
    while stack and len(rects) < MAX_RECTS:
        node, depth, x = stack.pop()
        w = node.total_weight * scale
        if w < MIN_PX:
            continue
        rects.append((x, depth, w, node))
        max_depth = max(max_depth, depth)
        child_x = x
        for child in reversed(node.sorted_children()):
            stack.append((child, depth + 1, child_x))
            child_x += child.total_weight * scale
    return rects, max_depth


def render_svg(root: CallNode, title: str = "flamegraph", width: int = 1200, href_base: str | None = None) -> str:
    """Render the call tree as a standalone SVG string (icicle, root at top).

    If `href_base` is set, each frame becomes a link to
    `{href_base}?focus=<name>` so a browser can click-to-zoom (re-root).
    """
    total = root.total_weight
    if total <= 0:
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="40">'
            f'<text x="8" y="24">empty flamegraph: {_esc(title)}</text></svg>'
        )
    rects, max_depth = _layout(root, width - 2 * PAD)
    height = HEADER_H + (max_depth + 1) * FRAME_H + PAD
    ns = 'xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink"'
    parts = [
        f'<svg {ns} width="{width}" height="{height}" font-family="Consolas,monospace" font-size="11">',
        "<style>rect:hover{stroke:#000;stroke-width:0.5}a{cursor:pointer}</style>",
        f'<text x="{PAD}" y="16" font-size="14" font-weight="bold">{_esc(title)}</text>',
        f'<text x="{PAD}" y="29" fill="#888">{total:,} samples · {len(rects):,} frames · '
        f"{'click a frame to zoom · ' if href_base else ''}hover for detail</text>",
    ]
    for x, depth, w, node in rects:
        y = HEADER_H + depth * FRAME_H
        pct = 100.0 * node.total_weight / total
        tip = f"{node.name}\n{pct:.2f}%  ({node.total_weight:,} samples)"
        label = ""
        if w > 36:
            chars = max(0, int(w / 6) - 1)
            label = f'<text x="{x + 2:.1f}" y="{y + 11}" pointer-events="none">' f"{_esc(node.name[:chars])}</text>"
        body = (
            f"<title>{_esc(tip)}</title>"
            f'<rect x="{x:.1f}" y="{y}" width="{max(w - 0.5, 0.5):.1f}" height="{FRAME_H - 1}" '
            f'fill="{_color(node.name)}"/>{label}'
        )
        if href_base:
            parts.append(f'<a xlink:href="{_esc(href_base)}?focus={quote(node.name)}">{body}</a>')
        else:
            parts.append(f"<g>{body}</g>")
    parts.append("</svg>")
    return "\n".join(parts)
