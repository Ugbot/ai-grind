"""Bounded text rendering of a call tree for the LLM.

Two views, both bounded so they never flood the caller:
- `render_text_tree`: an indented flame-tree, pruned below `min_pct` and capped
  at `max_depth` / `max_nodes`.
- `top_table`: a markdown top-N table of hottest functions (Exc% / Inc%).
"""

from __future__ import annotations

from devtools_mcp.flamegraph.tree import CallNode, function_stats, top_leaves

MAX_NODES = 300  # bound: never print more tree rows than this
_BAR_WIDTH = 20


def _bar(pct: float) -> str:
    # ASCII only, the tree may be written to non-UTF-8 streams on some hosts.
    filled = int(round(pct / 100.0 * _BAR_WIDTH))
    filled = max(0, min(_BAR_WIDTH, filled))
    return "#" * filled + "." * (_BAR_WIDTH - filled)


def render_text_tree(
    root: CallNode,
    min_pct: float = 0.5,
    max_depth: int = 32,
    max_nodes: int = MAX_NODES,
) -> str:
    """Indented flame-tree, widest child first, pruned below min_pct."""
    assert root.total_weight >= 0, "tree has negative weight"
    total = root.total_weight
    if total == 0:
        return "_(no samples)_"
    cap = min(max_nodes, MAX_NODES)
    lines: list[str] = []
    truncated = False
    # Pre-order DFS; push children reversed so widest is emitted first.
    stack: list[tuple[CallNode, int]] = [(root, 0)]
    while stack:
        node, depth = stack.pop()
        pct = 100.0 * node.total_weight / total
        self_pct = 100.0 * node.self_weight / total
        indent = "  " * depth
        lines.append(f"{indent}{_bar(pct)} {pct:5.1f}% {node.name}  (self {self_pct:.1f}%)")
        if len(lines) >= cap:
            truncated = True
            break
        if depth >= max_depth:
            continue
        kids = [c for c in node.sorted_children() if 100.0 * c.total_weight / total >= min_pct]
        for child in reversed(kids):
            stack.append((child, depth + 1))
    if truncated:
        lines.append(f"… tree truncated at {cap} rows, narrow with min_pct/max_depth or use devtools_analyze")
    return "\n".join(lines)


def top_table(root: CallNode, n: int = 15) -> str:
    """Markdown table of the hottest functions by exclusive weight."""
    total = root.total_weight
    if total == 0:
        return "_(no samples)_"
    stats = function_stats(root)
    rows = top_leaves(stats, n)
    out = ["| Exc% | Inc% | function |", "|---:|---:|---|"]
    for name, exc, inc in rows:
        out.append(f"| {100.0 * exc / total:.1f} | {100.0 * inc / total:.1f} | {name} |")
    return "\n".join(out)
