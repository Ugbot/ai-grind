"""Aggregate StackSamples into a call tree and per-function hotspot stats.

The call tree is the flame-graph skeleton: each node is a frame, its width is the
inclusive (total) weight of everything below it, and `self_weight` is the
exclusive (leaf) weight that landed in that frame directly.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field

from devtools_mcp.models import StackSample

MAX_DEPTH = 256  # bound: frames deeper than this are folded into the leaf


@dataclass
class CallNode:
    """One frame in the call tree."""

    name: str
    total_weight: int = 0  # inclusive. This frame and everything it called
    self_weight: int = 0  # exclusive, samples that stopped here
    children: dict[str, CallNode] = field(default_factory=dict)

    def child(self, name: str) -> CallNode:
        """Get or create a child by frame name."""
        node = self.children.get(name)
        if node is None:
            node = CallNode(name=name)
            self.children[name] = node
        return node

    def sorted_children(self) -> list[CallNode]:
        """Children by descending inclusive weight (widest first)."""
        return sorted(self.children.values(), key=lambda c: c.total_weight, reverse=True)


def build_call_tree(samples: list[StackSample], root_name: str = "all") -> CallNode:
    """Fold samples into a call tree. Root inclusive == sum of all weights."""
    assert isinstance(samples, list), "samples must be a list"
    root = CallNode(name=root_name)
    for s in samples:
        if not s.frames or s.weight <= 0:
            continue
        frames = s.frames[:MAX_DEPTH]
        root.total_weight += s.weight
        node = root
        for frame in frames:
            node = node.child(frame)
            node.total_weight += s.weight
        node.self_weight += s.weight  # leaf of this stack
    assert root.total_weight >= 0, "negative total weight"
    return root


def walk(root: CallNode) -> Iterator[tuple[CallNode, int]]:
    """Depth-first iterate (node, depth); root is depth 0. Bounded by MAX_DEPTH."""
    stack: list[tuple[CallNode, int]] = [(root, 0)]
    seen = 0
    while stack:
        node, depth = stack.pop()
        yield node, depth
        seen += 1
        if depth >= MAX_DEPTH:
            continue
        for child in node.sorted_children():
            stack.append((child, depth + 1))
    assert seen >= 1, "walk produced no nodes"


def function_stats(root: CallNode) -> dict[str, tuple[int, int]]:
    """Per-function (exclusive, inclusive) weights aggregated across all paths.

    Inclusive is counted only at the top-most occurrence on each path, so direct
    or indirect recursion is not double-counted.
    """
    stats: dict[str, list[int]] = {}
    # (node, ancestor_names), explicit stack, bounded by tree depth.
    work: list[tuple[CallNode, frozenset[str]]] = [(root, frozenset())]
    while work:
        node, ancestors = work.pop()
        if node.name != root.name or node is not root:
            entry = stats.setdefault(node.name, [0, 0])
            entry[0] += node.self_weight
            if node.name not in ancestors:
                entry[1] += node.total_weight
        child_ancestors = ancestors | {node.name}
        for child in node.children.values():
            work.append((child, child_ancestors))
    return {name: (e[0], e[1]) for name, e in stats.items()}


def top_leaves(stats: dict[str, tuple[int, int]], n: int = 20) -> list[tuple[str, int, int]]:
    """Functions ranked by exclusive weight (where cycles actually burn)."""
    rows = [(name, exc, inc) for name, (exc, inc) in stats.items()]
    rows.sort(key=lambda r: r[1], reverse=True)
    return rows[: max(0, n)]


def top_dispatchers(stats: dict[str, tuple[int, int]], n: int = 20) -> list[tuple[str, int, int]]:
    """Functions ranked by inclusive-minus-exclusive (time spent in callees)."""
    rows = [(name, exc, inc) for name, (exc, inc) in stats.items()]
    rows.sort(key=lambda r: r[2] - r[1], reverse=True)
    return rows[: max(0, n)]


def _merge_into(dst: CallNode, src: CallNode) -> None:
    """Merge src's weights/children into dst (same logical frame)."""
    dst.total_weight += src.total_weight
    dst.self_weight += src.self_weight
    for name, child in src.children.items():
        existing = dst.children.get(name)
        if existing is None:
            dst.children[name] = child
        else:
            _merge_into(existing, child)


def focus(root: CallNode, name: str) -> CallNode | None:
    """Re-root the tree at `name`: merge every top-most subtree with that frame.

    Powers click-to-zoom, clicking a frame shows only what ran beneath it.
    Returns None if the frame isn't present.
    """
    focused = CallNode(name=name)
    found = False
    # Top-most occurrences only (don't double-count nested same-name frames).
    stack: list[CallNode] = [root]
    while stack:
        node = stack.pop()
        if node.name == name and node is not root:
            found = True
            _merge_into(focused, node)
            continue  # don't descend, its subtree is already merged
        stack.extend(node.children.values())
    return focused if found else None


def function_frame(samples: list) -> object:
    """Per-function DataFrame from stack samples: self/total counts + % share.

    The universal function-level view for any sampling backend that produces
    StackSamples (dtrace profile, perf record, etw cpu, jvm jfr/asprof, cdb
    stacks). Percent columns are normalized by the run's own total weight so
    two runs with different sample counts compare fairly.
    """
    import polars as pl

    root = build_call_tree(samples, root_name="all")
    stats = function_stats(root)
    stats.pop("all", None)
    total = sum(s.weight for s in samples) or 1
    rows = [
        {
            "function": name,
            "self": self_w,
            "total": tot_w,
            "self_pct": round(100.0 * self_w / total, 2),
            "total_pct": round(100.0 * tot_w / total, 2),
        }
        for name, (self_w, tot_w) in stats.items()
    ]
    if not rows:
        return pl.DataFrame(
            schema={"function": pl.Utf8, "self": pl.Int64, "total": pl.Int64,
                    "self_pct": pl.Float64, "total_pct": pl.Float64})
    return pl.DataFrame(rows).sort("total", descending=True)
