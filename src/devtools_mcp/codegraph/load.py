"""Load and index a native `knowledge-graph.json` export for viewing.

Pure, bounded, dependency-free. The graph can be large, so the view is
ego-centric: pick a focus node and show its neighbourhood (the SVG renderer
uses `ego()`), keeping any single page bounded.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

MAX_NODES: int = 50_000
MAX_EDGES: int = 200_000
EGO_MAX_NODES: int = 60  # nodes shown around a focus in one view


@dataclass
class CodeGraph:
    """Indexed view of a code property graph export."""

    nodes: dict[str, dict]  # id -> node object
    edges: list[dict]  # edge objects
    out_adj: dict[str, list[dict]] = field(default_factory=dict)  # src id -> edges
    in_adj: dict[str, list[dict]] = field(default_factory=dict)  # dst id -> edges

    def degree(self, node_id: str) -> int:
        return len(self.out_adj.get(node_id, ())) + len(self.in_adj.get(node_id, ()))

    def top_node(self) -> str | None:
        """The highest-degree node — a good default focus."""
        if not self.nodes:
            return None
        return max(self.nodes, key=self.degree)

    def ego(self, focus: str, hops: int = 1, max_nodes: int = EGO_MAX_NODES) -> tuple[dict[str, int], list[dict]]:
        """Return the neighbourhood of `focus`: a `{node_id: column}` placement
        (column < 0 = dependents/in, 0 = focus, > 0 = dependencies/out) and the
        edges among the included nodes. Bounded by `max_nodes`.
        """
        assert focus in self.nodes, f"unknown focus node: {focus!r}"
        placement: dict[str, int] = {focus: 0}
        # BFS outward (dependencies, positive columns) and inward (dependents).
        for adj, sign, endpoint in ((self.out_adj, 1, "target"), (self.in_adj, -1, "source")):
            frontier = [focus]
            for level in range(1, hops + 1):
                nxt: list[str] = []
                for cur in frontier:
                    for edge in adj.get(cur, ()):  # bounded by graph size
                        other = str(edge.get(endpoint, ""))
                        if not other or other in placement:
                            continue
                        if len(placement) >= max_nodes:
                            break
                        placement[other] = sign * level
                        nxt.append(other)
                    if len(placement) >= max_nodes:
                        break
                frontier = nxt
                if not frontier or len(placement) >= max_nodes:
                    break
        included = set(placement)
        sub_edges = [
            e for e in self.edges if str(e.get("source", "")) in included and str(e.get("target", "")) in included
        ]
        assert len(placement) <= max_nodes, "ego exceeded node bound"
        return placement, sub_edges


def load_graph(data: str | dict) -> CodeGraph:
    """Parse a `knowledge-graph.json` string or dict into an indexed CodeGraph."""
    obj = json.loads(data) if isinstance(data, str) else data
    if not isinstance(obj, dict):
        raise ValueError("knowledge-graph must be a JSON object")
    nodes: dict[str, dict] = {}
    for n in (obj.get("nodes") or [])[:MAX_NODES]:
        node_id = str(n.get("id", ""))
        if node_id:
            nodes[node_id] = n
    edges: list[dict] = []
    out_adj: dict[str, list[dict]] = {}
    in_adj: dict[str, list[dict]] = {}
    for e in (obj.get("edges") or [])[:MAX_EDGES]:
        src = str(e.get("source", ""))
        dst = str(e.get("target", ""))
        if not src or not dst:
            continue
        edges.append(e)
        out_adj.setdefault(src, []).append(e)
        in_adj.setdefault(dst, []).append(e)
    return CodeGraph(nodes=nodes, edges=edges, out_adj=out_adj, in_adj=in_adj)
