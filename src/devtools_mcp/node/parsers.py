"""Parsers for V8 profiles: .cpuprofile (CPU) and .heapprofile (sampling heap).

Both are the same JSON shapes Chrome DevTools / `node --cpu-prof|--heap-prof`
emit, so they're directly synthesizable for tests.
"""

from __future__ import annotations

import json
import os

from devtools_mcp.models import StackSample

MAX_NODES = 2_000_000  # bound


def _frame(call_frame: dict) -> str:
    """Human frame name from a V8 callFrame."""
    name = (call_frame or {}).get("functionName") or ""
    if name:
        return name
    url = (call_frame or {}).get("url") or ""
    base = os.path.basename(url) if url else ""
    line = (call_frame or {}).get("lineNumber")
    if base:
        return f"(anonymous {base}:{line})" if line is not None else f"(anonymous {base})"
    return "(anonymous)"


def parse_cpuprofile(text: str) -> list[StackSample]:
    """Parse a V8 .cpuprofile into folded StackSamples (one weight per sample)."""
    assert isinstance(text, str), "cpuprofile must be str"
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []
    nodes = {n["id"]: n for n in data.get("nodes", [])}
    assert len(nodes) <= MAX_NODES, f"too many cpuprofile nodes: {len(nodes)}"
    parent: dict[int, int] = {}
    for n in nodes.values():
        for child in n.get("children", []) or []:
            parent[child] = n["id"]

    def root_path(node_id: int) -> list[str]:
        frames: list[str] = []
        seen: set[int] = set()
        cur: int | None = node_id
        while cur is not None and cur in nodes and cur not in seen:
            seen.add(cur)
            frames.append(_frame(nodes[cur].get("callFrame", {})))
            cur = parent.get(cur)
        frames.reverse()  # root-first
        return frames

    agg: dict[tuple[str, ...], int] = {}
    samples = data.get("samples") or []
    if samples:
        for nid in samples:
            if nid in nodes:
                key = tuple(root_path(nid))
                agg[key] = agg.get(key, 0) + 1
    else:  # fall back to per-node hitCount
        for nid, n in nodes.items():
            hits = n.get("hitCount") or 0
            if hits > 0:
                agg_key = tuple(root_path(nid))
                agg[agg_key] = agg.get(agg_key, 0) + hits
    return [StackSample(frames=list(k), weight=w) for k, w in agg.items() if k]


def parse_heapprofile(text: str) -> list[StackSample]:
    """Parse a V8 .heapprofile (sampling allocations) into StackSamples by bytes."""
    assert isinstance(text, str), "heapprofile must be str"
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []
    head = data.get("head")
    if not head:
        return []
    samples: list[StackSample] = []
    # DFS carrying the root->node frame path; weight = selfSize (bytes).
    stack: list[tuple[dict, list[str]]] = [(head, [])]
    while stack:
        node, path = stack.pop()
        frames = [*path, _frame(node.get("callFrame", {}))]
        self_size = int(node.get("selfSize") or 0)
        if self_size > 0:
            samples.append(StackSample(frames=frames, weight=self_size))
        for child in node.get("children", []) or []:
            stack.append((child, frames))
    return samples
