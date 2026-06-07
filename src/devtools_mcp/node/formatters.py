"""Node summary formatters — bounded."""

from __future__ import annotations

from devtools_mcp.flamegraph.tree import build_call_tree, function_stats, top_leaves
from devtools_mcp.formatters.utils import format_run_header, human_bytes
from devtools_mcp.node.models import NodeResult

_TOP = 15


def format_node_summary(result: NodeResult) -> str:
    parts = [format_run_header(result), ""]
    if not result.stack_samples:
        parts.append("No profile data captured.")
        return "\n".join(parts)
    is_heap = result.weight_unit == "bytes"
    total = sum(s.weight for s in result.stack_samples)
    headline = human_bytes(total) if is_heap else f"{total:,} samples"
    parts.append(f"**{'Allocations' if is_heap else 'CPU'}:** {headline}")

    tree = build_call_tree(result.stack_samples)
    col = "bytes" if is_heap else "Exc%"
    parts.append(f"\n**Hottest functions ({col}):**")
    parts.append(f"| {col} | Inc% | function |")
    parts.append("|---:|---:|---|")
    for name, exc, inc in top_leaves(function_stats(tree), _TOP):
        left = human_bytes(exc) if is_heap else f"{100.0 * exc / total:.1f}"
        parts.append(f"| {left} | {100.0 * inc / total:.1f} | {name} |")
    parts.append(f'\n_Flame graph: devtools_flamegraph(run_id="{result.run_id}")._')
    parts.append(f'_Query all: devtools_analyze(run_id="{result.run_id}")._')
    return "\n".join(parts)
