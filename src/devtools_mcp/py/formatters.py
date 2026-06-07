"""Python summary formatters — bounded per tool."""

from __future__ import annotations

from collections import Counter

from devtools_mcp.flamegraph.tree import build_call_tree, function_stats, top_leaves
from devtools_mcp.formatters.utils import format_run_header
from devtools_mcp.py.models import PyResult

_TOP = 15


def format_py_summary(result: PyResult) -> str:
    if result.tool in ("cpu", "pyspy", "memray"):
        return _format_sampling(result)
    if result.tool == "cprofile":
        return _format_cprofile(result)
    if result.tool in ("threads", "dump"):
        return _format_dump(result)
    return f"Unknown py tool: {result.tool}"


def _format_sampling(result: PyResult) -> str:
    parts = [format_run_header(result), ""]
    if not result.stack_samples:
        parts.append("No samples captured.")
        return "\n".join(parts)
    total = sum(s.weight for s in result.stack_samples)
    parts.append(f"**Samples:** {total:,}" + (f" · **PID:** {result.pid}" if result.pid else ""))
    tree = build_call_tree(result.stack_samples)
    parts.append("\n**Hottest functions (Exc% / Inc%):**")
    parts.append("| Exc% | Inc% | function |")
    parts.append("|---:|---:|---|")
    for name, exc, inc in top_leaves(function_stats(tree), _TOP):
        parts.append(f"| {100.0 * exc / total:.1f} | {100.0 * inc / total:.1f} | {name} |")
    parts.append(f'\n_Flame graph: devtools_flamegraph(run_id="{result.run_id}")._')
    return "\n".join(parts)


def _format_cprofile(result: PyResult) -> str:
    parts = [format_run_header(result), ""]
    if not result.func_stats:
        parts.append("No profile data.")
        return "\n".join(parts)
    parts.append(f"**Functions profiled:** {len(result.func_stats)}")
    parts.append("\n**Top by cumulative time:**")
    parts.append("| cumtime | tottime | ncalls | function |")
    parts.append("|---:|---:|---:|---|")
    for s in result.func_stats[:_TOP]:
        parts.append(f"| {s.cumtime:.4f} | {s.tottime:.4f} | {s.ncalls:,} | {s.function} |")
    parts.append(f'\n_Query all functions: devtools_analyze(run_id="{result.run_id}")._')
    return "\n".join(parts)


def _format_dump(result: PyResult) -> str:
    parts = [format_run_header(result), ""]
    if not result.threads:
        parts.append("No threads parsed.")
        return "\n".join(parts)
    states = Counter(t.state or "?" for t in result.threads)
    parts.append(f"**Threads:** {len(result.threads)} · "
                 + ", ".join(f"{k}={v}" for k, v in states.most_common()))
    for t in result.threads[:_TOP]:
        top = t.frames[0] if t.frames else "(no frames)"
        parts.append(f"  - `{t.name or t.tid}` [{t.state}] → {top}")
    parts.append(f'\n_Query all threads: devtools_analyze(run_id="{result.run_id}")._')
    return "\n".join(parts)
