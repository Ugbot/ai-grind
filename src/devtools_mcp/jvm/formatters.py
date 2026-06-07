"""JVM summary formatters — bounded per tool."""

from __future__ import annotations

from collections import Counter

from devtools_mcp.flamegraph.tree import build_call_tree, function_stats, top_leaves
from devtools_mcp.formatters.utils import format_run_header, human_bytes
from devtools_mcp.jvm.models import JvmResult

_TOP = 15


def format_jvm_summary(result: JvmResult) -> str:
    if result.tool in ("jfr", "asprof"):
        return _format_profile(result)
    if result.tool == "threads":
        return _format_threads(result)
    if result.tool == "heap":
        return _format_heap(result)
    return f"Unknown jvm tool: {result.tool}"


def _format_profile(result: JvmResult) -> str:
    parts = [format_run_header(result), ""]
    if not result.stack_samples:
        parts.append("No execution samples captured.")
        return "\n".join(parts)
    total = sum(s.weight for s in result.stack_samples)
    parts.append(f"**Samples:** {total:,} · **PID:** {result.pid}")
    if result.event_counts:
        top_events = sorted(result.event_counts.items(), key=lambda kv: kv[1], reverse=True)[:5]
        parts.append("**Events:** " + ", ".join(f"{k}={v:,}" for k, v in top_events))
    tree = build_call_tree(result.stack_samples)
    parts.append("\n**Hottest methods (Exc% / Inc%):**")
    parts.append("| Exc% | Inc% | method |")
    parts.append("|---:|---:|---|")
    for name, exc, inc in top_leaves(function_stats(tree), _TOP):
        parts.append(f"| {100.0 * exc / total:.1f} | {100.0 * inc / total:.1f} | {name} |")
    parts.append(f'\n_Flame graph: devtools_flamegraph(run_id="{result.run_id}")._')
    return "\n".join(parts)


def _format_threads(result: JvmResult) -> str:
    parts = [format_run_header(result), ""]
    if not result.threads:
        parts.append("No threads parsed.")
        return "\n".join(parts)
    if result.deadlock:
        parts.append("**!! DEADLOCK DETECTED !!**\n")
    states = Counter(t.state or "?" for t in result.threads)
    parts.append(f"**Threads:** {len(result.threads)} · " + ", ".join(f"{k}={v}" for k, v in states.most_common()))
    blocked = [t for t in result.threads if t.state in ("BLOCKED", "WAITING", "TIMED_WAITING")]
    if blocked:
        parts.append(f"\n**Blocked/waiting (top {_TOP}):**")
        for t in blocked[:_TOP]:
            top = t.frames[0] if t.frames else "(no frames)"
            parts.append(f"  - `{t.name}` [{t.state}] → {top}")
    parts.append(f'\n_Query all threads: devtools_analyze(run_id="{result.run_id}")._')
    return "\n".join(parts)


def _format_heap(result: JvmResult) -> str:
    parts = [format_run_header(result), ""]
    if not result.heap_classes:
        parts.append("No heap histogram parsed.")
        return "\n".join(parts)
    parts.append(f"**Classes:** {len(result.heap_classes)} · **Total:** {human_bytes(result.total_bytes)}")
    parts.append("\n**Top consumers by retained bytes:**")
    parts.append("| bytes | instances | class |")
    parts.append("|---:|---:|---|")
    for c in sorted(result.heap_classes, key=lambda c: c.bytes, reverse=True)[:_TOP]:
        parts.append(f"| {human_bytes(c.bytes)} | {c.instances:,} | {c.class_name} |")
    parts.append(f'\n_Query all classes: devtools_analyze(run_id="{result.run_id}")._')
    return "\n".join(parts)
