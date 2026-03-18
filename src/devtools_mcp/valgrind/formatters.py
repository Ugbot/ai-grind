"""Summary formatters for each Valgrind tool's results."""

from __future__ import annotations

from devtools_mcp.formatters.utils import format_run_header, human_bytes
from devtools_mcp.valgrind.models import (
    CachegrindResult,
    CallgrindResult,
    MassifAllocation,
    MassifResult,
    MemcheckResult,
    ThreadCheckResult,
)


def format_memcheck_summary(result: MemcheckResult) -> str:
    """Format memcheck results as a concise actionable summary."""
    parts = [format_run_header(result), ""]

    total_errors = len(result.errors)
    if total_errors == 0:
        parts.append("No memory errors detected.")
        return "\n".join(parts)

    parts.append(f"**{total_errors} error(s) found:**")
    parts.append("")

    for kind, count in sorted(result.error_summary.items(), key=lambda x: -x[1]):
        parts.append(f"- {kind}: {count}")

    if result.leak_summary:
        parts.append("")
        parts.append("**Leak summary:**")
        for leak_kind, leak_bytes in sorted(result.leak_summary.items(), key=lambda x: -x[1]):
            parts.append(f"- {leak_kind}: {human_bytes(leak_bytes)}")

    parts.append("")
    parts.append("**Top errors:**")
    for err in result.errors[:5]:
        loc = err.stack[0].location if err.stack else "unknown"
        parts.append(f"- [{err.kind}] {err.what[:100]} at {loc}")

    parts.append("")
    parts.append(f'Use `valgrind_analyze_errors(run_id="{result.run_id}")` for deeper analysis.')
    return "\n".join(parts)


def format_threadcheck_summary(result: ThreadCheckResult) -> str:
    """Format helgrind/drd results as a concise summary."""
    parts = [format_run_header(result), ""]

    total_errors = len(result.errors)
    if total_errors == 0:
        parts.append("No thread errors detected.")
        return "\n".join(parts)

    parts.append(f"**{total_errors} thread error(s) found:**")
    parts.append("")

    for kind, count in sorted(result.error_summary.items(), key=lambda x: -x[1]):
        parts.append(f"- {kind}: {count}")

    parts.append("")
    parts.append("**Top errors:**")
    for err in result.errors[:5]:
        loc = err.stack[0].location if err.stack else "unknown"
        tid = f" [thread {err.thread_id}]" if err.thread_id else ""
        parts.append(f"- [{err.kind}]{tid} {err.what[:100]} at {loc}")

    parts.append("")
    parts.append(f'Use `valgrind_analyze_errors(run_id="{result.run_id}")` for deeper analysis.')
    return "\n".join(parts)


def format_callgrind_summary(result: CallgrindResult) -> str:
    """Format callgrind results as a profiling summary."""
    parts = [format_run_header(result), ""]

    if not result.functions:
        parts.append("No profiling data collected.")
        return "\n".join(parts)

    parts.append(f"**Events:** {', '.join(result.events)}")
    parts.append(f"**Functions profiled:** {len(result.functions)}")

    if result.totals:
        parts.append("")
        parts.append("**Totals:**")
        for event, val in result.totals.items():
            parts.append(f"- {event}: {val:,}")

    if result.events:
        primary = result.events[0]
        sorted_fns = sorted(
            result.functions,
            key=lambda f: f.self_cost.get(primary, 0),
            reverse=True,
        )
        parts.append("")
        parts.append(f"**Top 10 hotspots by {primary}:**")
        total = result.totals.get(primary, 1)
        for fn in sorted_fns[:10]:
            cost = fn.self_cost.get(primary, 0)
            pct = cost / total * 100
            loc = f" ({fn.file})" if fn.file else ""
            parts.append(f"- `{fn.name}`{loc}: {cost:,} ({pct:.1f}%)")

    parts.append("")
    parts.append(f'Use `valgrind_analyze_hotspots(run_id="{result.run_id}")` for detailed analysis.')
    return "\n".join(parts)


def format_cachegrind_summary(result: CachegrindResult) -> str:
    """Format cachegrind results as a cache summary."""
    parts = [format_run_header(result), ""]

    if not result.lines:
        parts.append("No cache profiling data collected.")
        return "\n".join(parts)

    parts.append(f"**Events:** {', '.join(result.events)}")

    if result.summary:
        parts.append("")
        parts.append("**Summary:**")
        ir = result.summary.get("Ir", 0)
        i1mr = result.summary.get("I1mr", 0)
        dr = result.summary.get("Dr", 0)
        d1mr = result.summary.get("D1mr", 0)
        dw = result.summary.get("Dw", 0)
        d1mw = result.summary.get("D1mw", 0)

        parts.append(f"- Instructions: {ir:,}")
        parts.append(f"- Data reads: {dr:,} (L1 miss rate: {d1mr / max(dr, 1) * 100:.2f}%)")
        parts.append(f"- Data writes: {dw:,} (L1 miss rate: {d1mw / max(dw, 1) * 100:.2f}%)")
        parts.append(f"- I1 miss rate: {i1mr / max(ir, 1) * 100:.2f}%")

    parts.append("")
    parts.append(f'Use `valgrind_analyze_cache(run_id="{result.run_id}")` for per-function analysis.')
    return "\n".join(parts)


def format_massif_summary(result: MassifResult) -> str:
    """Format massif results as a memory profile summary."""
    parts = [format_run_header(result), ""]

    if not result.snapshots:
        parts.append("No heap profiling data collected.")
        return "\n".join(parts)

    parts.append(f"**Snapshots:** {len(result.snapshots)}")
    parts.append(f"**Time unit:** {result.time_unit}")

    peak = None
    for snap in result.snapshots:
        if snap.is_peak:
            peak = snap
            break

    if peak:
        total = peak.heap_bytes + peak.heap_extra_bytes + peak.stacks_bytes
        parts.append("")
        parts.append("**Peak memory usage:**")
        parts.append(f"- Heap: {human_bytes(peak.heap_bytes)}")
        parts.append(f"- Heap overhead: {human_bytes(peak.heap_extra_bytes)}")
        parts.append(f"- Stacks: {human_bytes(peak.stacks_bytes)}")
        parts.append(f"- **Total: {human_bytes(total)}**")

        if peak.heap_tree:
            parts.append("")
            parts.append("**Top allocations at peak:**")
            _format_alloc_tree(peak.heap_tree, parts, depth=0, max_depth=3)

    if result.snapshots:
        final = result.snapshots[-1]
        final_total = final.heap_bytes + final.heap_extra_bytes + final.stacks_bytes
        parts.append("")
        parts.append(f"**Final heap:** {human_bytes(final_total)}")

    parts.append("")
    parts.append(f'Use `valgrind_analyze_memory(run_id="{result.run_id}")` for timeline analysis.')
    return "\n".join(parts)


def _format_alloc_tree(
    alloc: MassifAllocation,
    parts: list[str],
    depth: int = 0,
    max_depth: int = 3,
) -> None:
    """Recursively format allocation tree."""
    if depth > max_depth:
        return
    indent = "  " * depth
    fn = alloc.function or "???"
    loc = ""
    if alloc.file and alloc.line:
        loc = f" ({alloc.file}:{alloc.line})"
    elif alloc.file:
        loc = f" ({alloc.file})"
    parts.append(f"{indent}- `{fn}`{loc}: {human_bytes(alloc.bytes)}")
    for child in alloc.children[:5]:
        _format_alloc_tree(child, parts, depth + 1, max_depth)
    if len(alloc.children) > 5:
        parts.append(f"{indent}  ... and {len(alloc.children) - 5} more")
