"""Markdown formatters for MCP tool responses.

Converts analysis results into readable markdown text suitable for Claude.
"""

from __future__ import annotations

import polars as pl

from valgrind_mcp.models import (
    CallgrindResult,
    CachegrindResult,
    MassifResult,
    MemcheckError,
    MemcheckResult,
    ThreadCheckResult,
    ThreadError,
    ValgrindResult,
)


def format_run_header(result: ValgrindResult) -> str:
    """Format a run header with basic info."""
    lines = [
        f"**Tool:** {result.tool}",
        f"**Binary:** {result.binary}",
        f"**Duration:** {result.duration_seconds:.1f}s",
        f"**Exit code:** {result.exit_code}",
        f"**Run ID:** `{result.run_id}`",
    ]
    if result.args:
        lines.insert(2, f"**Args:** {' '.join(result.args)}")
    return "\n".join(lines)


def format_memcheck_summary(result: MemcheckResult) -> str:
    """Format memcheck results as a concise actionable summary."""
    parts = [format_run_header(result), ""]

    total_errors = len(result.errors)
    if total_errors == 0:
        parts.append("No memory errors detected.")
        return "\n".join(parts)

    parts.append(f"**{total_errors} error(s) found:**")
    parts.append("")

    # Error summary by kind
    for kind, count in sorted(result.error_summary.items(), key=lambda x: -x[1]):
        parts.append(f"- {kind}: {count}")

    # Leak summary
    if result.leak_summary:
        parts.append("")
        parts.append("**Leak summary:**")
        for leak_kind, leak_bytes in sorted(result.leak_summary.items(), key=lambda x: -x[1]):
            parts.append(f"- {leak_kind}: {_human_bytes(leak_bytes)}")

    # Top 5 errors with locations
    parts.append("")
    parts.append("**Top errors:**")
    for err in result.errors[:5]:
        loc = err.stack[0].location if err.stack else "unknown"
        parts.append(f"- [{err.kind}] {err.what[:100]} at {loc}")

    parts.append("")
    parts.append(f"Use `valgrind_analyze_errors(run_id=\"{result.run_id}\")` for deeper analysis.")
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
    parts.append(f"Use `valgrind_analyze_errors(run_id=\"{result.run_id}\")` for deeper analysis.")
    return "\n".join(parts)


def format_callgrind_summary(result: CallgrindResult) -> str:
    """Format callgrind results as a profiling summary."""
    parts = [format_run_header(result), ""]

    if not result.functions:
        parts.append("No profiling data collected.")
        return "\n".join(parts)

    parts.append(f"**Events:** {', '.join(result.events)}")
    parts.append(f"**Functions profiled:** {len(result.functions)}")

    # Totals
    if result.totals:
        parts.append("")
        parts.append("**Totals:**")
        for event, val in result.totals.items():
            parts.append(f"- {event}: {val:,}")

    # Top 10 hotspots by first event
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
    parts.append(f"Use `valgrind_analyze_hotspots(run_id=\"{result.run_id}\")` for detailed analysis.")
    return "\n".join(parts)


def format_cachegrind_summary(result: CachegrindResult) -> str:
    """Format cachegrind results as a cache summary."""
    parts = [format_run_header(result), ""]

    if not result.lines:
        parts.append("No cache profiling data collected.")
        return "\n".join(parts)

    parts.append(f"**Events:** {', '.join(result.events)}")

    # Summary totals with miss rates
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
        parts.append(f"- Data reads: {dr:,} (L1 miss rate: {d1mr/max(dr,1)*100:.2f}%)")
        parts.append(f"- Data writes: {dw:,} (L1 miss rate: {d1mw/max(dw,1)*100:.2f}%)")
        parts.append(f"- I1 miss rate: {i1mr/max(ir,1)*100:.2f}%")

    parts.append("")
    parts.append(f"Use `valgrind_analyze_cache(run_id=\"{result.run_id}\")` for per-function analysis.")
    return "\n".join(parts)


def format_massif_summary(result: MassifResult) -> str:
    """Format massif results as a memory profile summary."""
    parts = [format_run_header(result), ""]

    if not result.snapshots:
        parts.append("No heap profiling data collected.")
        return "\n".join(parts)

    parts.append(f"**Snapshots:** {len(result.snapshots)}")
    parts.append(f"**Time unit:** {result.time_unit}")

    # Peak info
    peak = None
    for snap in result.snapshots:
        if snap.is_peak:
            peak = snap
            break

    if peak:
        total = peak.heap_bytes + peak.heap_extra_bytes + peak.stacks_bytes
        parts.append("")
        parts.append("**Peak memory usage:**")
        parts.append(f"- Heap: {_human_bytes(peak.heap_bytes)}")
        parts.append(f"- Heap overhead: {_human_bytes(peak.heap_extra_bytes)}")
        parts.append(f"- Stacks: {_human_bytes(peak.stacks_bytes)}")
        parts.append(f"- **Total: {_human_bytes(total)}**")

        if peak.heap_tree:
            parts.append("")
            parts.append("**Top allocations at peak:**")
            _format_alloc_tree(peak.heap_tree, parts, depth=0, max_depth=3)

    # Final snapshot
    if result.snapshots:
        final = result.snapshots[-1]
        final_total = final.heap_bytes + final.heap_extra_bytes + final.stacks_bytes
        parts.append("")
        parts.append(f"**Final heap:** {_human_bytes(final_total)}")

    parts.append("")
    parts.append(f"Use `valgrind_analyze_memory(run_id=\"{result.run_id}\")` for timeline analysis.")
    return "\n".join(parts)


def format_error_details(errors: list[MemcheckError] | list[ThreadError], max_errors: int = 10) -> str:
    """Format detailed error info including full stack traces."""
    parts = []
    for i, err in enumerate(errors[:max_errors]):
        parts.append(f"### Error {i+1}: {err.kind}")
        parts.append(f"**{err.what}**")
        if hasattr(err, "bytes_leaked") and err.bytes_leaked:
            parts.append(f"Leaked: {_human_bytes(err.bytes_leaked)} in {err.blocks_leaked} block(s)")
        if hasattr(err, "thread_id") and err.thread_id:
            parts.append(f"Thread: {err.thread_id}")

        parts.append("")
        parts.append("**Stack:**")
        for frame in err.stack:
            parts.append(f"  {frame.location}")

        if err.auxwhat:
            parts.append("")
            parts.append(f"**{err.auxwhat}**")

        if err.auxstack:
            parts.append("**Auxiliary stack:**")
            for frame in err.auxstack:
                parts.append(f"  {frame.location}")

        parts.append("")

    if len(errors) > max_errors:
        parts.append(f"... and {len(errors) - max_errors} more error(s)")

    return "\n".join(parts)


def format_dataframe(df: pl.DataFrame, title: str = "", max_rows: int = 30) -> str:
    """Format a Polars DataFrame as a markdown table."""
    parts = []
    if title:
        parts.append(f"**{title}**")
        parts.append("")

    if df.is_empty():
        parts.append("No data.")
        return "\n".join(parts)

    # Truncate if needed
    display_df = df.head(max_rows)

    # Build markdown table
    headers = display_df.columns
    parts.append("| " + " | ".join(headers) + " |")
    parts.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for row in display_df.iter_rows():
        cells = []
        for val in row:
            if isinstance(val, float):
                cells.append(f"{val:.2f}")
            elif isinstance(val, int) and abs(val) > 9999:
                cells.append(f"{val:,}")
            else:
                cells.append(str(val) if val is not None else "")
        parts.append("| " + " | ".join(cells) + " |")

    if len(df) > max_rows:
        parts.append(f"\n*Showing {max_rows} of {len(df)} rows*")

    return "\n".join(parts)


def format_comparison(comparison_df: pl.DataFrame, title: str = "Comparison") -> str:
    """Format a comparison DataFrame."""
    return format_dataframe(comparison_df, title=title)


def _format_alloc_tree(alloc, parts: list[str], depth: int = 0, max_depth: int = 3) -> None:
    """Recursively format allocation tree."""
    from valgrind_mcp.models import MassifAllocation
    if depth > max_depth:
        return
    indent = "  " * depth
    fn = alloc.function or "???"
    loc = ""
    if alloc.file and alloc.line:
        loc = f" ({alloc.file}:{alloc.line})"
    elif alloc.file:
        loc = f" ({alloc.file})"
    parts.append(f"{indent}- `{fn}`{loc}: {_human_bytes(alloc.bytes)}")
    for child in alloc.children[:5]:
        _format_alloc_tree(child, parts, depth + 1, max_depth)
    if len(alloc.children) > 5:
        parts.append(f"{indent}  ... and {len(alloc.children) - 5} more")


def _human_bytes(n: int) -> str:
    """Format byte count into human-readable string."""
    if n < 0:
        return f"-{_human_bytes(-n)}"
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            if unit == "B":
                return f"{n} {unit}"
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
