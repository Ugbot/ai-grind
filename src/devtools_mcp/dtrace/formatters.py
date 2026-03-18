"""DTrace-specific formatters."""

from __future__ import annotations

from devtools_mcp.dtrace.models import DTraceResult
from devtools_mcp.formatters.utils import format_run_header


def format_dtrace_summary(result: DTraceResult) -> str:
    """Format a concise DTrace result summary."""
    parts = [format_run_header(result), ""]

    if result.one_liner:
        parts.append(f"**One-liner:** `{result.one_liner}`")
    elif result.script:
        parts.append(f"**Script:** `{result.script}`")

    if result.aggregations:
        parts.append(f"**Aggregations:** {len(result.aggregations)} entries")
        # Show top 10 by value
        sorted_aggs = sorted(result.aggregations, key=lambda a: a.value, reverse=True)
        for agg in sorted_aggs[:10]:
            key_str = " ".join(agg.keys)
            parts.append(f"  - {key_str}: {agg.value:,}")
        if len(result.aggregations) > 10:
            parts.append(f"  ... and {len(result.aggregations) - 10} more")

    if result.stacks:
        parts.append(f"**Stack traces:** {len(result.stacks)}")
        sorted_stacks = sorted(result.stacks, key=lambda s: s.count, reverse=True)
        for stack in sorted_stacks[:5]:
            top = stack.frames[0] if stack.frames else "?"
            parts.append(f"  - {top} ({stack.count:,}x, {len(stack.frames)} frames)")

    if result.quantizations:
        parts.append(f"**Distributions:** {len(result.quantizations)}")
        for quant in result.quantizations[:3]:
            parts.append(f"  - {quant.key or 'default'}: {quant.total:,} total across {len(quant.buckets)} buckets")

    if result.probe_hits:
        parts.append(f"**Probe hits:** {len(result.probe_hits)}")

    if not any([result.aggregations, result.stacks, result.quantizations, result.probe_hits]):
        # Show raw output truncated
        raw_lines = result.raw_output.strip().splitlines()
        non_dtrace = [ln for ln in raw_lines if not ln.startswith("dtrace:") and ln.strip()]
        if non_dtrace:
            parts.append("**Output:**")
            for ln in non_dtrace[:15]:
                parts.append(f"  {ln}")
            if len(non_dtrace) > 15:
                parts.append(f"  ... ({len(non_dtrace)} lines total)")
        else:
            parts.append("No structured data captured.")

    parts.append("")
    parts.append(f'Use `devtools_analyze(run_id="{result.run_id}")` to drill in.')
    return "\n".join(parts)
