"""perf-specific formatters."""

from __future__ import annotations

from devtools_mcp.formatters.utils import format_run_header
from devtools_mcp.perf.models import PerfAnnotationResult, PerfRecordResult, PerfStatResult


def format_perf_summary(result: PerfStatResult | PerfRecordResult | PerfAnnotationResult) -> str:
    """Format a perf result summary."""
    if isinstance(result, PerfStatResult):
        return _format_stat(result)
    if isinstance(result, PerfRecordResult):
        return _format_record(result)
    if isinstance(result, PerfAnnotationResult):
        return _format_annotate(result)
    return f"Unknown perf result: {type(result)}"


def _format_stat(result: PerfStatResult) -> str:
    parts = [format_run_header(result), ""]
    if not result.counters:
        parts.append("No counter data collected.")
        return "\n".join(parts)

    parts.append("**Hardware counters:**")
    for c in result.counters:
        val = f"{c.value:,.0f}" if c.value > 100 else f"{c.value:.4f}"
        unit = f" {c.unit}" if c.unit else ""
        var = f" ({c.variance_pct:.2f}% variance)" if c.variance_pct else ""
        parts.append(f"  - {c.event}: {val}{unit}{var}")

    if result.ipc is not None:
        parts.append(f"\n**IPC:** {result.ipc:.2f}")

    parts.append(f'\nUse `devtools_analyze(run_id="{result.run_id}")` for details.')
    return "\n".join(parts)


def _format_record(result: PerfRecordResult) -> str:
    parts = [format_run_header(result), ""]
    if not result.samples:
        parts.append("No sampling data collected.")
        return "\n".join(parts)

    parts.append(f"**Samples:** {result.total_samples:,}")
    parts.append("**Top hotspots:**")
    for s in result.samples[:15]:
        parts.append(f"  - {s.overhead_pct:5.2f}%  `{s.symbol}` ({s.shared_object})")

    parts.append(f'\nUse `devtools_analyze(run_id="{result.run_id}")` for details.')
    return "\n".join(parts)


def _format_annotate(result: PerfAnnotationResult) -> str:
    parts = [format_run_header(result), ""]
    if not result.lines:
        parts.append("No annotation data.")
        return "\n".join(parts)

    parts.append(f"**Symbol:** `{result.symbol}`")
    parts.append("**Hot instructions:**")
    hot_lines = sorted(result.lines, key=lambda x: x.percent, reverse=True)
    for line in hot_lines[:10]:
        if line.percent > 0:
            parts.append(f"  {line.percent:5.2f}%  {line.address}: {line.instruction}")

    parts.append(f'\nUse `devtools_analyze(run_id="{result.run_id}")` for details.')
    return "\n".join(parts)
