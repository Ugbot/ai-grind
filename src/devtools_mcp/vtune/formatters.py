"""VTune summary formatters — bounded top-N tables, never the full report."""

from __future__ import annotations

from devtools_mcp.formatters.utils import format_run_header
from devtools_mcp.vtune.models import VtuneResult

_TOP = 15
_SUMMARY_MAX_LINES = 25


def format_vtune_summary(result: VtuneResult) -> str:
    """Top functions by the primary metric + the interesting summary lines."""
    assert isinstance(result, VtuneResult), f"expected VtuneResult, got {type(result)}"
    parts = [format_run_header(result), ""]
    parts.append(f"**Analysis:** {result.analysis_type} · {len(result.functions)} function rows")
    if result.result_dir:
        parts.append(f"**Result dir:** `{result.result_dir}` (open in the VTune GUI for the full picture)")

    if result.functions:
        metric_keys = list(result.functions[0].metrics.keys())[:3]
        header = " | ".join(metric_keys) if metric_keys else "value"
        top = sorted(result.functions, key=lambda f: f.primary, reverse=True)[:_TOP]
        parts.append(f"\n**Top functions ({metric_keys[0] if metric_keys else 'primary'}):**")
        parts.append(f"| function | module | {header} |")
        parts.append("|---|---|" + "---|" * max(len(metric_keys), 1))
        for fn in top:
            if fn.primary <= 0:
                break
            cells = (
                " | ".join(f"{fn.metrics.get(k, 0):.3f}" for k in metric_keys) if metric_keys else f"{fn.primary:.3f}"
            )
            name = fn.function if len(fn.function) <= 90 else fn.function[:87] + "..."
            parts.append(f"| {name} | {fn.module} | {cells} |")
    else:
        parts.append("\nNo function rows parsed — see the summary below / check symbols.")

    summary_lines = [line for line in result.summary_text.splitlines() if line.strip()]
    if summary_lines:
        parts.append("\n**Summary (first lines):**")
        parts.append("```")
        parts.extend(summary_lines[:_SUMMARY_MAX_LINES])
        if len(summary_lines) > _SUMMARY_MAX_LINES:
            parts.append(f"... {len(summary_lines) - _SUMMARY_MAX_LINES} more lines (devtools_raw)")
        parts.append("```")

    if result.stack_samples:
        parts.append(f'\n_Flame graph: devtools_flamegraph(run_id="{result.run_id}")._')
    parts.append(f'_Query all rows: devtools_analyze(run_id="{result.run_id}")._')
    return "\n".join(parts)
