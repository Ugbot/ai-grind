"""ETW summary formatters — bounded top-N Exc%/Inc% tables."""

from __future__ import annotations

from devtools_mcp.etw.models import EtwResult
from devtools_mcp.etw.parsers import is_synthetic, shorten
from devtools_mcp.formatters.utils import format_run_header

_TOP = 15


def format_etw_summary(result: EtwResult) -> str:
    """Top CPU leaves (Exc%) and dispatchers (Inc%) — never the full table."""
    parts = [format_run_header(result), ""]
    real = [s for s in result.samples if not is_synthetic(s.name)]
    if not real:
        parts.append("No CPU samples decoded. Check PDB/symbol resolution.")
        return "\n".join(parts)

    parts.append(f"**Process:** {result.process or '?'} · {len(real)} resolved nodes")
    if result.etl_path:
        parts.append(f"**ETL:** `{result.etl_path}`")

    by_exc = sorted(real, key=lambda s: s.exc_pct, reverse=True)[:_TOP]
    parts.append("\n**Hottest leaves (Exc% — where cycles burn):**")
    parts.append("| Exc% | Inc% | function |")
    parts.append("|---:|---:|---|")
    for s in by_exc:
        if s.exc_pct < 0.1:
            break
        parts.append(f"| {s.exc_pct:.2f} | {s.inc_pct:.2f} | {shorten(s.name)} |")

    by_inc = sorted(real, key=lambda s: s.inc_pct - s.exc_pct, reverse=True)[:_TOP]
    parts.append("\n**Top dispatchers (Inc% — time in callees):**")
    parts.append("| Inc% | Exc% | function |")
    parts.append("|---:|---:|---|")
    for s in by_inc:
        if s.inc_pct < 0.5:
            break
        parts.append(f"| {s.inc_pct:.2f} | {s.exc_pct:.2f} | {shorten(s.name)} |")

    if result.stack_samples:
        parts.append(f'\n_Flame graph: devtools_flamegraph(run_id="{result.run_id}")._')
    parts.append(f'_Query all nodes: devtools_analyze(run_id="{result.run_id}")._')
    return "\n".join(parts)
