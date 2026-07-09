"""RenderDoc summary formatters — bounded, with copy-pasteable next steps."""

from __future__ import annotations

from pathlib import Path

from devtools_mcp.formatters.utils import format_run_header, human_bytes
from devtools_mcp.models import RunBase
from devtools_mcp.renderdoc.models import (
    RenderdocCaptureResult,
    RenderdocReplayResult,
    RenderdocThumbResult,
)

_TOP_DRAWS = 10
_TOP_RESOURCES = 5


def format_renderdoc_summary(result: RunBase) -> str:
    """Dispatch on the concrete result type."""
    if isinstance(result, RenderdocReplayResult):
        return _format_replay(result)
    if isinstance(result, RenderdocCaptureResult):
        return _format_capture(result)
    if isinstance(result, RenderdocThumbResult):
        return _format_thumb(result)
    return f"Unknown renderdoc result: {type(result).__name__}"


def _format_replay(result: RenderdocReplayResult) -> str:
    assert result.suite == "renderdoc", f"bad suite {result.suite!r}"
    parts = [format_run_header(result), ""]
    stats = result.stats
    counts = " · ".join(f"{stats.get(k, 0)} {k}" for k in ("draws", "dispatches", "copies", "markers"))
    truncated = " · TRUNCATED (raise --max-actions)" if result.truncated else ""
    parts.append(f"**Frame:** {result.api or 'unknown API'} · frame {result.frame_number} · {counts}{truncated}")
    parts.append(f"**Capture:** `{result.rdc_path}`")

    timed = [a for a in result.actions if a.duration_us is not None]
    if timed:
        top = sorted(timed, key=lambda a: a.duration_us or 0.0, reverse=True)[:_TOP_DRAWS]
        parts.append(f"\n**Slowest actions (GPU Duration, top {len(top)}):**")
        parts.append("| eid | action | µs |")
        parts.append("|---|---|---|")
        for a in top:
            name = a.name if len(a.name) <= 80 else a.name[:77] + "..."
            parts.append(f"| {a.event_id} | {name} | {a.duration_us:.1f} |")
    elif result.actions:
        draws = [a for a in result.actions if "Drawcall" in a.flags or "Dispatch" in a.flags]
        top = sorted(draws, key=lambda a: a.num_indices * max(a.num_instances, 1), reverse=True)[:_TOP_DRAWS]
        if top:
            parts.append(f"\n**Largest draws (indices×instances, top {len(top)}):**")
            parts.append("| eid | action | indices | instances |")
            parts.append("|---|---|---|---|")
            for a in top:
                name = a.name if len(a.name) <= 80 else a.name[:77] + "..."
                parts.append(f"| {a.event_id} | {name} | {a.num_indices} | {a.num_instances} |")

    if result.resources:
        biggest = sorted(result.resources, key=lambda r: r.bytes, reverse=True)[:_TOP_RESOURCES]
        listing = ", ".join(f"{r.name or r.resource_id} ({human_bytes(r.bytes)})" for r in biggest)
        parts.append(f"\n**Largest resources:** {listing}")

    parts.append("")
    if result.tool == "analyze" and not timed:
        parts.append(f'_GPU timings: devtools_run(suite="renderdoc", tool="counters", binary="{result.rdc_path}")._')
    if timed:
        parts.append(f'_GPU-time flame graph: devtools_flamegraph(run_id="{result.run_id}")._')
    parts.append(f'_Query all rows: devtools_analyze(run_id="{result.run_id}")._')
    return "\n".join(parts)


def _format_capture(result: RenderdocCaptureResult) -> str:
    assert result.suite == "renderdoc", f"bad suite {result.suite!r}"
    parts = [format_run_header(result), ""]
    parts.append(f"**Capture mode:** {result.mode}")
    if result.frame_captured is not None:
        parts.append(f"**Frame captured:** {result.frame_captured}")
    if result.app_exit_code is not None:
        parts.append(f"**App exit code:** {result.app_exit_code}")
    if result.rdc_paths:
        parts.append(f"\n**Captures ({len(result.rdc_paths)}):**")
        for path in result.rdc_paths:
            p = Path(path)
            size = f" ({human_bytes(p.stat().st_size)})" if p.is_file() else ""
            parts.append(f"- `{path}`{size}")
        first = result.rdc_paths[0]
        parts.append(f'\n_Next: devtools_run(suite="renderdoc", tool="analyze", binary="{first}")._')
    else:
        parts.append(
            "\nNo captures produced. In launch-wait mode press F12/PrintScreen in-app; "
            "in targetcontrol mode try a larger --warmup or an explicit --frame N."
        )
    if result.capture_log:
        parts.append(f"\n```\n{result.capture_log}\n```")
    return "\n".join(parts)


def _format_thumb(result: RenderdocThumbResult) -> str:
    assert result.suite == "renderdoc", f"bad suite {result.suite!r}"
    parts = [format_run_header(result), ""]
    parts.append(f"**Thumbnail:** `{result.thumb_path}` ({result.width}x{result.height})")
    parts.append(f"**Capture:** `{result.rdc_path}`")
    return "\n".join(parts)
