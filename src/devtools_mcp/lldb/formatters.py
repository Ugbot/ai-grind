"""LLDB-specific formatters for debug state snapshots."""

from __future__ import annotations

from devtools_mcp.lldb.models import LldbSnapshot

_RAW_MAX_LINES = 60  # bounded preview of memory/expression/disassemble output


def format_snapshot_summary(snapshot: LldbSnapshot) -> str:
    """Format a concise summary of an LLDB snapshot."""
    parts: list[str] = [
        f"**Snapshot:** `{snapshot.run_id}` ({snapshot.snapshot_type})",
        f"**Session:** `{snapshot.session_id}`",
        f"**Binary:** {snapshot.binary}",
    ]

    if snapshot.threads:
        parts.append(f"**Threads:** {len(snapshot.threads)}")
        for thread in snapshot.threads[:5]:
            top = thread.frames[0].location if thread.frames else "no frames"
            reason = f" [{thread.stop_reason}]" if thread.stop_reason else ""
            parts.append(f"  - Thread #{thread.thread_id}{reason}: {top}")
        if len(snapshot.threads) > 5:
            parts.append(f"  ... and {len(snapshot.threads) - 5} more threads")

    if snapshot.variables:
        parts.append(f"**Variables:** {len(snapshot.variables)}")
        for var in snapshot.variables[:10]:
            parts.append(f"  - ({var.type}) {var.name} = {var.value}")
        if len(snapshot.variables) > 10:
            parts.append(f"  ... and {len(snapshot.variables) - 10} more")

    if snapshot.breakpoints:
        parts.append(f"**Breakpoints:** {len(snapshot.breakpoints)}")
        for bp in snapshot.breakpoints[:10]:
            loc = f"{bp.file}:{bp.line}" if bp.file else bp.name or bp.address or "?"
            parts.append(f"  - #{bp.id} {loc} (hits: {bp.hit_count})")

    if snapshot.registers:
        parts.append(f"**Registers:** {len(snapshot.registers)}")

    # memory / expression / disassemble carry their result only in raw_output;
    # render it (bounded) so those inspections aren't silently empty.
    structured = snapshot.threads or snapshot.variables or snapshot.breakpoints or snapshot.registers
    raw = (snapshot.raw_output or "").strip()
    if raw and not structured:
        lines = raw.splitlines()
        shown = "\n".join(lines[:_RAW_MAX_LINES])
        parts.append(f"```\n{shown}\n```")
        if len(lines) > _RAW_MAX_LINES:
            parts.append(f"_… {len(lines) - _RAW_MAX_LINES} more lines. See `devtools_raw`._")

    parts.append("")
    parts.append(f'Query with `devtools_analyze(run_id="{snapshot.run_id}")` or `devtools_search()`')
    return "\n".join(parts)
