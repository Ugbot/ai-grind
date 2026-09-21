"""CDB summary formatters. Bounded."""

from __future__ import annotations

from devtools_mcp.cdb.models import CdbSnapshot
from devtools_mcp.formatters.utils import format_run_header

_TOP_FRAMES = 15


def format_cdb_summary(result: CdbSnapshot) -> str:
    parts = [format_run_header(result), ""]

    if result.analysis:
        parts.append("**Crash analysis (!analyze -v):**")
        for key in (
            "EXCEPTION_CODE_STR",
            "EXCEPTION_CODE",
            "SYMBOL_NAME",
            "MODULE_NAME",
            "IMAGE_NAME",
            "FAILURE_BUCKET_ID",
        ):
            if key in result.analysis:
                parts.append(f"  - {key}: {result.analysis[key]}")
        parts.append("")

    if result.threads:
        nframes = sum(len(t.frames) for t in result.threads)
        parts.append(f"**Threads:** {len(result.threads)} · {nframes} frames")
        # Show the faulting / first thread's top frames only.
        first = result.threads[0]
        parts.append(f"\n**Thread {first.index} top frames:**")
        for f in first.frames[:_TOP_FRAMES]:
            loc = f" [{f.file}:{f.line}]" if f.file and f.line else ""
            parts.append(f"  {f.index:>2} {f.symbol}{f.offset}{loc}")

    if result.registers:
        key_regs = {k: result.registers[k] for k in ("rip", "rsp", "rbp", "rax") if k in result.registers}
        if key_regs:
            parts.append("\n**Registers:** " + ", ".join(f"{k}={v}" for k, v in key_regs.items()))

    parts.append(f'\n_Query all frames: devtools_analyze(run_id="{result.run_id}")._')
    if any(t.frames for t in result.threads):
        parts.append(f'_Flame graph: devtools_flamegraph(run_id="{result.run_id}")._')
    return "\n".join(parts)
