"""Parse perf output into structured models."""

from __future__ import annotations

import re

from devtools_mcp.models import RunBase
from devtools_mcp.perf.models import (
    PerfAnnotationLine,
    PerfAnnotationResult,
    PerfCounter,
    PerfRecordResult,
    PerfSample,
    PerfStatResult,
)


def parse_perf_stat(text: str, run_base: RunBase) -> PerfStatResult:
    """Parse perf stat CSV output (-x , format).

    Format: value,unit,event,variance,enabled_pct
    Example: 1234567,,cycles,0.12%,100.00%
    """
    counters: list[PerfCounter] = []
    duration = 0.0
    ipc: float | None = None

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split(",")
        if len(parts) < 3:
            continue

        try:
            value_str = parts[0].strip()
            if not value_str or value_str == "<not supported>" or value_str == "<not counted>":
                continue
            value = float(value_str)
        except ValueError:
            continue

        unit = parts[1].strip() if len(parts) > 1 else ""
        event = parts[2].strip() if len(parts) > 2 else ""

        variance = _parse_pct(parts[3]) if len(parts) > 3 else None
        enabled = _parse_pct(parts[4]) if len(parts) > 4 else None

        counters.append(
            PerfCounter(
                event=event,
                value=value,
                unit=unit,
                variance_pct=variance,
                enabled_pct=enabled,
            )
        )

        if event == "duration_time":
            duration = value / 1e9  # nanoseconds to seconds

    # Compute IPC if we have both cycles and instructions
    cycles = next((c.value for c in counters if c.event == "cycles"), None)
    instructions = next((c.value for c in counters if c.event == "instructions"), None)
    if cycles and instructions and cycles > 0:
        ipc = instructions / cycles

    return PerfStatResult(
        **run_base.model_dump(),
        counters=counters,
        duration_time=duration,
        ipc=ipc,
        raw_output=text,
    )


def parse_perf_report(text: str, run_base: RunBase) -> PerfRecordResult:
    """Parse perf report --stdio output.

    Lines like:
    # Overhead  Command   Shared Object  Symbol
        12.34%  myapp     myapp          [.] hot_function
         5.67%  myapp     libc.so.6      [.] malloc
    """
    samples: list[PerfSample] = []
    total_samples = 0

    for line in text.splitlines():
        # Match sample lines: "  12.34%  command  shared_object  [.] symbol"
        match = re.match(
            r"\s+(\d+\.\d+)%\s+(\S+)\s+(\S+)\s+\[.\]\s+(\S+)",
            line,
        )
        if match:
            samples.append(
                PerfSample(
                    overhead_pct=float(match.group(1)),
                    command=match.group(2),
                    shared_object=match.group(3),
                    symbol=match.group(4),
                )
            )
            continue

        # Total samples line
        total_match = re.search(r"(\d+)\s+samples", line)
        if total_match:
            total_samples = int(total_match.group(1))

    return PerfRecordResult(
        **run_base.model_dump(),
        samples=samples,
        total_samples=total_samples,
        raw_output=text,
    )


def parse_perf_annotate(text: str, run_base: RunBase, symbol: str = "") -> PerfAnnotationResult:
    """Parse perf annotate --stdio output.

    Lines like:
     Percent |      Source code & Disassembly
       5.23  :  400520:   mov    %rax,%rdi
              :  400523:   call   401000 <malloc@plt>
    """
    lines: list[PerfAnnotationLine] = []

    for raw_line in text.splitlines():
        # Annotation line with percentage
        match = re.match(r"\s+(\d+\.\d+)\s+:\s+([0-9a-fA-F]+):\s+(.*)", raw_line)
        if match:
            lines.append(
                PerfAnnotationLine(
                    percent=float(match.group(1)),
                    address=match.group(2),
                    instruction=match.group(3).strip(),
                )
            )
            continue

        # Line without percentage (0%)
        match = re.match(r"\s+:\s+([0-9a-fA-F]+):\s+(.*)", raw_line)
        if match:
            lines.append(
                PerfAnnotationLine(
                    percent=0.0,
                    address=match.group(1),
                    instruction=match.group(2).strip(),
                )
            )

    return PerfAnnotationResult(
        **run_base.model_dump(),
        symbol=symbol,
        lines=lines,
        raw_output=text,
    )


def _parse_pct(s: str) -> float | None:
    """Parse a percentage string like '0.12%' into a float, or None."""
    s = s.strip()
    if not s:
        return None
    try:
        return float(s.rstrip("%"))
    except ValueError:
        return None
