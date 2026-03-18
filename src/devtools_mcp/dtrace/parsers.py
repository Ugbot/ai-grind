"""Parse DTrace text output into structured models.

DTrace output formats:
- Aggregations: "  key  value" or multi-key tables
- Quantize: power-of-2 histograms with ASCII bars
- Stack traces: indented function names followed by count
- Printf: user-defined formatted output
"""

from __future__ import annotations

import re

from devtools_mcp.dtrace.models import (
    DTraceAggregation,
    DTraceProbeHit,
    DTraceQuantization,
    DTraceQuantizeBucket,
    DTraceResult,
    DTraceStackTrace,
)
from devtools_mcp.models import RunBase


def parse_dtrace_output(
    text: str,
    run_base: RunBase,
    script: str = "",
    one_liner: str = "",
) -> DTraceResult:
    """Parse DTrace output, auto-detecting the output format."""
    aggregations: list[DTraceAggregation] = []
    stacks: list[DTraceStackTrace] = []
    quantizations: list[DTraceQuantization] = []
    probe_hits: list[DTraceProbeHit] = []

    lines = text.splitlines()
    i = 0

    while i < len(lines):
        line = lines[i]

        # Skip dtrace info lines
        if line.startswith("dtrace:") or not line.strip():
            i += 1
            continue

        # Quantize/lquantize distribution
        if _is_quantize_header(line, lines, i):
            quant, i = _parse_quantize(lines, i)
            if quant:
                quantizations.append(quant)
            continue

        # Stack trace (indented function names followed by blank line + count)
        if _is_stack_start(line, lines, i):
            stack, i = _parse_stack(lines, i)
            if stack:
                stacks.append(stack)
            continue

        # Aggregation line: "  key  value" or "  key1  key2  value"
        agg = _parse_aggregation_line(line)
        if agg:
            aggregations.append(agg)
            i += 1
            continue

        # Probe hit (from printf-style output)
        probe_hit = _parse_probe_hit(line)
        if probe_hit:
            probe_hits.append(probe_hit)

        i += 1

    return DTraceResult(
        **run_base.model_dump(),
        script=script,
        one_liner=one_liner,
        aggregations=aggregations,
        stacks=stacks,
        quantizations=quantizations,
        probe_hits=probe_hits,
        raw_output=text,
    )


def _is_quantize_header(line: str, lines: list[str], idx: int) -> bool:
    """Check if this line starts a quantize distribution."""
    stripped = line.strip()
    # Quantize headers look like:
    # "  value  ------------- Distribution ------------- count"
    if "Distribution" in stripped:
        return True
    # Or just a key label followed by distribution lines
    return idx + 1 < len(lines) and "Distribution" in lines[idx + 1]


def _parse_quantize(lines: list[str], start: int) -> tuple[DTraceQuantization | None, int]:
    """Parse a quantize/lquantize distribution block."""
    i = start
    key = ""
    buckets: list[DTraceQuantizeBucket] = []

    # Skip header lines
    while i < len(lines):
        line = lines[i].strip()
        if "Distribution" in line:
            i += 1
            break
        if line and not line.startswith("dtrace:"):
            key = line
        i += 1

    # Parse bucket lines: "  low  |@@@@  count"
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            break

        # Pattern: "  16 |@@@@@@@                                      42"
        match = re.match(r"\s*(-?\d+)\s*\|[@ ]*\s+(\d+)", line)
        if match:
            low = int(match.group(1))
            count = int(match.group(2))
            # High is the next power of 2 (or next bucket's low)
            high = low * 2 if low > 0 else (1 if low == 0 else low // 2)
            buckets.append(DTraceQuantizeBucket(low=low, high=high, count=count))
            i += 1
        else:
            break

    if not buckets:
        return None, i

    total = sum(b.count for b in buckets)
    return DTraceQuantization(key=key, buckets=buckets, total=total), i


def _is_stack_start(line: str, lines: list[str], idx: int) -> bool:
    """Check if this looks like the start of a stack trace."""
    stripped = line.strip()
    # Stack frames typically look like module`function+offset or just function names
    if "`" in stripped and not stripped[0].isdigit():
        # Check if next few lines also look like frames
        frame_count = 0
        for j in range(idx, min(idx + 5, len(lines))):
            if "`" in lines[j] or lines[j].startswith("  "):
                frame_count += 1
        return frame_count >= 2
    return False


def _parse_stack(lines: list[str], start: int) -> tuple[DTraceStackTrace | None, int]:
    """Parse a stack trace block (indented frames followed by count)."""
    frames: list[str] = []
    i = start
    count = 1

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped:
            # Blank line — next non-blank might be the count
            i += 1
            if i < len(lines):
                count_match = re.match(r"\s*(\d+)\s*$", lines[i])
                if count_match:
                    count = int(count_match.group(1))
                    i += 1
            break

        # Frame line
        if "`" in stripped or stripped.startswith("0x"):
            frames.append(stripped)
        elif re.match(r"\s*\d+\s*$", stripped):
            count = int(stripped.strip())
            i += 1
            break
        else:
            frames.append(stripped)

        i += 1

    if not frames:
        return None, i

    return DTraceStackTrace(frames=frames, count=count), i


def _parse_aggregation_line(line: str) -> DTraceAggregation | None:
    """Try to parse a line as an aggregation result.

    Formats:
    "  read                    42"
    "  bash  read              42"
    """
    stripped = line.strip()
    if not stripped or stripped.startswith("dtrace:"):
        return None

    # Split on whitespace, last token should be a number
    parts = stripped.split()
    if len(parts) < 2:
        return None

    try:
        value = int(parts[-1])
    except ValueError:
        return None

    keys = parts[:-1]
    # Heuristic: keys shouldn't look like numbers
    if all(k.isdigit() or k.startswith("-") for k in keys):
        return None

    return DTraceAggregation(keys=keys, value=value, agg_type="count")


def _parse_probe_hit(line: str) -> DTraceProbeHit | None:
    """Try to parse a line as a probe hit from printf output.

    Common formats:
    "CPU     ID FUNCTION:NAME"
    "  0  12345 syscall::read:entry  pid=1234 execname=bash"
    """
    match = re.match(
        r"\s*(\d+)\s+(\d+)\s+(\S+::?\S+)\s*(.*)",
        line,
    )
    if match:
        return DTraceProbeHit(
            cpu=int(match.group(1)),
            probe=match.group(3),
            args=match.group(4).strip(),
        )
    return None
