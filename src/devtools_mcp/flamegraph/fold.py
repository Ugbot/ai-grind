"""Brendan-Gregg folded-stack text <-> StackSample.

Folded format (one line per unique stack):
    root;child;leaf <count>

This is exactly async-profiler's collapsed output, so `parse_folded` ingests it
directly. Frames are root-first; the trailing integer is the weight.
"""

from __future__ import annotations

from devtools_mcp.models import StackSample

MAX_LINES = 5_000_000  # bound: refuse pathologically huge folded files
MAX_FRAMES = 1024  # bound: a single stack deeper than this is truncated


def parse_folded(text: str) -> list[StackSample]:
    """Parse folded-stack text into StackSamples. Malformed lines are skipped."""
    assert isinstance(text, str), "folded input must be str"
    samples: list[StackSample] = []
    lines = text.splitlines()
    assert len(lines) <= MAX_LINES, f"folded input too large: {len(lines)} lines"
    for line in lines:
        line = line.rstrip()
        if not line or line.startswith("#"):
            continue
        sep = line.rfind(" ")
        if sep <= 0:
            continue
        count_str = line[sep + 1 :]
        if not count_str.isdigit():
            continue
        frames = line[:sep].split(";")
        if len(frames) > MAX_FRAMES:
            frames = frames[:MAX_FRAMES]
        frames = [f for f in (fr.strip() for fr in frames) if f]
        if not frames:
            continue
        samples.append(StackSample(frames=frames, weight=int(count_str)))
    assert all(s.weight >= 0 for s in samples), "negative weight parsed"
    return samples


def emit_folded(samples: list[StackSample]) -> str:
    """Emit folded-stack text from StackSamples (for interop / re-export)."""
    assert isinstance(samples, list), "samples must be a list"
    out: list[str] = []
    for s in samples:
        if not s.frames:
            continue
        out.append(";".join(s.frames) + f" {s.weight}")
    return "\n".join(out)
