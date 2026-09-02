"""Parsers for PerfView's flat CPU-stack CSV (SaveCPUStacksAsCsv)."""

from __future__ import annotations

import csv
import io
import re

from devtools_mcp.etw.models import EtwSample

_TEMPLATE = re.compile(r"<[^<>]*>")
MAX_ROWS = 500_000  # bound: refuse a pathologically huge CSV


def split_module(name: str) -> tuple[str, str]:
    """Split PerfView's `module!function` into (module, function)."""
    if "!" in name:
        mod, fn = name.split("!", 1)
        return mod, fn
    return "", name


def shorten(name: str, max_len: int = 110) -> str:
    """Collapse verbose C++ template/lambda noise so a name fits one line."""
    short = name
    for _ in range(5):  # bounded
        new = _TEMPLATE.sub("<>", short)
        if new == short:
            break
        short = new
    short = short.replace("`anonymous namespace'::", "(anon)::")
    short = re.sub(r"::`\d+'::<lambda_\d+>", "::<lam>", short)
    if len(short) > max_len:
        short = short[: max_len - 3] + "..."
    return short


def is_synthetic(name: str) -> bool:
    """PerfView pseudo-nodes (process/thread/module aggregates), not real frames."""
    if name.endswith("!?") or "!?!?" in name:
        return True
    if name.startswith("Thread (") or name.startswith("Process"):
        return True
    return name in ("ROOT", "BROKEN")


def parse_perfview_csv(text: str) -> list[EtwSample]:
    """Parse SaveCPUStacksAsCsv output (Name, Exc, Exc%, Inc, Inc%, First, Last)."""
    assert isinstance(text, str), "csv text must be str"
    samples: list[EtwSample] = []
    reader = csv.DictReader(io.StringIO(text))
    for i, row in enumerate(reader):
        if i >= MAX_ROWS:
            break
        name = row.get("Name", "")
        if not name:
            continue
        try:
            exc, exc_pct = float(row["Exc"]), float(row["Exc%"])
            inc, inc_pct = float(row["Inc"]), float(row["Inc%"])
        except (KeyError, ValueError):
            continue
        mod, fn = split_module(name)
        samples.append(
            EtwSample(
                name=name,
                module=mod,
                function=fn,
                exc=exc,
                exc_pct=exc_pct,
                inc=inc,
                inc_pct=inc_pct,
                first_ms=float(row.get("First") or 0),
                last_ms=float(row.get("Last") or 0),
            )
        )
    assert all(s.exc_pct >= 0 for s in samples), "negative Exc% parsed"
    return samples
