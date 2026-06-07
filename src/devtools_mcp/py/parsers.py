"""Parsers for Python tool output: pstats (cProfile) and py-spy dump."""

from __future__ import annotations

import pstats
import re

from devtools_mcp.py.models import PyFuncStat, PyThread

_THREAD = re.compile(r'^Thread\s+(?P<tid>\S+)\s+\((?P<state>[^)]*)\):\s*"?(?P<name>[^"]*)"?')
_FRAME = re.compile(r"^\s+(?P<func>.+?)\s+\((?P<loc>.+:\d+)\)\s*$")


def parse_pstats(path: str) -> list[PyFuncStat]:
    """Read a cProfile .prof file into PyFuncStat rows via the stdlib pstats module."""
    assert path, "pstats path required"
    stats = pstats.Stats(path)
    rows: list[PyFuncStat] = []
    for (fname, line, func), (cc, nc, tt, ct, _callers) in stats.stats.items():  # type: ignore[attr-defined]
        calls = nc or 0
        rows.append(
            PyFuncStat(
                function=f"{fname}:{line}({func})",
                ncalls=calls,
                tottime=tt,
                cumtime=ct,
                percall_tot=tt / calls if calls else 0.0,
                percall_cum=ct / (cc or calls or 1),
            )
        )
    rows.sort(key=lambda r: r.cumtime, reverse=True)
    return rows


def parse_pyspy_dump(text: str) -> list[PyThread]:
    """Parse `py-spy dump` output into threads with frames."""
    assert isinstance(text, str), "dump text must be str"
    threads: list[PyThread] = []
    current: PyThread | None = None
    for line in text.splitlines():
        m = _THREAD.match(line)
        if m:
            current = PyThread(tid=m.group("tid"), state=m.group("state").strip(),
                               name=m.group("name").strip())
            threads.append(current)
            continue
        if current is None:
            continue
        fm = _FRAME.match(line)
        if fm:
            current.frames.append(f"{fm.group('func').strip()} ({fm.group('loc').strip()})")
    return threads
