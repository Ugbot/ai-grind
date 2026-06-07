"""Parsers for JVM tool output: JFR JSON, jstack dumps, class histograms."""

from __future__ import annotations

import json
import re

from devtools_mcp.jvm.models import JvmHeapClass, JvmThread
from devtools_mcp.models import StackSample

MAX_EVENTS = 2_000_000  # bound on JFR events
MAX_FRAMES = 1024  # bound per stack

_THREAD_HDR = re.compile(r'^"(?P<name>.*?)"\s*(?P<rest>.*)$')
_STATE = re.compile(r"java\.lang\.Thread\.State:\s*(\S+)")
_FRAME = re.compile(r"^\s*at\s+(.*)$")
_HISTO = re.compile(r"^\s*(\d+):\s+(\d+)\s+(\d+)\s+(\S.*?)\s*$")


def _frame_name(frame: dict) -> str:
    """Build 'Class.method' from a JFR frame's method object."""
    method = frame.get("method") or {}
    cls = ((method.get("type") or {}).get("name")) or ""
    name = method.get("name") or "?"
    return f"{cls}.{name}" if cls else name


def parse_jfr_json(text: str) -> tuple[list[StackSample], dict[str, int]]:
    """Parse `jfr print --json` into aggregated StackSamples + per-event counts."""
    assert isinstance(text, str), "jfr json must be str"
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return [], {}
    events = data.get("events")
    if events is None:
        events = (data.get("recording") or {}).get("events", [])
    assert len(events) <= MAX_EVENTS, f"too many JFR events: {len(events)}"

    counts: dict[str, int] = {}
    agg: dict[tuple[str, ...], int] = {}
    for ev in events:
        etype = ev.get("type", "?")
        counts[etype] = counts.get(etype, 0) + 1
        values = ev.get("values") or {}
        stack = values.get("stackTrace") or {}
        frames = stack.get("frames") or []
        if not frames or "ExecutionSample" not in etype:
            continue
        names = [_frame_name(f) for f in frames[:MAX_FRAMES]]
        names = list(reversed(names))  # JFR is leaf-first; flame wants root-first
        key = tuple(names)
        agg[key] = agg.get(key, 0) + 1

    samples = [StackSample(frames=list(k), weight=w) for k, w in agg.items()]
    return samples, counts


def parse_jstack(text: str) -> tuple[list[JvmThread], bool]:
    """Parse a jstack / Thread.print dump into threads; detect deadlock."""
    assert isinstance(text, str), "jstack text must be str"
    deadlock = "Found one Java-level deadlock" in text or "Found a total of" in text
    threads: list[JvmThread] = []
    current: JvmThread | None = None
    for line in text.splitlines():
        hdr = _THREAD_HDR.match(line) if line.startswith('"') else None
        if hdr:
            current = JvmThread(name=hdr.group("name"))
            rest = hdr.group("rest")
            current.daemon = "daemon" in rest
            tid = re.search(r"tid=(\S+)", rest)
            nid = re.search(r"nid=(\S+)", rest)
            prio = re.search(r"\bprio=(\d+)", rest)
            current.tid = tid.group(1) if tid else ""
            current.nid = nid.group(1) if nid else ""
            current.priority = int(prio.group(1)) if prio else None
            threads.append(current)
            continue
        if current is None:
            continue
        st = _STATE.search(line)
        if st:
            current.state = st.group(1)
            continue
        fr = _FRAME.match(line)
        if fr:
            current.frames.append(fr.group(1).strip())
    return threads, deadlock


def parse_class_histogram(text: str) -> tuple[list[JvmHeapClass], int]:
    """Parse jmap -histo / GC.class_histogram into class rows + total bytes."""
    assert isinstance(text, str), "histogram text must be str"
    classes: list[JvmHeapClass] = []
    total_bytes = 0
    for line in text.splitlines():
        m = _HISTO.match(line)
        if not m:
            if line.strip().lower().startswith("total"):
                nums = re.findall(r"\d+", line)
                if len(nums) >= 2:
                    total_bytes = int(nums[1])
            continue
        classes.append(
            JvmHeapClass(
                rank=int(m.group(1)), instances=int(m.group(2)),
                bytes=int(m.group(3)), class_name=m.group(4),
            )
        )
    return classes, total_bytes
