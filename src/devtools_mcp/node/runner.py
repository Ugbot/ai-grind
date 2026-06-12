"""Node.js tool execution: `node --cpu-prof` / `--heap-prof` on a script.

Launches a Node script under the V8 profiler and parses the emitted profile.
Capture needs node + a script; tests exercise the parsers with synthetic JSON.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import tempfile
import time

from devtools_mcp.models import create_run_base
from devtools_mcp.node.models import NodeResult
from devtools_mcp.node.parsers import parse_cpuprofile, parse_heapprofile


def find_node() -> str | None:
    return shutil.which("node")


async def check_node() -> dict[str, str]:
    node = find_node()
    return {"installed": "true" if node else "false", "path": node or "node"}


async def _run(cmd: list[str], cwd: str, timeout: int) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        _o, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return 124, "timed out"
    return proc.returncode or 0, err.decode("utf-8", "replace")


def _find_profile(d: str, suffix: str) -> str | None:
    for name in os.listdir(d):
        if name.endswith(suffix):
            return os.path.join(d, name)
    return None


async def run_node(
    tool: str = "cpu",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    **kwargs: object,
) -> tuple[str | None, NodeResult | None, str]:
    """Run a Node script under --cpu-prof / --heap-prof and parse the profile."""
    node = find_node()
    if not node:
        return "node not found. Install Node.js (nodejs.org).", None, ""
    if tool in ("alloc", "heap"):  # canonical alloc verb -> V8 sampling heap profile
        tool = "alloc"
    if tool not in ("cpu", "alloc"):
        return f"Unknown node tool: {tool} (cpu|alloc)", None, ""
    if not binary or not os.path.exists(binary):
        return f"node {tool} needs a script path as `binary` (got {binary!r}).", None, ""

    out_dir = tempfile.mkdtemp(prefix="devtools-node-")
    flag = "--cpu-prof" if tool == "cpu" else "--heap-prof"
    suffix = ".cpuprofile" if tool == "cpu" else ".heapprofile"
    # Absolute script path + run from its own dir so relative require()s resolve;
    # the profile still lands in out_dir via the absolute --*-prof-dir.
    script = os.path.abspath(binary)
    cwd = os.path.dirname(script) or "."
    cmd = [node, flag, f"{flag}-dir", out_dir, script, *(args or [])]
    start = time.monotonic()
    rc, err = await _run(cmd, cwd=cwd, timeout=timeout)
    prof = _find_profile(out_dir, suffix)
    if prof is None:
        return f"node produced no {suffix} (exit {rc}): {err.strip()[:300]}", None, ""

    text = pathlib.Path(prof).read_text(encoding="utf-8", errors="replace")
    samples = parse_cpuprofile(text) if tool == "cpu" else parse_heapprofile(text)
    base = create_run_base(
        suite="node", tool=tool, binary=binary, args=args or [], duration_seconds=time.monotonic() - start, exit_code=rc
    )
    result = NodeResult(
        **base.model_dump(),
        stack_samples=samples,
        total_weight=sum(s.weight for s in samples),
        weight_unit="samples" if tool == "cpu" else "bytes",
        profile_path=prof,
    )
    return None, result, prof
