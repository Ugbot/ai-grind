"""JVM tool execution: jcmd (JFR / Thread.print / class histogram), jstack, async-profiler.

All tools target a live JVM by PID (`binary` = pid digits, or `--pid N` in
extra_args). Capture needs a running JVM; tests exercise the parsers with
synthetic output.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import tempfile
import time

from devtools_mcp.flamegraph.fold import parse_folded
from devtools_mcp.jvm.models import JvmResult
from devtools_mcp.jvm.parsers import parse_class_histogram, parse_jfr_json, parse_jstack
from devtools_mcp.models import create_run_base

MAX_DURATION = 300


def _tool(name: str) -> str | None:
    return shutil.which(name) or shutil.which(name + ".exe")


def find_asprof() -> str | None:
    env = os.environ.get("DEVTOOLS_ASPROF") or os.environ.get("ASPROF")
    if env and pathlib.Path(env).exists():
        return env
    return _tool("asprof")


async def check_jvm() -> dict[str, str]:
    """Report which JVM tools are present (jcmd drives jfr/threads/heap)."""
    return {
        "jcmd": _tool("jcmd") or "",
        "jstack": _tool("jstack") or "",
        "jmap": _tool("jmap") or "",
        "jfr": _tool("jfr") or "",
        "asprof": find_asprof() or "",
    }


def _pid(binary: str, extra_args: list[str] | None) -> str | None:
    if binary and binary.isdigit():
        return binary
    if extra_args and "--pid" in extra_args:
        i = extra_args.index("--pid")
        if i + 1 < len(extra_args):
            return extra_args[i + 1]
    return None


def _duration(extra_args: list[str] | None) -> int:
    if extra_args and "--duration" in extra_args:
        i = extra_args.index("--duration")
        if i + 1 < len(extra_args) and extra_args[i + 1].isdigit():
            return min(int(extra_args[i + 1]), MAX_DURATION)
    return 10


async def _run(cmd: list[str], timeout: int) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return 124, "", "timed out"
    return proc.returncode or 0, out.decode("utf-8", "replace"), err.decode("utf-8", "replace")


async def run_jvm(
    tool: str = "cpu",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    **kwargs: object,
) -> tuple[str | None, JvmResult | None, str]:
    """Dispatch to a JVM tool. Returns (error, result, raw_path)."""
    pid = _pid(binary, extra_args)
    if not pid:
        return "No JVM pid. Pass the PID as `binary` or `--pid N` in extra_args.", None, ""
    if tool in ("cpu", "jfr"):
        return await _run_jfr(pid, extra_args, timeout)
    if tool == "threads":
        return await _run_threads(pid, timeout)
    if tool == "heap":
        return await _run_heap(pid, timeout)
    if tool in ("alloc", "asprof"):
        return await _run_asprof(pid, extra_args, timeout)
    return f"Unknown jvm tool: {tool}", None, ""


def _result(tool: str, pid: str, start: float, **fields: object) -> JvmResult:
    base = create_run_base(suite="jvm", tool=tool, binary=pid, duration_seconds=time.monotonic() - start)
    return JvmResult(**base.model_dump(), pid=pid, **fields)


async def _run_jfr(pid: str, extra_args: list[str] | None, timeout: int) -> tuple[str | None, JvmResult | None, str]:
    jcmd, jfr = _tool("jcmd"), _tool("jfr")
    if not jcmd or not jfr:
        return "jcmd/jfr not found (need a JDK on PATH).", None, ""
    dur = _duration(extra_args)
    fd, path = tempfile.mkstemp(prefix="devtools-jfr-", suffix=".jfr")
    os.close(fd)
    start = time.monotonic()
    rc, _o, err = await _run(
        [jcmd, pid, "JFR.start", "name=devtools", "settings=profile", f"duration={dur}s", f"filename={path}"], timeout
    )
    if rc != 0:
        return f"JFR.start failed: {err.strip()}", None, ""
    await asyncio.sleep(dur + 2)  # let the recording complete + auto-dump
    rc, text, err = await _run([jfr, "print", "--json", path], timeout)
    if rc != 0:
        return f"jfr print failed: {err.strip()}", None, path
    samples, counts = parse_jfr_json(text)
    result = _result(
        "cpu",
        pid,
        start,
        stack_samples=samples,
        total_samples=sum(s.weight for s in samples),
        event_counts=counts,
        jfr_path=path,
    )
    return None, result, path


async def _run_threads(pid: str, timeout: int) -> tuple[str | None, JvmResult | None, str]:
    jcmd, jstack = _tool("jcmd"), _tool("jstack")
    start = time.monotonic()
    if jcmd:
        rc, text, err = await _run([jcmd, pid, "Thread.print"], timeout)
    elif jstack:
        rc, text, err = await _run([jstack, pid], timeout)
    else:
        return "jcmd/jstack not found (need a JDK on PATH).", None, ""
    if rc != 0:
        return f"thread dump failed: {err.strip()}", None, ""
    threads, deadlock = parse_jstack(text)
    return None, _result("threads", pid, start, threads=threads, deadlock=deadlock, raw_output=text), ""


async def _run_heap(pid: str, timeout: int) -> tuple[str | None, JvmResult | None, str]:
    jcmd, jmap = _tool("jcmd"), _tool("jmap")
    start = time.monotonic()
    if jcmd:
        rc, text, err = await _run([jcmd, pid, "GC.class_histogram"], timeout)
    elif jmap:
        rc, text, err = await _run([jmap, "-histo:live", pid], timeout)
    else:
        return "jcmd/jmap not found (need a JDK on PATH).", None, ""
    if rc != 0:
        return f"class histogram failed: {err.strip()}", None, ""
    classes, total = parse_class_histogram(text)
    return None, _result("heap", pid, start, heap_classes=classes, total_bytes=total, raw_output=text), ""


async def _run_asprof(pid: str, extra_args: list[str] | None, timeout: int) -> tuple[str | None, JvmResult | None, str]:
    asprof = find_asprof()
    if not asprof:
        return (
            (
                "async-profiler not found. Download from github.com/async-profiler/async-profiler "
                "and set $DEVTOOLS_ASPROF or put `asprof` on PATH."
            ),
            None,
            "",
        )
    dur = _duration(extra_args)
    # canonical `alloc` verb -> async-profiler allocation event (override with -e in extra_args)
    event = "cpu" if (extra_args and "-e" in extra_args) else "alloc"
    fd, path = tempfile.mkstemp(prefix="devtools-asprof-", suffix=".folded")
    os.close(fd)
    start = time.monotonic()
    rc, _o, err = await _run([asprof, "-d", str(dur), "-e", event, "-o", "collapsed", "-f", path, pid], timeout)
    if rc != 0:
        return f"asprof failed: {err.strip()}", None, path
    samples = parse_folded(pathlib.Path(path).read_text(encoding="utf-8", errors="replace"))
    return None, _result("alloc", pid, start, stack_samples=samples, total_samples=sum(s.weight for s in samples)), path
