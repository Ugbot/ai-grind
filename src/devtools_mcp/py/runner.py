"""Python tool execution: py-spy (sampling/dump) and cProfile (stdlib).

py-spy attaches to a live process by PID **or** launches a script; cProfile
launches a script and writes a .prof. Capture needs the tool/script; tests
exercise the parsers with synthetic data.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import sys
import tempfile
import time

from devtools_mcp.flamegraph.fold import parse_folded
from devtools_mcp.models import create_run_base
from devtools_mcp.py.models import PyResult
from devtools_mcp.py.parsers import parse_pstats, parse_pyspy_dump

MAX_DURATION = 300


def find_pyspy() -> str | None:
    return shutil.which("py-spy")


async def check_py() -> dict[str, str]:
    return {"py-spy": find_pyspy() or "", "python": sys.executable}


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
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return 124, "", "timed out"
    return proc.returncode or 0, out.decode("utf-8", "replace"), err.decode("utf-8", "replace")


def _result(tool: str, target: str, start: float, **fields: object) -> PyResult:
    base = create_run_base(suite="py", tool=tool, binary=target, duration_seconds=time.monotonic() - start)
    return PyResult(**base.model_dump(), **fields)


async def run_py(
    tool: str = "cpu",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    **kwargs: object,
) -> tuple[str | None, PyResult | None, str]:
    """Dispatch to a Python tool."""
    if tool == "cprofile":
        return await _run_cprofile(binary, args or [], timeout)
    if tool in ("cpu", "pyspy"):
        return await _run_pyspy(binary, args or [], extra_args, timeout)
    if tool in ("threads", "dump"):
        return await _run_dump(binary, extra_args, timeout)
    return f"Unknown py tool: {tool} (cpu|threads|cprofile)", None, ""


async def _run_cprofile(binary: str, args: list[str], timeout: int) -> tuple[str | None, PyResult | None, str]:
    if not binary or not os.path.exists(binary):
        return f"cprofile needs a script path as `binary` (got {binary!r}).", None, ""
    fd, prof = tempfile.mkstemp(prefix="devtools-cprof-", suffix=".prof")
    os.close(fd)
    start = time.monotonic()
    rc, _o, err = await _run([sys.executable, "-m", "cProfile", "-o", prof, binary, *args], timeout)
    if rc != 0 and not os.path.getsize(prof):
        return f"cProfile failed: {err.strip()}", None, prof
    return None, _result("cprofile", binary, start, func_stats=parse_pstats(prof), profile_path=prof), prof


async def _run_pyspy(binary: str, args: list[str], extra_args: list[str] | None,
                     timeout: int) -> tuple[str | None, PyResult | None, str]:
    pyspy = find_pyspy()
    if not pyspy:
        return "py-spy not found. Install with `pip install py-spy`.", None, ""
    pid = _pid(binary, extra_args)
    fd, raw = tempfile.mkstemp(prefix="devtools-pyspy-", suffix=".folded")
    os.close(fd)
    start = time.monotonic()
    cmd = [pyspy, "record", "-f", "raw", "-o", raw, "-d", str(_duration(extra_args))]
    if pid:
        cmd += ["--pid", pid]
    elif binary:
        cmd += ["--", sys.executable, binary, *args]
    else:
        return "py-spy needs a PID (`binary`=pid or --pid N) or a script path.", None, ""
    rc, _o, err = await _run(cmd, timeout)
    if rc != 0 and not os.path.getsize(raw):
        return f"py-spy failed: {err.strip()}", None, raw
    samples = parse_folded(pathlib.Path(raw).read_text(encoding="utf-8", errors="replace"))
    return None, _result("cpu", pid or binary, start, pid=pid or "", stack_samples=samples,
                         total_samples=sum(s.weight for s in samples)), raw


async def _run_dump(binary: str, extra_args: list[str] | None, timeout: int) -> tuple[str | None, PyResult | None, str]:
    pyspy = find_pyspy()
    if not pyspy:
        return "py-spy not found. Install with `pip install py-spy`.", None, ""
    pid = _pid(binary, extra_args)
    if not pid:
        return "py-spy dump needs a PID (`binary`=pid or --pid N).", None, ""
    start = time.monotonic()
    rc, out, err = await _run([pyspy, "dump", "--pid", pid], timeout)
    if rc != 0:
        return f"py-spy dump failed: {err.strip()}", None, ""
    return None, _result("threads", pid, start, pid=pid, threads=parse_pyspy_dump(out), raw_output=out), ""
