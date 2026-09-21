"""Drive the Intel VTune Profiler CLI (`vtune -collect` / `-report`).

One run = collect into a result dir, then decode three bounded reports:
summary (text), a function-grouped CSV (the queryable frame), and a top-down
CSV (folded into flame-graph stacks). The result dir is kept. It remains
openable in the VTune GUI (`vtune-gui <dir>`), the wrapper only ever surfaces
bounded slices of it.

Targets: a launch binary (+args), or an attach via `--pid N` in extra_args
(optionally `--duration N` seconds). `--result-dir D` reuses/keeps a specific
directory; `--report-only` re-decodes an existing result dir without
collecting (the ETW `--decode-only` analog).
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import tempfile
import time

from devtools_mcp.models import create_run_base
from devtools_mcp.vtune.models import VtuneResult
from devtools_mcp.vtune.parsers import parse_function_csv, parse_topdown_csv

# Unified verb -> vtune analysis type.
ANALYSES: dict[str, str] = {
    "cpu": "hotspots",
    "threads": "threading",
    "alloc": "memory-consumption",
    "memory": "memory-access",
    "uarch": "uarch-exploration",
    "snapshot": "performance-snapshot",
}

_SUMMARY_MAX_CHARS = 20_000
_WINDOWS_DEFAULTS = (
    r"C:\Program Files (x86)\Intel\oneAPI\vtune\latest\bin64\vtune.exe",
    r"C:\Program Files\Intel\oneAPI\vtune\latest\bin64\vtune.exe",
)
_POSIX_DEFAULTS = (
    "/opt/intel/oneapi/vtune/latest/bin64/vtune",
    os.path.expanduser("~/intel/oneapi/vtune/latest/bin64/vtune"),
)


def find_vtune() -> str | None:
    """Locate vtune: $DEVTOOLS_VTUNE -> PATH -> default oneAPI install dirs."""
    env = os.environ.get("DEVTOOLS_VTUNE")
    if env and pathlib.Path(env).exists():
        return env
    on_path = shutil.which("vtune") or shutil.which("vtune.exe")
    if on_path:
        return on_path
    for candidate in _WINDOWS_DEFAULTS + _POSIX_DEFAULTS:
        if pathlib.Path(candidate).exists():
            return candidate
    return None


async def _vtune(vtune: str, args: list[str], timeout: int, cwd: str | None = None) -> tuple[int, str, str]:
    """One vtune invocation; returns (exit_code, stdout, stderr)."""
    assert args, "vtune called with no arguments"
    proc = await asyncio.create_subprocess_exec(
        vtune,
        *args,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return 124, "", f"vtune timed out after {timeout}s"
    return proc.returncode or 0, out.decode("utf-8", errors="replace"), err.decode("utf-8", errors="replace")


async def check_vtune() -> dict[str, str]:
    """Detect the vtune CLI and its version."""
    path = find_vtune()
    if not path:
        return {
            "installed": "false",
            "version": "",
            "path": "vtune",
            "error": (
                "vtune not found. Install Intel VTune Profiler (oneAPI), put `vtune` on PATH "
                "(or run setvars), or set $DEVTOOLS_VTUNE to the executable."
            ),
        }
    code, out, _ = await _vtune(path, ["-version"], timeout=30)
    version = out.splitlines()[0].strip() if out else ""
    if code != 0:
        return {"installed": "false", "version": version, "path": path, "error": f"vtune -version exited {code}"}
    return {"installed": "true", "version": version, "path": path}


def _opt(extra_args: list[str] | None, flag: str) -> str | None:
    """Value following `flag` in extra_args, if present."""
    if not extra_args or flag not in extra_args:
        return None
    i = extra_args.index(flag)
    return extra_args[i + 1] if i + 1 < len(extra_args) else None


def _passthrough(extra_args: list[str] | None) -> list[str]:
    """extra_args minus the wrapper's own flags, handed to `vtune -collect` verbatim."""
    own = {"--pid", "--duration", "--result-dir", "--report-only"}
    out: list[str] = []
    skip = False
    for arg in extra_args or []:  # bounded by caller-provided list
        if skip:
            skip = False
            continue
        if arg in own:
            skip = arg != "--report-only"  # --report-only takes no value
            continue
        out.append(arg)
    assert len(out) <= len(extra_args or []), "passthrough grew the argument list"
    return out


async def run_vtune(
    tool: str = "cpu",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 600,
    **kwargs: object,
) -> tuple[str | None, VtuneResult | None, str]:
    """Collect one VTune analysis and decode bounded reports from it."""
    if tool not in ANALYSES:
        return f"Unknown vtune tool: {tool} (one of: {', '.join(sorted(ANALYSES))})", None, ""
    vtune = find_vtune()
    if not vtune:
        return (await check_vtune())["error"], None, ""

    analysis = ANALYSES[tool]
    pid = _opt(extra_args, "--pid")
    duration = _opt(extra_args, "--duration")
    report_only = bool(extra_args and "--report-only" in extra_args)
    result_dir = _opt(extra_args, "--result-dir") or os.path.join(
        tempfile.mkdtemp(prefix="devtools-vtune-"), f"r-{tool}"
    )
    start = time.monotonic()

    if not report_only:
        collect = ["-collect", analysis, "-result-dir", result_dir, *_passthrough(extra_args)]
        if pid:
            collect += ["-target-pid", pid]
            if duration:
                collect += ["-duration", duration]
        else:
            if not binary or not pathlib.Path(binary).exists():
                return f"binary not found: {binary} (or pass --pid N in extra_args)", None, ""
            collect += ["--", binary, *map(str, args or [])]
        code, _, err = await _vtune(vtune, collect, timeout)
        if code != 0:
            tail = err.strip().splitlines()[-5:]
            return f"vtune -collect {analysis} failed (exit {code}): " + " | ".join(tail), None, ""
    elif not pathlib.Path(result_dir).exists():
        return f"--report-only: result dir not found: {result_dir}", None, ""

    base_report = ["-report", "summary", "-result-dir", result_dir]
    _, summary_out, _ = await _vtune(vtune, base_report, timeout)

    csv_args = [
        "-report",
        "hotspots",
        "-group-by",
        "function",
        "-result-dir",
        result_dir,
        "-format",
        "csv",
        "-csv-delimiter",
        "comma",
    ]
    csv_code, csv_out, _ = await _vtune(vtune, csv_args, timeout)
    functions = parse_function_csv(csv_out) if csv_code == 0 and csv_out.strip() else []

    stack_samples = []
    if tool in ("cpu", "threads", "memory"):
        td_args = ["-report", "top-down", "-result-dir", result_dir, "-format", "csv", "-csv-delimiter", "comma"]
        td_code, td_out, _ = await _vtune(vtune, td_args, timeout)
        if td_code == 0 and td_out.strip():
            stack_samples = parse_topdown_csv(td_out)

    csv_path = ""
    if csv_out.strip():
        csv_path = os.path.join(result_dir, "devtools-functions.csv")
        pathlib.Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(csv_path).write_text(csv_out, encoding="utf-8")

    run_base = create_run_base(
        suite="vtune",
        tool=tool,
        binary=binary or f"pid:{pid}",
        args=list(args or []),
        duration_seconds=time.monotonic() - start,
    )
    result = VtuneResult(
        **run_base.model_dump(),
        analysis_type=analysis,
        result_dir=result_dir,
        summary_text=summary_out[:_SUMMARY_MAX_CHARS],
        functions=functions,
        stack_samples=stack_samples,
        csv_path=csv_path,
    )
    assert result.suite == "vtune" and result.tool == tool, "result identity mismatch"
    return None, result, csv_path
