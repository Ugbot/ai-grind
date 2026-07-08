"""ETW capture + decode via PerfView (ported from marbledb/tools/perf_trace.py).

The hard part isn't automating PerfView — it's the Windows symbol/elevation
plumbing: `_NT_SYMBOL_PATH` for PDB + system-DLL resolution, tolerating the
elevated-child exit-code-2 (the parent exits non-zero while the child keeps
writing the ETL), and waiting for the merge to settle. Capture needs a real exe
and admin; tests exercise the CSV parser with synthetic data.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import time

from devtools_mcp.etw.models import EtwResult
from devtools_mcp.etw.parsers import parse_perfview_csv
from devtools_mcp.flamegraph.fold import parse_folded
from devtools_mcp.models import create_run_base

_KNOWN = pathlib.Path(r"C:/code/PerfView.exe")
_MS_SYMBOLS = "https://msdl.microsoft.com/download/symbols"
_SYMBOL_CACHE = pathlib.Path(os.path.expandvars(r"%LOCALAPPDATA%/Temp/SymbolCache"))


def find_perfview() -> str | None:
    """Locate PerfView: $DEVTOOLS_PERFVIEW -> C:/code/PerfView.exe -> PATH."""
    env = os.environ.get("DEVTOOLS_PERFVIEW")
    if env and pathlib.Path(env).exists():
        return env
    if _KNOWN.exists():
        return str(_KNOWN)
    return shutil.which("PerfView") or shutil.which("PerfView.exe")


async def check_etw() -> dict[str, str]:
    """Detect PerfView availability for the ETW backend."""
    path = find_perfview()
    if path:
        return {"installed": "true", "version": "PerfView", "path": path}
    return {
        "installed": "false",
        "version": "",
        "path": "PerfView.exe",
        "error": "PerfView not found. Put it at C:/code/PerfView.exe, on PATH, or set $DEVTOOLS_PERFVIEW.",
    }


def _symbol_path(extra_dirs: list[pathlib.Path]) -> str:
    parts = [str(d) for d in extra_dirs if d.exists()]
    parts.append(f"SRV*{_SYMBOL_CACHE}*{_MS_SYMBOLS}")
    return ";".join(parts)


def _perfview_env(exe_dir: pathlib.Path | None) -> dict[str, str]:
    env = os.environ.copy()
    env["MSYS_NO_PATHCONV"] = "1"
    dirs = [exe_dir] if exe_dir else []
    env["_NT_SYMBOL_PATH"] = _symbol_path([d for d in dirs if d])
    return env


def _opt(extra_args: list[str] | None, flag: str) -> str | None:
    if not extra_args or flag not in extra_args:
        return None
    i = extra_args.index(flag)
    return extra_args[i + 1] if i + 1 < len(extra_args) else None


async def _perfview(pv: str, args: list[str], env: dict[str, str], timeout: int) -> int:
    proc = await asyncio.create_subprocess_exec(
        pv,
        *args,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,  # _NT_SYMBOL_PATH (exe dir + MS server) — without this,
        # PerfView never resolves the profiled binary's own PDB
    )
    try:
        await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return 124
    return proc.returncode or 0


async def _wait_for_etl(etl: pathlib.Path, timeout_s: int = 120) -> bool:
    """Wait for the elevated collector's merge to settle (size stable, no .etl.new)."""
    deadline = time.monotonic() + timeout_s
    last, stable = -1, 0
    while time.monotonic() < deadline:
        if not etl.exists() or etl.with_suffix(".etl.new").exists():
            await asyncio.sleep(0.5)
            continue
        size = etl.stat().st_size
        if size == last and size > 0:
            stable += 1
            if stable >= 3:
                return True
        else:
            last, stable = size, 0
        await asyncio.sleep(1.0)
    return etl.exists() and etl.stat().st_size > 0


async def run_etw(
    tool: str = "cpu",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    **kwargs: object,
) -> tuple[str | None, EtwResult | None, str]:
    """Capture (or re-decode) an ETW CPU profile and parse the hotspot CSV."""
    if tool != "cpu":
        return f"Unknown etw tool: {tool} (only 'cpu')", None, ""
    pv = find_perfview()
    if not pv:
        return (await check_etw())["error"], None, ""

    binary_args = args or []
    decode_only = bool(extra_args and "--decode-only" in extra_args)
    process = _opt(extra_args, "--process") or pathlib.Path(binary).stem
    etl = pathlib.Path(_opt(extra_args, "--etl") or os.path.join(os.environ.get("TEMP", "."), "devtools-etw.etl"))
    folded = _opt(extra_args, "--folded")
    start = time.monotonic()

    if not decode_only:
        if not binary or not pathlib.Path(binary).exists():
            return f"binary not found: {binary}", None, ""
        env = _perfview_env(pathlib.Path(binary).parent)
        cap = [
            "/AcceptEULA",
            "/NoGui",
            f"/DataFile:{etl}",
            "/NoNGenRundown",
            "/NoClrRundown",
            "/NoV2Rundown",
            "/CpuSampleMSec:0.125",
            "/ThreadTime",
            "/Zip:false",
            "/Merge:true",
            f"/FocusProcess:{process}.exe",
            "run",
            binary,
            *map(str, binary_args),
        ]
        await _perfview(pv, cap, env, timeout)  # exit-code-2 is expected; check the file
        if not await _wait_for_etl(etl):
            return f"ETL never stabilised at {etl}", None, ""

    csv_out = etl.with_suffix(".perfView.csv")
    if csv_out.exists():
        csv_out.unlink()
    env = _perfview_env(pathlib.Path(binary).parent if binary else None)
    decode = ["/AcceptEULA", "/NoGui", "UserCommand", "SaveCPUStacksAsCsv", str(etl), process]
    code = await _perfview(pv, decode, env, timeout)
    if code != 0 or not csv_out.exists():
        return f"SaveCPUStacksAsCsv failed (exit {code}) for {etl}", None, ""

    text = csv_out.read_text(encoding="utf-8", errors="replace")
    samples = parse_perfview_csv(text)
    stack_samples = parse_folded(pathlib.Path(folded).read_text(encoding="utf-8", errors="replace")) if folded else []

    run_base = create_run_base(
        suite="etw", tool="cpu", binary=binary, args=binary_args, duration_seconds=time.monotonic() - start
    )
    result = EtwResult(
        **run_base.model_dump(),
        process=process,
        samples=samples,
        stack_samples=stack_samples,
        etl_path=str(etl),
        csv_path=str(csv_out),
    )
    return None, result, str(csv_out)
