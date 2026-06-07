"""CDB batch-mode execution: run a scripted command sequence, capture, parse.

Headless and non-interactive (`-c "...;q"`), so no pseudo-console is needed —
the same philosophy as pwsh-non-interactive. Works on a crash dump (`--dump`) or
a live exe. cdb.exe ships with the Windows SDK "Debugging Tools for Windows".
Capture needs cdb installed + a target; tests exercise the parsers.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import time

from devtools_mcp.cdb.models import CdbSnapshot
from devtools_mcp.cdb.parsers import parse_analyze, parse_registers, parse_stacks
from devtools_mcp.models import create_run_base

_SDK_PATHS = [
    r"C:/Program Files (x86)/Windows Kits/10/Debuggers/x64/cdb.exe",
    r"C:/Program Files (x86)/Windows Kits/10/Debuggers/x86/cdb.exe",
    r"C:/Program Files/Windows Kits/10/Debuggers/x64/cdb.exe",
]
_SCRIPTS = {"stacks": "~*kn", "analyze": "!analyze -v", "inspect": "kn; r"}


def find_cdb() -> str | None:
    """Locate cdb.exe: $DEVTOOLS_CDB -> PATH -> Windows SDK Debuggers."""
    env = os.environ.get("DEVTOOLS_CDB")
    if env and pathlib.Path(env).exists():
        return env
    found = shutil.which("cdb")
    if found:
        return found
    for p in _SDK_PATHS:
        if pathlib.Path(p).exists():
            return p
    return None


async def check_cdb() -> dict[str, str]:
    """Detect cdb for the Windows debugger backend."""
    path = find_cdb()
    if path:
        return {"installed": "true", "version": "cdb", "path": path}
    return {
        "installed": "false", "version": "", "path": "cdb.exe",
        "error": "cdb not found. Install Debugging Tools for Windows (Windows SDK) "
                 "or `winget install Microsoft.WinDbg`, or set $DEVTOOLS_CDB.",
    }


def _opt(extra_args: list[str] | None, flag: str) -> str | None:
    if extra_args and flag in extra_args:
        i = extra_args.index(flag)
        if i + 1 < len(extra_args):
            return extra_args[i + 1]
    return None


async def run_cdb(
    tool: str = "stacks",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 120,
    **kwargs: object,
) -> tuple[str | None, CdbSnapshot | None, str]:
    """Run a CDB script on a dump or live exe; parse into a snapshot."""
    cdb = find_cdb()
    if not cdb:
        return (await check_cdb())["error"], None, ""
    if tool not in _SCRIPTS:
        return f"Unknown cdb tool: {tool} (stacks|analyze|inspect)", None, ""

    script = _SCRIPTS[tool] + "; q"
    dump = _opt(extra_args, "--dump")
    if dump:
        cmd = [cdb, "-z", dump, "-c", script]
        target = dump
    elif binary:
        cmd = [cdb, "-g", "-G", "-c", script, binary, *(args or [])]
        target = binary
    else:
        return "Need a live exe (binary) or a crash dump (--dump path).", None, ""

    start = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return "cdb timed out", None, ""
    text = out.decode("utf-8", "replace")

    snapshot = parse_cdb_output(tool, text, target, time.monotonic() - start)
    return None, snapshot, ""


def parse_cdb_output(tool: str, text: str, target: str, duration: float) -> CdbSnapshot:
    """Build a CdbSnapshot from raw cdb output (pure — used by tests too)."""
    base = create_run_base(suite="cdb", tool=tool, binary=target, duration_seconds=duration)
    threads = parse_stacks(text)
    analysis, exception = parse_analyze(text) if tool == "analyze" else ({}, "")
    registers = parse_registers(text) if tool in ("inspect", "analyze") else {}
    return CdbSnapshot(**base.model_dump(), threads=threads, analysis=analysis,
                       exception=exception, registers=registers, raw_output=text)
