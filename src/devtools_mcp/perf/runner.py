"""perf execution — stat, record, report, annotate."""

from __future__ import annotations

import asyncio
import os
import tempfile
import time

from devtools_mcp.models import create_run_base
from devtools_mcp.perf.models import PerfAnnotationResult, PerfRecordResult, PerfStatResult
from devtools_mcp.perf.parsers import parse_perf_annotate, parse_perf_report, parse_perf_stat


async def run_perf(
    tool: str = "stat",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    events: list[str] | None = None,
    repeat: int = 1,
    frequency: int | None = None,
    call_graph: str = "dwarf",
    symbol: str | None = None,
    perf_data: str | None = None,
    **kwargs: object,
) -> tuple[str | None, PerfStatResult | PerfRecordResult | PerfAnnotationResult | None, str]:
    """Run a perf tool. Returns (error, result, raw_path)."""
    binary_args = args or []

    if tool == "stat":
        return await _run_perf_stat(binary, binary_args, extra_args, events, repeat, timeout)
    if tool == "record":
        return await _run_perf_record(binary, binary_args, extra_args, events, frequency, call_graph, timeout)
    if tool == "annotate":
        return await _run_perf_annotate(perf_data or "perf.data", symbol, extra_args, timeout)
    return f"Unknown perf tool: {tool}", None, ""


async def _run_perf_stat(
    binary: str,
    binary_args: list[str],
    extra_args: list[str] | None,
    events: list[str] | None,
    repeat: int,
    timeout: int,
) -> tuple[str | None, PerfStatResult | None, str]:
    cmd = ["perf", "stat", "-x", ",", "--"]
    if events:
        cmd[2:2] = ["-e", ",".join(events)]
    if repeat > 1:
        cmd[2:2] = ["-r", str(repeat)]
    if extra_args:
        cmd[2:2] = extra_args
    cmd.extend([binary, *binary_args])

    fd, raw_path = tempfile.mkstemp(prefix="perf-stat-", suffix=".csv")
    os.close(fd)
    start = time.monotonic()

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except TimeoutError:
            proc.kill()
            await proc.wait()
            return "perf stat timed out", None, raw_path

        duration = time.monotonic() - start
        # perf stat outputs CSV to stderr with -x,
        output = stderr.decode("utf-8", errors="replace")
        with open(raw_path, "w") as f:
            f.write(output)

        run_base = create_run_base(
            suite="perf",
            tool="stat",
            binary=binary,
            args=binary_args,
            duration_seconds=duration,
            exit_code=proc.returncode or 0,
        )
        result = parse_perf_stat(output, run_base)
        return None, result, raw_path

    except FileNotFoundError:
        return "perf not found. Is perf installed? (Linux only)", None, raw_path


async def _run_perf_record(
    binary: str,
    binary_args: list[str],
    extra_args: list[str] | None,
    events: list[str] | None,
    frequency: int | None,
    call_graph: str,
    timeout: int,
) -> tuple[str | None, PerfRecordResult | None, str]:
    fd, perf_data = tempfile.mkstemp(prefix="perf-record-", suffix=".data")
    os.close(fd)

    cmd = ["perf", "record", "-o", perf_data, f"--call-graph={call_graph}"]
    if events:
        cmd.extend(["-e", ",".join(events)])
    if frequency:
        cmd.extend(["-F", str(frequency)])
    if extra_args:
        cmd.extend(extra_args)
    cmd.extend(["--", binary, *binary_args])

    start = time.monotonic()
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except TimeoutError:
            proc.kill()
            await proc.wait()

        duration = time.monotonic() - start

        # Now run perf report to get the profile
        report_proc = await asyncio.create_subprocess_exec(
            "perf",
            "report",
            "--stdio",
            "-i",
            perf_data,
            "--no-children",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        report_stdout, _ = await asyncio.wait_for(report_proc.communicate(), timeout=30)
        report_text = report_stdout.decode("utf-8", errors="replace")

        raw_path = perf_data + ".report"
        with open(raw_path, "w") as f:
            f.write(report_text)

        run_base = create_run_base(
            suite="perf",
            tool="record",
            binary=binary,
            args=binary_args,
            duration_seconds=duration,
            exit_code=proc.returncode or 0,
        )
        result = parse_perf_report(report_text, run_base)
        result.perf_data_path = perf_data
        return None, result, raw_path

    except FileNotFoundError:
        return "perf not found. Is perf installed? (Linux only)", None, perf_data


async def _run_perf_annotate(
    perf_data: str,
    symbol: str | None,
    extra_args: list[str] | None,
    timeout: int,
) -> tuple[str | None, PerfAnnotationResult | None, str]:
    cmd = ["perf", "annotate", "--stdio", "-i", perf_data]
    if symbol:
        cmd.extend(["-s", symbol])
    if extra_args:
        cmd.extend(extra_args)

    fd, raw_path = tempfile.mkstemp(prefix="perf-annotate-", suffix=".txt")
    os.close(fd)

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        output = stdout.decode("utf-8", errors="replace")

        with open(raw_path, "w") as f:
            f.write(output)

        run_base = create_run_base(
            suite="perf",
            tool="annotate",
            binary=perf_data,
            exit_code=proc.returncode or 0,
        )
        result = parse_perf_annotate(output, run_base, symbol=symbol or "")
        return None, result, raw_path

    except FileNotFoundError:
        return "perf not found. Is perf installed? (Linux only)", None, raw_path


async def check_perf(perf_path: str = "perf") -> dict[str, str]:
    """Check if perf is available."""
    try:
        proc = await asyncio.create_subprocess_exec(
            perf_path,
            "version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
        version = stdout.decode("utf-8", errors="replace").strip()
        return {"installed": "true", "version": version, "path": perf_path}
    except FileNotFoundError:
        return {
            "installed": "false",
            "version": "",
            "path": perf_path,
            "error": f"perf not found at '{perf_path}' (Linux only)",
        }
    except Exception as e:
        return {"installed": "false", "version": "", "path": perf_path, "error": str(e)}
