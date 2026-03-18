"""DTrace execution — run scripts and one-liners."""

from __future__ import annotations

import asyncio
import os
import tempfile
import time

from devtools_mcp.dtrace.models import DTraceResult
from devtools_mcp.dtrace.parsers import parse_dtrace_output
from devtools_mcp.models import create_run_base


async def run_dtrace(
    tool: str = "trace",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 30,
    script: str | None = None,
    one_liner: str | None = None,
    pid: int | None = None,
    sudo: bool = True,
    **kwargs: object,
) -> tuple[str | None, DTraceResult | None, str]:
    """Run a DTrace script or one-liner.

    Returns (error_msg, parsed_result, raw_output_path).
    """
    cmd: list[str] = []

    if sudo:
        cmd.append("sudo")

    cmd.append("dtrace")

    # Add extra args (e.g. -x bufsize=4m)
    if extra_args:
        cmd.extend(extra_args)

    # Script file or one-liner
    if script:
        cmd.extend(["-s", script])
    elif one_liner:
        cmd.extend(["-n", one_liner])
    elif tool == "syscall":
        # Convenience: trace syscalls
        probe = f"syscall:::entry /pid == {pid}/" if pid else "syscall:::entry"
        cmd.extend(["-n", f"{probe} {{ @[probefunc] = count(); }}"])
    elif tool == "profile":
        # Convenience: CPU profiling
        hz = 97
        probe = f"profile-{hz} /pid == {pid}/" if pid else f"profile-{hz}"
        cmd.extend(["-n", f"{probe} {{ @[ustack()] = count(); }}"])
    else:
        return "Provide script, one_liner, or use tool=syscall/profile", None, ""

    # Attach to process or command
    if pid and "-p" not in cmd:
        cmd.extend(["-p", str(pid)])
    elif binary and "-c" not in cmd:
        cmd_str = binary
        if args:
            cmd_str += " " + " ".join(args)
        cmd.extend(["-c", cmd_str])

    # Output file
    fd, raw_path = tempfile.mkstemp(prefix="dtrace-", suffix=".out")
    os.close(fd)

    start = time.monotonic()

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout,
            )
        except TimeoutError:
            proc.terminate()
            try:
                stdout_bytes, stderr_bytes = await asyncio.wait_for(
                    proc.communicate(),
                    timeout=5,
                )
            except TimeoutError:
                proc.kill()
                await proc.wait()
                stdout_bytes = b""
                stderr_bytes = b"DTrace timed out"

        duration = time.monotonic() - start
        stdout = stdout_bytes.decode("utf-8", errors="replace")
        stderr = stderr_bytes.decode("utf-8", errors="replace")

        # DTrace outputs data on stdout, diagnostics on stderr
        # Some output goes to stderr (e.g. "dtrace: script ... matched N probes")
        combined = stdout + "\n" + stderr

        # Save raw output
        with open(raw_path, "w") as f:
            f.write(combined)

        # Check for permission errors
        if "Permission denied" in stderr or "not permitted" in stderr.lower():
            return f"DTrace permission denied. On macOS, SIP may need to be configured.\n{stderr}", None, raw_path

        run_base = create_run_base(
            suite="dtrace",
            tool=tool,
            binary=binary,
            args=args,
            duration_seconds=duration,
            exit_code=proc.returncode or 0,
        )

        result = parse_dtrace_output(combined, run_base, script=script or "", one_liner=one_liner or "")
        return None, result, raw_path

    except FileNotFoundError:
        return "dtrace not found. Is DTrace installed?", None, raw_path
    except OSError as e:
        return f"Failed to run dtrace: {e}", None, raw_path


async def check_dtrace(dtrace_path: str = "dtrace") -> dict[str, str]:
    """Check if DTrace is available."""
    try:
        proc = await asyncio.create_subprocess_exec(
            dtrace_path,
            "-V",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
        version = stdout.decode("utf-8", errors="replace").strip().splitlines()[0]
        return {"installed": "true", "version": version, "path": dtrace_path}
    except FileNotFoundError:
        return {
            "installed": "false",
            "version": "",
            "path": dtrace_path,
            "error": f"dtrace not found at '{dtrace_path}'",
        }
    except Exception as e:
        return {"installed": "false", "version": "", "path": dtrace_path, "error": str(e)}
