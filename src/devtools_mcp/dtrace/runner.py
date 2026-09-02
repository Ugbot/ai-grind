"""DTrace execution. Run scripts and one-liners."""

from __future__ import annotations

import asyncio
import os
import shlex
import signal
import tempfile
import time

from devtools_mcp.dtrace.models import DTraceResult
from devtools_mcp.dtrace.parsers import parse_dtrace_output
from devtools_mcp.models import create_run_base

_SUDO_FAILURE = ("a password is required", "a terminal is required", "sudo: a password", "no tty present")


def _kill_group(proc: asyncio.subprocess.Process) -> None:
    """SIGKILL the whole group so a sudo'd root dtrace isn't orphaned."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        proc.kill()


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
    env: dict[str, str] | None = None,
    **kwargs: object,
) -> tuple[str | None, DTraceResult | None, str]:
    """Run a DTrace script or one-liner.

    `env`: extra environment for the PROFILED process. Merged over the
    server's own environment (not replacing it, dtrace itself needs PATH and
    sudo needs its own vars). Without this, profiling a binary whose behaviour
    is env-gated (feature flags, worker counts, kill switches) silently
    measures the default configuration instead of the one asked for.

    Returns (error_msg, parsed_result, raw_output_path).
    """
    if tool == "profile":  # legacy alias for the canonical CPU-profiling verb
        tool = "cpu"
    cmd: list[str] = []

    if sudo:
        # -n: never prompt. A headless MCP server has no tty, so an interactive
        # sudo would hang until timeout; fail fast with a clear message instead.
        cmd.extend(["sudo", "-n"])

    cmd.append("dtrace")

    # Add extra args (e.g. -x bufsize=4m)
    if extra_args:
        cmd.extend(extra_args)

    # `trace` has no built-in probe, so devtools_run callers pass the D program
    # via args (or direct callers via script/one_liner), otherwise it's unusable.
    program = one_liner or (" ".join(args) if tool == "trace" and args else "")

    # Script file or one-liner
    if script:
        cmd.extend(["-s", script])
    elif program:
        cmd.extend(["-n", program])
    elif tool == "syscall":
        # Convenience: trace syscalls
        probe = f"syscall:::entry /pid == {pid}/" if pid else "syscall:::entry"
        cmd.extend(["-n", f"{probe} {{ @[probefunc] = count(); }}"])
    elif tool == "cpu":
        # Convenience: CPU profiling (sampled user stacks)
        hz = 97
        probe = f"profile-{hz} /pid == {pid}/" if pid else f"profile-{hz}"
        cmd.extend(["-n", f"{probe} {{ @[ustack()] = count(); }}"])
    else:
        return (
            'dtrace tool=trace needs a D program via args (e.g. args=["syscall:::entry '
            '{ @[probefunc]=count(); }"]), or use tool=syscall / tool=cpu.',
            None,
            "",
        )

    # Attach to process or command (quote each token, dtrace -c splits on spaces)
    if pid and "-p" not in cmd:
        cmd.extend(["-p", str(pid)])
    elif binary and tool != "trace" and "-c" not in cmd:
        cmd_str = " ".join(shlex.quote(part) for part in [binary, *(args or [])])
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
            start_new_session=True,  # own process group so a timeout kills root dtrace too
            env=({**os.environ, **env} if env else None),
        )

        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout,
            )
        except TimeoutError:
            _kill_group(proc)  # SIGKILL the group, SIGTERM to sudo can't reach the child
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

        # Distinguish "sudo can't run non-interactively" from a real DTrace error.
        low = stderr.lower()
        if any(marker in low for marker in _SUDO_FAILURE):
            return (
                "dtrace needs root but sudo cannot prompt here. Configure passwordless "
                "sudo for dtrace, or run the server where sudo is already authenticated.",
                None,
                raw_path,
            )
        if "Permission denied" in stderr or "not permitted" in low:
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
