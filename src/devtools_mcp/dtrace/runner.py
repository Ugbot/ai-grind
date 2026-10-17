"""DTrace execution. Run scripts and one-liners."""

from __future__ import annotations

import asyncio
import os
import re
import shlex
import signal
import tempfile
import time

from devtools_mcp.dtrace.models import DTraceCollectionDiagnostics, DTraceResult
from devtools_mcp.dtrace.parsers import parse_dtrace_output
from devtools_mcp.models import create_run_base

_SUDO_FAILURE = ("a password is required", "a terminal is required", "sudo: a password", "no tty present")



_MAX_DIAGNOSTIC_SCAN_BYTES = 16 << 20
_MAX_DIAGNOSTIC_EXAMPLE_CHARS = 1024
_DROP = re.compile(r"\b(?:drops?|dropped)\b")
_DROP_COUNT = re.compile(r"^(\d+)\s")
_ERROR = re.compile(r"^(?:errors?\b|failed\b|fault\b|invalid\b|could not\b|cannot\b)")


def _collection_diagnostics(
    stderr: bytes, *, capture_complete: bool = True, max_scan_bytes: int = _MAX_DIAGNOSTIC_SCAN_BYTES,
) -> DTraceCollectionDiagnostics:
    """Scan captured bytes before summary truncation; examples and scan are bounded.

    Counts describe observed diagnostic lines, not lost events. A capped scan
    counts only complete lines and never certifies the unexamined suffix.
    """
    assert 0 < max_scan_bytes <= _MAX_DIAGNOSTIC_SCAN_BYTES
    extent = min(len(stderr), max_scan_bytes)
    scanned = stderr[:extent]
    if extent < len(stderr) and not scanned.endswith(b"\n"):
        scanned = scanned[:scanned.rfind(b"\n") + 1]
    result = DTraceCollectionDiagnostics(
        captured_stderr_bytes=len(stderr), scanned_stderr_bytes=len(scanned),
        capture_complete=capture_complete, scan_complete=len(scanned) == len(stderr),
    )
    # Prefer faults/drops over advisories if the bounded example list fills.
    examples: list[tuple[int, str]] = []
    for raw in scanned.splitlines():
        line = raw.decode("utf-8", errors="replace").strip()
        if not line.startswith("dtrace:"):
            continue
        message = line[len("dtrace:"):].strip().lower()
        dropped = _DROP.search(message) is not None
        count = _DROP_COUNT.match(message)
        if dropped and count is not None and not count[1].strip("0"):
            dropped = False
        error = _ERROR.search(message) is not None
        result.diagnostic_lines += 1
        result.error_lines += int(error)
        result.drop_lines += int(dropped)
        result.advisory_lines += int(not error and not dropped)
        sample = line[:_MAX_DIAGNOSTIC_EXAMPLE_CHARS]
        result.examples_truncated |= len(sample) < len(line)
        item = (0 if error or dropped else 1, sample)
        if item in examples:
            continue
        examples.append(item)
        examples.sort(key=lambda item: item[0])
        if len(examples) > 8:
            del examples[8:]
            result.examples_truncated = True
    result.examples = [line for _, line in examples]
    return result


def _kill_group(proc: asyncio.subprocess.Process) -> None:
    """SIGKILL the whole group so a sudo'd root dtrace isn't orphaned."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        proc.kill()


def _target_pred(pid: int | None, binary: str) -> str:
    """Restrict a convenience probe to the profiled process.

    With -p PID the PID is known; with -c the launched child is $target. Without
    a predicate the cpu/syscall verbs sample the whole machine.
    """
    if pid:
        return f" /pid == {pid}/"
    if binary:
        return " /pid == $target/"
    return ""


def build_dtrace_cmd(
    tool: str = "trace",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    script: str | None = None,
    one_liner: str | None = None,
    pid: int | None = None,
    sudo: bool = True,
    env: dict[str, str] | None = None,
) -> tuple[list[str], str]:
    """Build the dtrace argv. Returns (cmd, error); error is "" on success."""
    if tool == "profile":  # legacy alias for the canonical CPU-profiling verb
        tool = "cpu"
    cmd: list[str] = []
    if sudo:
        # -n: never prompt. A headless MCP server has no tty, so an interactive
        # sudo would hang until timeout; fail fast with a clear message instead.
        cmd.extend(["sudo", "-n"])
    cmd.append("dtrace")
    if extra_args:
        cmd.extend(extra_args)

    # `trace` has no built-in probe, so devtools_run callers pass the D program
    # via args (or direct callers via script/one_liner) — otherwise it's unusable.
    program = one_liner or (" ".join(args) if tool == "trace" and args else "")
    pred = _target_pred(pid, binary)
    if script:
        cmd.extend(["-s", script])
    elif program:
        cmd.extend(["-n", program])
    elif tool == "syscall":
        cmd.extend(["-n", f"syscall:::entry{pred} {{ @[probefunc] = count(); }}"])
    elif tool == "cpu":
        hz = 97
        cmd.extend(["-n", f"profile-{hz}{pred} {{ @[ustack()] = count(); }}"])
    else:
        return [], ('dtrace tool=trace needs a D program via args (e.g. args=["syscall:::entry '
                    '{ @[probefunc]=count(); }"]), or use tool=syscall / tool=cpu.')

    # Attach to process or command (quote each token — dtrace -c splits on spaces)
    if pid and "-p" not in cmd:
        cmd.extend(["-p", str(pid)])
    elif binary and tool != "trace" and "-c" not in cmd:
        # dtrace -c runs the child with dtrace's own environment, which sudo has
        # reset; run_dtrace therefore launches env-carrying targets itself and
        # attaches with -p instead (see _spawn_for_attach).
        cmd.extend(["-c", " ".join(shlex.quote(part) for part in [binary, *(args or [])])])
    return cmd, ""


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
    child = None
    run_binary = binary  # what the run records, even when we attach by pid
    if env and binary and not pid and tool != "trace":
        # sudo resets the environment (env_reset, no SETENV for dtrace) and SIP
        # stops dtrace -c from exec'ing env(1), so a -c child can never see
        # `env`. Launch the target ourselves - as the calling user, with the
        # requested environment - and attach to it. The first few ms before the
        # attach are not sampled.
        child = await asyncio.create_subprocess_exec(
            binary, *(args or []), env={**os.environ, **env},
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True)
        pid, binary = child.pid, ""
    cmd, err = build_dtrace_cmd(tool, binary=binary, args=args, extra_args=extra_args, script=script,
                                one_liner=one_liner, pid=pid, sudo=sudo, env=env)
    if err:
        if child is not None:
            _kill_group(child)
        return (err, None, "")

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

        capture_complete = True
        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout,
            )
        except TimeoutError:
            _kill_group(proc)  # SIGKILL the group, SIGTERM to sudo can't reach the child
            await proc.wait()
            capture_complete = False
            stdout_bytes = b""
            stderr_bytes = b"DTrace timed out"

        if child is not None:
            # dtrace -p returns when the target exits; reap it, and never leave
            # a profiled program running past a timeout or an attach failure.
            try:
                await asyncio.wait_for(child.wait(), timeout=5)
            except TimeoutError:
                _kill_group(child)
                await child.wait()

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
            binary=run_binary,
            args=args,
            duration_seconds=duration,
            exit_code=proc.returncode or 0,
        )

        result = parse_dtrace_output(combined, run_base, script=script or "", one_liner=one_liner or "")
        # Timeout currently discards communicate's partial output; its synthetic
        # message in raw output is not original captured stderr.
        result.collection_diagnostics = _collection_diagnostics(
            stderr_bytes if capture_complete else b"", capture_complete=capture_complete,
        )
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
