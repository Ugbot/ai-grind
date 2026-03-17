"""Async subprocess runner for valgrind invocations."""

from __future__ import annotations

import asyncio
import os
import tempfile
import time

from valgrind_mcp.models import RunResult

# Tools that support XML output
XML_TOOLS = {"memcheck", "helgrind", "drd"}

# Tool-specific output file flags
OUTPUT_FILE_FLAGS = {
    "callgrind": "--callgrind-out-file",
    "cachegrind": "--cachegrind-out-file",
    "massif": "--massif-out-file",
}

# Default extra flags per tool
DEFAULT_TOOL_FLAGS: dict[str, list[str]] = {
    "memcheck": ["--leak-check=full", "--show-leak-kinds=all", "--track-origins=yes"],
    "helgrind": [],
    "drd": [],
    "callgrind": ["--cache-sim=yes", "--branch-sim=yes"],
    "cachegrind": [],
    "massif": ["--stacks=yes"],
}


async def run_valgrind(
    tool: str,
    binary: str,
    binary_args: list[str] | None = None,
    valgrind_args: list[str] | None = None,
    timeout: int = 300,
    working_dir: str | None = None,
    valgrind_path: str = "valgrind",
) -> RunResult:
    """Run a valgrind tool against a binary and return the result.

    Creates a temporary file for tool output, builds the correct command line,
    and runs valgrind asynchronously with a configurable timeout.
    """
    binary_args = binary_args or []
    valgrind_args = valgrind_args or []
    working_dir = working_dir or os.path.dirname(os.path.abspath(binary))

    # Create temp file for output
    suffix = ".xml" if tool in XML_TOOLS else ".out"
    tmp_fd, tmp_path = tempfile.mkstemp(prefix=f"valgrind-{tool}-", suffix=suffix)
    os.close(tmp_fd)

    # Build command
    cmd: list[str] = [valgrind_path]

    # Tool selection (memcheck is default)
    if tool != "memcheck":
        cmd.append(f"--tool={tool}")

    # Default flags for this tool
    cmd.extend(DEFAULT_TOOL_FLAGS.get(tool, []))

    # Output file configuration
    if tool in XML_TOOLS:
        cmd.extend(["--xml=yes", f"--xml-file={tmp_path}"])
    elif tool in OUTPUT_FILE_FLAGS:
        cmd.append(f"{OUTPUT_FILE_FLAGS[tool]}={tmp_path}")

    # User-provided extra valgrind args
    cmd.extend(valgrind_args)

    # The binary and its args
    cmd.append(binary)
    cmd.extend(binary_args)

    start = time.monotonic()

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=working_dir,
        )

        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except TimeoutError:
            proc.kill()
            await proc.wait()
            duration = time.monotonic() - start
            return RunResult(
                exit_code=-1,
                stdout="",
                stderr=f"Valgrind timed out after {timeout}s",
                output_path=tmp_path,
                duration_seconds=duration,
                valgrind_args_used=cmd,
            )

        duration = time.monotonic() - start

        return RunResult(
            exit_code=proc.returncode or 0,
            stdout=stdout_bytes.decode("utf-8", errors="replace"),
            stderr=stderr_bytes.decode("utf-8", errors="replace"),
            output_path=tmp_path,
            duration_seconds=duration,
            valgrind_args_used=cmd,
        )

    except FileNotFoundError:
        duration = time.monotonic() - start
        return RunResult(
            exit_code=-1,
            stdout="",
            stderr=f"valgrind not found at '{valgrind_path}'. Is valgrind installed?",
            output_path=tmp_path,
            duration_seconds=duration,
            valgrind_args_used=cmd,
        )
    except OSError as e:
        duration = time.monotonic() - start
        return RunResult(
            exit_code=-1,
            stdout="",
            stderr=f"Failed to run valgrind: {e}",
            output_path=tmp_path,
            duration_seconds=duration,
            valgrind_args_used=cmd,
        )


async def check_valgrind(valgrind_path: str = "valgrind") -> dict[str, str]:
    """Check if valgrind is installed and return version info."""
    try:
        proc = await asyncio.create_subprocess_exec(
            valgrind_path,
            "--version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
        version = stdout.decode("utf-8", errors="replace").strip()
        return {"installed": "true", "version": version, "path": valgrind_path}
    except FileNotFoundError:
        return {
            "installed": "false",
            "version": "",
            "path": valgrind_path,
            "error": f"valgrind not found at '{valgrind_path}'",
        }
    except Exception as e:
        return {"installed": "false", "version": "", "path": valgrind_path, "error": str(e)}
