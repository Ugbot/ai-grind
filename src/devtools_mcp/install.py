"""Install-plan resolution and execution for backend tool dependencies.

MCP-free and testable: the devtools_install tool (tools/install_tools.py) is a
thin wrapper. Dry-run (format_plan) is the primary mode, the agent runs the
commands in its own shell under the client's permission system. Execution
(run_steps) is opt-in and env-gated at the tool layer.
"""

from __future__ import annotations

import asyncio
import pathlib
import sys

from devtools_mcp.registry import MAX_INSTALL_STEPS, InstallSpec, InstallStep

_OUTPUT_TAIL_LINES = 20
_DOWNLOAD_MAX_BYTES = 512 * 1024 * 1024  # 512 MB cap on kind="download"
_DOWNLOAD_CHUNK = 1 << 16

_PLATFORMS = {"win32": "windows", "linux": "linux", "darwin": "darwin"}


def resolve_platform() -> str:
    """Map sys.platform to the InstallSpec platform key."""
    platform = _PLATFORMS.get(sys.platform, "")
    assert sys.platform, "sys.platform must not be empty"
    return platform


def steps_for(spec: InstallSpec, platform: str | None = None) -> list[InstallStep]:
    """Ordered install steps for the (current) platform; [] if unsupported."""
    assert isinstance(spec, InstallSpec), f"expected InstallSpec, got {type(spec)}"
    key = platform if platform is not None else resolve_platform()
    steps = spec.platforms.get(key, [])  # type: ignore[call-overload]
    assert len(steps) <= MAX_INSTALL_STEPS, "steps exceed bound"
    return list(steps)


def format_plan(suite: str, steps: list[InstallStep], note: str = "", url: str = "") -> str:
    """Dry-run output: numbered verbatim commands with elevation markers."""
    assert suite, "suite must not be empty"
    if not steps:
        lines = [f"No install commands for suite '{suite}' on this platform ({resolve_platform() or sys.platform})."]
        if url:
            lines.append(f"Manual install: {url}")
        return "\n".join(lines)
    lines = [f"**Install plan for '{suite}'** ({len(steps)} step(s)): run these in your shell:", ""]
    for i, step in enumerate(steps, 1):
        marker = " [admin]" if step.elevation else ""
        if step.kind == "download":
            lines.append(f"{i}. download {step.argv[0]} -> {step.argv[1]}{marker}: {step.description}")
        else:
            lines.append(f"{i}. `{' '.join(step.argv)}`{marker}: {step.description}")
    if any(s.elevation for s in steps):
        lines.append("")
        lines.append("[admin] steps need an elevated shell (Windows: run as Administrator; POSIX: sudo).")
    if note:
        lines.append("")
        lines.append(f"Note: {note}")
    if url:
        lines.append(f"Docs: {url}")
    return "\n".join(lines)


async def run_steps(steps: list[InstallStep], timeout: int = 900) -> list[tuple[InstallStep, int, str]]:
    """Run steps sequentially, stopping at the first failure.

    Returns (step, exit_code, bounded output tail) per attempted step.
    Commands run as argv vectors. Never through a shell.
    """
    assert 0 < len(steps) <= MAX_INSTALL_STEPS, f"bad step count {len(steps)}"
    assert timeout > 0, f"bad timeout {timeout}"
    results: list[tuple[InstallStep, int, str]] = []
    for step in steps:
        if step.kind == "download":
            code, output = await _download(step.argv[0], step.argv[1])
        else:
            code, output = await _run_argv(step.argv, timeout)
        results.append((step, code, output))
        if code != 0:
            break
    assert len(results) <= len(steps), "more results than steps"
    return results


async def _run_argv(argv: list[str], timeout: int) -> tuple[int, str]:
    """One subprocess; returns (exit_code, last output lines)."""
    assert argv, "empty argv"
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except (OSError, FileNotFoundError) as exc:
        return 127, f"{argv[0]}: {exc}"
    try:
        out_bytes, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return 124, f"timed out after {timeout}s"
    text = out_bytes.decode("utf-8", errors="replace")
    tail = "\n".join(text.strip().splitlines()[-_OUTPUT_TAIL_LINES:])
    return proc.returncode or 0, tail


async def _download(url: str, dest: str) -> tuple[int, str]:
    """Stream url -> dest with a size cap; returns (0, message) on success."""
    assert url.startswith(("http://", "https://")), f"bad download url {url!r}"
    import httpx

    path = pathlib.Path(dest)
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    try:
        async with (
            httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client,
            client.stream("GET", url) as response,
        ):
            response.raise_for_status()
            with open(path, "wb") as f:
                async for chunk in response.aiter_bytes(_DOWNLOAD_CHUNK):
                    written += len(chunk)
                    if written > _DOWNLOAD_MAX_BYTES:
                        raise ValueError(f"download exceeds {_DOWNLOAD_MAX_BYTES} bytes")
                    f.write(chunk)
    except Exception as exc:  # noqa: BLE001  # network/user errors, not invariants
        path.unlink(missing_ok=True)
        return 1, f"download failed: {type(exc).__name__}: {exc}"
    assert written <= _DOWNLOAD_MAX_BYTES, "download bound violated"
    return 0, f"downloaded {written:,} bytes -> {dest}"
