"""Execution + result assembly for the JS package managers (npm/pnpm/yarn).

They share one shape: run a command, capture output, and assemble a normalized
BuildResult from the parsed deps/vulns/scripts. `capture` returns both a
parse-target text and the raw text so the runner can persist the full log while
handing the parser a clean copy.
"""

from __future__ import annotations

from devtools_mcp.build.exec import run_capture, tail
from devtools_mcp.build.models import BuildResult, BuildTask, Dependency, Vulnerability
from devtools_mcp.models import create_run_base


async def capture(cmd: list[str], project: str, timeout: int, tool: str) -> tuple[int, str, str]:
    """Run `cmd` in `project`; return (returncode, parse_text, raw_text).

    Both texts are the merged stdout+stderr today; the split lets callers keep
    the full log while a future tool-specific cleaner can pre-slice `parse_text`.
    """
    assert cmd, "empty command"
    assert isinstance(tool, str), "tool must be str"
    rc, text = await run_capture(cmd, cwd=project, timeout=timeout)
    return rc, text, text


def assemble(
    suite: str,
    tool: str,
    project: str,
    command: str,
    duration: float,
    rc: int,
    raw: str,
    *,
    deps: list[Dependency] | None = None,
    vulns: list[Vulnerability] | None = None,
    scripts: dict[str, str] | None = None,
) -> BuildResult:
    """Assemble a normalized BuildResult for a JS package-manager run."""
    assert isinstance(suite, str) and suite, "suite required"
    assert isinstance(tool, str) and tool, "tool required"
    base = create_run_base(suite=suite, tool=tool, binary=project, duration_seconds=duration, exit_code=rc)
    available = [BuildTask(name=name, description=cmd, group="scripts") for name, cmd in (scripts or {}).items()]
    return BuildResult(
        **base.model_dump(),
        command=command,
        success=rc == 0,
        dependencies=deps or [],
        vulnerabilities=vulns or [],
        available_tasks=available,
        raw_output=tail(raw),
    )
