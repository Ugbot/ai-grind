"""Execution + result assembly for the JS package managers (npm/pnpm/yarn).

They share one shape: run a command, capture output, and assemble a normalized
BuildResult from the parsed deps/vulns/scripts. `capture` returns both a
parse-target text and the raw text so the runner can persist the full log while
handing the parser a clean copy.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from devtools_mcp.build.exec import run_capture, tail, write_raw
from devtools_mcp.build.jsdeps import parse_package_scripts
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
    success: bool | None = None,
) -> BuildResult:
    """Assemble a normalized BuildResult for a JS package-manager run.

    `success` overrides the default rc==0 for informational tools (audit and
    outdated exit non-zero when they find something — that is not a failure).
    """
    assert isinstance(suite, str) and suite, "suite required"
    assert isinstance(tool, str) and tool, "tool required"
    base = create_run_base(suite=suite, tool=tool, binary=project, duration_seconds=duration, exit_code=rc)
    available = [BuildTask(name=name, description=cmd, group="scripts") for name, cmd in (scripts or {}).items()]
    return BuildResult(
        **base.model_dump(),
        command=command,
        success=rc == 0 if success is None else success,
        dependencies=deps or [],
        vulnerabilities=vulns or [],
        available_tasks=available,
        raw_output=tail(raw),
    )


def _no_parse(_text: str) -> list:
    return []


@dataclass(frozen=True)
class JsPackageManager:
    """One JS package manager's config; the runner logic is shared (run_pm).

    npm/pnpm/yarn differ only in the binary, version banner, argv per verb, and
    which parser reads deps/outdated/audit output — so those are data here and
    the flow lives once in run_pm. `tasks` is handled uniformly from package.json.
    """

    suite: str
    version_prefix: str
    argv: dict[str, Callable[[list[str], list[str]], list[str]]]
    not_found: str
    dep_parser: Callable[[str], list[Dependency]] = _no_parse
    outdated_parser: Callable[[str], list[Dependency]] = _no_parse
    audit_parser: Callable[[str], list[Vulnerability]] = _no_parse
    informational: frozenset[str] = field(default_factory=lambda: frozenset({"audit", "outdated"}))

    def resolve(self) -> str | None:
        return shutil.which(self.suite)

    def verbs(self) -> str:
        return "|".join([*self.argv, "tasks"])


async def check_pm(pm: JsPackageManager) -> dict[str, str]:
    """Probe `<pm> --version`; version banner is prefix + reported version."""
    exe = pm.resolve()
    version = ""
    if exe:
        try:
            proc = await asyncio.create_subprocess_exec(
                exe, "--version", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
            )
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            version = pm.version_prefix + out.decode("utf-8", "replace").strip()
        except (TimeoutError, OSError):
            version = ""
    return {"path": exe or "", "version": version}


async def run_pm(
    pm: JsPackageManager,
    tool: str = "deps",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    **kwargs: object,
) -> tuple[str | None, BuildResult | None, str]:
    """Run one verb of a JS package manager and normalize the output."""
    project = binary or os.getcwd()
    if not pathlib.Path(project).is_dir():
        return f"project dir not found: {project}", None, ""
    exe = pm.resolve()
    if not exe:
        return pm.not_found, None, ""
    if tool == "tasks":
        scripts = parse_package_scripts(project)
        return None, assemble(pm.suite, "tasks", project, f"{pm.suite} run", 0.0, 0, "", scripts=scripts), ""
    if tool not in pm.argv:
        return f"Unknown {pm.suite} tool: {tool} ({pm.verbs()})", None, ""

    argv = pm.argv[tool](args or [], extra_args or [])
    start = time.monotonic()
    rc, ptext, raw = await capture([exe, *argv], project, timeout, tool)
    raw_path = write_raw(f"devtools-{pm.suite}-", raw)
    deps = pm.dep_parser(ptext) if tool == "deps" else (pm.outdated_parser(ptext) if tool == "outdated" else [])
    vulns = pm.audit_parser(ptext) if tool == "audit" else []
    # audit/outdated exit non-zero when they find things — findings are not failure.
    success = (rc == 0 or bool(deps) or bool(vulns)) if tool in pm.informational else None
    result = assemble(
        pm.suite,
        tool,
        project,
        f"{pm.suite} " + " ".join(argv),
        time.monotonic() - start,
        rc,
        raw,
        deps=deps,
        vulns=vulns,
        success=success,
    )
    return None, result, raw_path
