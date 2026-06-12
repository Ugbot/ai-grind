"""Gradle execution: prefer the project's `gradlew` wrapper, else `gradle`."""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import time

from devtools_mcp.build.exec import run_capture, tail, write_raw
from devtools_mcp.build.models import BuildResult
from devtools_mcp.build.parsers import parse_junit_dir
from devtools_mcp.gradle.parsers import parse_gradle_build, parse_gradle_deps, parse_gradle_tasks
from devtools_mcp.models import create_run_base

_JUNIT_DIRS = ["**/build/test-results/**/*.xml"]
_PLAIN = "--console=plain"


def resolve_gradle(project_dir: str) -> str | None:
    """gradlew wrapper in the project, else gradle on PATH."""
    for w in ("gradlew.bat", "gradlew"):
        p = pathlib.Path(project_dir) / w
        if p.exists():
            return str(p)
    return shutil.which("gradle")


async def check_gradle() -> dict[str, str]:
    gradle = shutil.which("gradle")
    version = ""
    if gradle:
        try:
            proc = await asyncio.create_subprocess_exec(
                gradle, "-v", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
            )
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=20)
            for line in out.decode("utf-8", "replace").splitlines():
                if line.startswith("Gradle "):
                    version = line.strip()
                    break
        except (TimeoutError, OSError):
            version = ""
    return {"path": gradle or "", "version": version}


def _argv(tool: str, args: list[str] | None, extra: list[str] | None) -> list[str]:
    extra = extra or []
    if tool == "build":
        return [*(args or ["build"]), _PLAIN, *extra]
    if tool == "test":
        return ["test", _PLAIN, *extra]
    if tool == "tasks":
        return ["tasks", "--all", _PLAIN]
    if tool == "deps":
        return [*(args or ["dependencies"]), _PLAIN, *extra]
    if tool == "sync":
        return [*(args or ["dependencies"]), "--refresh-dependencies", _PLAIN, *extra]
    return []


async def run_gradle(
    tool: str = "build",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    **kwargs: object,
) -> tuple[str | None, BuildResult | None, str]:
    """Run a Gradle task in a project directory and normalize the output."""
    project = binary or os.getcwd()
    if not pathlib.Path(project).is_dir():
        return f"project dir not found: {project}", None, ""
    gradle = resolve_gradle(project)
    if not gradle:
        return "Gradle not found. Add a gradlew wrapper to the project or install gradle.", None, ""
    argv = _argv(tool, args, extra_args)
    if not argv:
        return f"Unknown gradle tool: {tool} (build|test|tasks|dependencies|refresh)", None, ""

    start = time.monotonic()
    rc, text = await run_capture([gradle, *argv], cwd=project, timeout=timeout)
    raw_path = write_raw("devtools-gradle-", text)

    success, exec_tasks, failures = parse_gradle_build(text)
    if tool not in ("build", "test"):
        success = rc == 0
    deps = parse_gradle_deps(text) if tool in ("deps", "sync") else []
    available = parse_gradle_tasks(text) if tool == "tasks" else []
    tests = parse_junit_dir(project, _JUNIT_DIRS) if tool in ("build", "test") else []

    base = create_run_base(
        suite="gradle", tool=tool, binary=project, args=argv, duration_seconds=time.monotonic() - start, exit_code=rc
    )
    result = BuildResult(
        **base.model_dump(),
        command=" ".join(["gradle", *argv]),
        success=success,
        dependencies=deps,
        tests=tests,
        executed_tasks=exec_tasks if tool in ("build", "test") else [],
        available_tasks=available,
        failures=failures,
        raw_output=tail(text),
    )
    return None, result, raw_path
