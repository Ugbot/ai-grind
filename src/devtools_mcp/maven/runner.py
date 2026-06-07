"""Maven execution: prefer the project's `mvnw` wrapper, else `mvn` on PATH."""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import time

from devtools_mcp.build.exec import run_capture, tail, write_raw
from devtools_mcp.build.models import BuildResult
from devtools_mcp.build.parsers import parse_junit_dir
from devtools_mcp.maven.parsers import parse_maven_build, parse_maven_resolve, parse_maven_tree
from devtools_mcp.models import create_run_base

_JUNIT_DIRS = ["**/target/surefire-reports/*.xml", "**/target/failsafe-reports/*.xml"]


def resolve_maven(project_dir: str) -> str | None:
    """mvnw wrapper in the project, else mvn on PATH."""
    for w in ("mvnw.cmd", "mvnw.bat", "mvnw"):
        p = pathlib.Path(project_dir) / w
        if p.exists():
            return str(p)
    return shutil.which("mvn")


async def check_maven() -> dict[str, str]:
    mvn = shutil.which("mvn")
    version = ""
    if mvn:
        try:
            proc = await asyncio.create_subprocess_exec(
                mvn, "-v", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            version = out.decode("utf-8", "replace").splitlines()[0] if out else ""
        except (TimeoutError, OSError, IndexError):
            version = ""
    return {"path": mvn or "", "version": version}


async def run_maven(
    tool: str = "build",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    **kwargs: object,
) -> tuple[str | None, BuildResult | None, str]:
    """Run a Maven goal/phase in a project directory and normalize the output."""
    project = binary or os.getcwd()
    if not pathlib.Path(project).is_dir():
        return f"project dir not found: {project}", None, ""
    mvn = resolve_maven(project)
    if not mvn:
        return "Maven not found. Install it (e.g. `choco install maven`) or add an mvnw wrapper.", None, ""

    goals = {"build": args or ["package"], "test": ["test"],
             "deps": ["dependency:tree"], "sync": ["dependency:resolve"]}.get(tool)
    if goals is None:
        return f"Unknown maven tool: {tool} (build|test|deps|sync)", None, ""

    cmd = [mvn, "-B", "-ntp", *goals, *(extra_args or [])]
    start = time.monotonic()
    rc, text = await run_capture(cmd, cwd=project, timeout=timeout)
    raw_path = write_raw("devtools-mvn-", text)

    success, modules, failures = parse_maven_build(text)
    if tool in ("build", "test") and not modules:
        success = rc == 0 and "BUILD FAILURE" not in text
    deps = parse_maven_tree(text) if tool == "deps" else (parse_maven_resolve(text) if tool == "sync" else [])
    tests = parse_junit_dir(project, _JUNIT_DIRS) if tool in ("build", "test") else []

    base = create_run_base(suite="maven", tool=tool, binary=project, args=goals,
                           duration_seconds=time.monotonic() - start, exit_code=rc)
    result = BuildResult(**base.model_dump(), command=" ".join(["mvn", *goals]),
                         success=success, dependencies=deps, tests=tests, modules=modules,
                         failures=failures, raw_output=tail(text))
    return None, result, raw_path
