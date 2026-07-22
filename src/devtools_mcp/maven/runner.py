"""Maven execution: prefer the project's `mvnw` wrapper, else `mvn` on PATH."""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import time

from devtools_mcp.build.exec import run_capture, tail, write_raw
from devtools_mcp.build.models import BuildResult
from devtools_mcp.build.osv import query_osv
from devtools_mcp.build.parsers import parse_junit_dir
from devtools_mcp.maven.parsers import (
    parse_maven_build,
    parse_maven_outdated,
    parse_maven_projects,
    parse_maven_resolve,
    parse_maven_tree,
)
from devtools_mcp.models import create_run_base

_JUNIT_DIRS = ["**/target/surefire-reports/*.xml", "**/target/failsafe-reports/*.xml"]
# Fully-qualified goal: runs against any project without touching its pom.
_VERSIONS_PLUGIN = "org.codehaus.mojo:versions-maven-plugin:2.18.0"
_BUILDISH = ("build", "test", "check")


def resolve_maven(project_dir: str) -> str | None:
    """mvnw wrapper in the project, else mvn on PATH.

    Wrapper projects commit both mvnw and mvnw.cmd, so the POSIX script must come
    first on non-Windows — otherwise the Windows .cmd is picked and fails to exec.
    """
    wrappers = ("mvnw.cmd", "mvnw.bat", "mvnw") if os.name == "nt" else ("mvnw", "mvnw.cmd", "mvnw.bat")
    for w in wrappers:
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
                mvn, "-v", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
            )
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

    if tool == "insight" and not args:
        return 'maven insight needs args=["<group:artifact>"] (dependency:tree -Dincludes filter).', None, ""
    goals = {
        "build": args or ["package"],
        "test": ["test"],
        "check": args or ["verify"],
        "deps": ["dependency:tree"],
        "sync": ["dependency:resolve"],
        "audit": ["dependency:tree"],  # tree, then OSV over the parsed rows
        "outdated": [f"{_VERSIONS_PLUGIN}:display-dependency-updates"],
        "insight": ["dependency:tree", f"-Dincludes={args[0]}"] if args else None,
        "projects": ["validate"],
    }.get(tool)
    if goals is None:
        return f"Unknown maven tool: {tool} (build|test|check|deps|sync|audit|outdated|insight|projects)", None, ""

    cmd = [mvn, "-B", "-ntp", *goals, *(extra_args or [])]
    start = time.monotonic()
    launched_at = time.time() - 2  # wall-clock; surefire reports older than this are from a prior run
    rc, text = await run_capture(cmd, cwd=project, timeout=timeout)
    raw_path = write_raw("devtools-mvn-", text)

    success, modules, failures = parse_maven_build(text)
    if tool in _BUILDISH and not modules:
        success = rc == 0 and "BUILD FAILURE" not in text
    deps = []
    vulns = []
    if tool in ("deps", "insight"):
        deps = parse_maven_tree(text)
    elif tool == "sync":
        deps = parse_maven_resolve(text)
    elif tool == "outdated":
        deps = parse_maven_outdated(text)
        success = rc == 0 or bool(deps)
    elif tool == "audit":
        deps = parse_maven_tree(text)
        vulns, osv_errors = await asyncio.to_thread(query_osv, deps)
        failures.extend(osv_errors)
        success = rc == 0 and not osv_errors
    if tool == "projects":
        modules = parse_maven_projects(text) or modules
    # Filter stale surefire reports only on failure (see gradle runner for why).
    tests = (
        parse_junit_dir(project, _JUNIT_DIRS, newer_than=None if success else launched_at)
        if tool in _BUILDISH
        else []
    )

    base = create_run_base(
        suite="maven", tool=tool, binary=project, args=goals, duration_seconds=time.monotonic() - start, exit_code=rc
    )
    result = BuildResult(
        **base.model_dump(),
        command=" ".join(["mvn", *goals]),
        success=success,
        dependencies=deps,
        tests=tests,
        vulnerabilities=vulns,
        modules=modules,
        failures=failures,
        raw_output=tail(text),
    )
    return None, result, raw_path
