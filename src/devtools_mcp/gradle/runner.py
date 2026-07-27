"""Gradle execution: prefer the project's `gradlew` wrapper, else `gradle`."""

from __future__ import annotations

import asyncio
import itertools
import os
import pathlib
import shutil
import time

from devtools_mcp.build.exec import run_capture, tail, write_raw
from devtools_mcp.build.models import BuildResult, Dependency
from devtools_mcp.build.osv import query_osv
from devtools_mcp.build.parsers import parse_junit_dir
from devtools_mcp.gradle.parsers import (
    parse_gradle_build,
    parse_gradle_deps,
    parse_gradle_insight,
    parse_gradle_outdated,
    parse_gradle_projects,
    parse_gradle_tasks,
)
from devtools_mcp.models import create_run_base

_JUNIT_DIRS = ["**/build/test-results/**/*.xml"]
_PLAIN = "--console=plain"
_INIT_DIR = pathlib.Path(__file__).parent / "init"
_BUILDISH = ("build", "test", "check")  # verbs whose success comes from BUILD SUCCESSFUL + JUnit
_MAX_REPORTS = 100  # bound: dependencyUpdates report files read per run


def resolve_gradle(project_dir: str) -> str | None:
    """gradlew wrapper in the project, else gradle on PATH.

    Standard wrapper projects commit both gradlew and gradlew.bat, so the POSIX
    script must come first on non-Windows — otherwise the non-executable .bat is
    picked and the run fails.
    """
    wrappers = ("gradlew.bat", "gradlew") if os.name == "nt" else ("gradlew", "gradlew.bat")
    for w in wrappers:
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
    if tool == "check":
        return [*(args or ["check"]), _PLAIN, *extra]
    if tool == "tasks":
        return ["tasks", "--all", _PLAIN]
    # deps/sync/audit default to a devtools-registered all-projects report task:
    # plain `dependencies` at a multi-module root only shows the (empty) root
    # project, which would make the tree — and an audit over it — silently empty.
    all_deps = ["devtoolsAllDeps", "--init-script", str(_INIT_DIR / "alldeps.init.gradle")]
    if tool in ("deps", "audit"):  # audit = deps tree, then OSV over the parsed rows
        return [*(args or all_deps), _PLAIN, *extra]
    if tool == "sync":
        return [*(args or all_deps), "--refresh-dependencies", _PLAIN, *extra]
    if tool == "outdated":
        return ["dependencyUpdates", "--init-script", str(_INIT_DIR / "outdated.init.gradle"), _PLAIN, *extra]
    if tool == "insight":
        if not args:
            return []
        # args = [artifact, optional ":module"]; Gradle 9 requires an explicit
        # configuration, so default one unless the caller set it via extra_args.
        task = f"{args[1]}:dependencyInsight" if len(args) > 1 else "dependencyInsight"
        conf = [] if "--configuration" in extra else ["--configuration", "compileClasspath"]
        return [task, "--dependency", args[0], *conf, _PLAIN, *extra]
    if tool == "projects":
        return ["projects", _PLAIN]
    return []


def _read_outdated_reports(project: str, newer_than: float | None = None) -> list[Dependency]:
    """Merge every dependencyUpdates report.json under the project (multi-module).

    `newer_than` skips reports left by a previous run (see parse_junit_dir).
    """
    deps: list[Dependency] = []
    seen: set[tuple[str, str, str]] = set()
    reports = pathlib.Path(project).glob("**/build/dependencyUpdates/report.json")
    for report in itertools.islice(reports, _MAX_REPORTS):
        try:
            if newer_than is not None and report.stat().st_mtime < newer_than:
                continue
            rows = parse_gradle_outdated(report.read_text(encoding="utf-8", errors="replace"))
        except OSError:
            continue
        for d in rows:
            key = (d.group, d.artifact, d.version)
            if key not in seen:
                seen.add(key)
                deps.append(d)
    return deps


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
    if tool == "insight" and not args:
        return 'gradle insight needs args=["<artifact>"] (e.g. args=["guava"]).', None, ""
    argv = _argv(tool, args, extra_args)
    if not argv:
        verbs = "build|test|check|deps|sync|tasks|audit|outdated|insight|projects"
        return f"Unknown gradle tool: {tool} ({verbs})", None, ""

    start = time.monotonic()
    launched_at = time.time() - 2  # wall-clock; report files older than this are from a prior run
    rc, text = await run_capture([gradle, *argv], cwd=project, timeout=timeout)
    raw_path = write_raw("devtools-gradle-", text)

    success, exec_tasks, failures = parse_gradle_build(text)
    if tool not in _BUILDISH:
        success = rc == 0
    deps: list[Dependency] = []
    vulns = []
    modules = []
    if tool in ("deps", "sync"):
        deps = parse_gradle_deps(text)
    elif tool == "insight":
        deps = parse_gradle_insight(text)
    elif tool == "outdated":
        deps = _read_outdated_reports(project, newer_than=launched_at)
        success = rc == 0 or bool(deps)
    elif tool == "audit":
        deps = parse_gradle_deps(text)
        vulns, osv_errors = await asyncio.to_thread(query_osv, deps)
        failures.extend(osv_errors)
        success = rc == 0 and not osv_errors
    elif tool == "projects":
        modules = parse_gradle_projects(text)
    available = parse_gradle_tasks(text) if tool == "tasks" else []
    # Only distrust stale reports when the build failed — otherwise a successful
    # rerun with UP-TO-DATE tests (gradle doesn't rewrite the XML) would wrongly
    # show zero tests. A failed compile, though, must not surface last run's passes.
    tests = (
        parse_junit_dir(project, _JUNIT_DIRS, newer_than=None if success else launched_at) if tool in _BUILDISH else []
    )

    base = create_run_base(
        suite="gradle", tool=tool, binary=project, args=argv, duration_seconds=time.monotonic() - start, exit_code=rc
    )
    result = BuildResult(
        **base.model_dump(),
        command=" ".join(["gradle", *argv]),
        success=success,
        dependencies=deps,
        tests=tests,
        vulnerabilities=vulns,
        modules=modules,
        executed_tasks=exec_tasks if tool in _BUILDISH else [],
        available_tasks=available,
        failures=failures,
        raw_output=tail(text),
    )
    return None, result, raw_path
