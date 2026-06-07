"""Parsers for Gradle console output: dependencies, build tasks, task listing."""

from __future__ import annotations

import re

from devtools_mcp.build.models import BuildTask, Dependency

_DEP = re.compile(r"^(?P<prefix>[ |]*(?:\+---|\\---) )(?P<rest>.+)$")
_TASK = re.compile(r"^> Task (?P<name>:\S+)(?:\s+(?P<outcome>UP-TO-DATE|FAILED|SKIPPED|NO-SOURCE|FROM-CACHE))?\s*$")
_LISTED = re.compile(r"^(?P<name>[\w:.\-]+)(?:\s+-\s+(?P<desc>.+))?$")
_DASHES = re.compile(r"^-{3,}$")
MAX_LINES = 500_000


def _strip_markers(rest: str) -> tuple[str, bool]:
    omitted = "(*)" in rest
    for marker in (" (*)", " (c)", " (n)"):
        rest = rest.replace(marker, "")
    return rest.strip(), omitted


def parse_gradle_deps(text: str) -> list[Dependency]:
    """Parse `gradle dependencies` tree output into Dependency nodes."""
    assert isinstance(text, str), "text must be str"
    deps: list[Dependency] = []
    scope = ""
    for raw in text.splitlines()[:MAX_LINES]:
        m = _DEP.match(raw)
        if not m:
            s = raw.rstrip()
            if s and s[0].isalpha() and not s.startswith((
                "Root project", "Project ", "BUILD", "Deprecated", "No dependencies")):
                scope = s.split(" ")[0]
            continue
        depth = len(m.group("prefix")) // 5
        rest, omitted = _strip_markers(m.group("rest"))
        left, _, resolved = rest.partition(" -> ")
        left, resolved = left.strip(), resolved.strip()
        if left.startswith("project "):
            deps.append(Dependency(artifact=left, scope=scope, depth=depth, omitted=omitted))
            continue
        parts = left.split(":")
        group = parts[0] if len(parts) >= 2 else ""
        artifact = parts[1] if len(parts) >= 2 else parts[0]
        version = parts[2] if len(parts) >= 3 else ""
        final = resolved or version
        deps.append(Dependency(
            group=group, artifact=artifact, version=version, requested=version,
            resolved=final, scope=scope, depth=depth,
            conflict=bool(resolved) and resolved != version, omitted=omitted,
        ))
    return deps


def parse_gradle_build(text: str) -> tuple[bool, list[BuildTask], list[str]]:
    """Parse `> Task` lines, BUILD SUCCESSFUL/FAILED, and '* What went wrong'."""
    assert isinstance(text, str), "text must be str"
    success = "BUILD SUCCESSFUL" in text
    tasks: list[BuildTask] = []
    failures: list[str] = []
    lines = text.splitlines()[:MAX_LINES]
    capture = 0
    for line in lines:
        m = _TASK.match(line)
        if m:
            tasks.append(BuildTask(name=m.group("name"), outcome=m.group("outcome") or "EXECUTED"))
            continue
        if line.startswith("* What went wrong:"):
            capture = 15
            continue
        if capture > 0:
            if line.startswith("* ") or not line.strip():
                capture = 0
            else:
                failures.append(line.strip())
                capture -= 1
    return success, tasks, failures


def parse_gradle_tasks(text: str) -> list[BuildTask]:
    """Parse `gradle tasks --all` into available BuildTasks (name/group/desc)."""
    assert isinstance(text, str), "text must be str"
    lines = text.splitlines()[:MAX_LINES]
    tasks: list[BuildTask] = []
    group = ""
    for i, line in enumerate(lines):
        if i + 1 < len(lines) and _DASHES.match(lines[i + 1].strip()) and line.strip():
            group = line.strip()
            continue
        if _DASHES.match(line.strip()) or not line.strip() or not group:
            continue
        m = _LISTED.match(line.strip())
        if m and not line.startswith(("To see", "BUILD", "Deprecated", "(*)")):
            tasks.append(BuildTask(name=m.group("name"), group=group, description=(m.group("desc") or "").strip()))
    return tasks
