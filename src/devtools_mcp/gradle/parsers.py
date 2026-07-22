"""Parsers for Gradle console output: dependencies, build tasks, task listing,
dependencyUpdates JSON, dependencyInsight, and the projects listing."""

from __future__ import annotations

import json
import re

from devtools_mcp.build.models import BuildModule, BuildTask, Dependency

_DEP = re.compile(r"^(?P<prefix>[ |]*(?:\+---|\\---) )(?P<rest>.+)$")
_TASK = re.compile(r"^> Task (?P<name>:\S+)(?:\s+(?P<outcome>UP-TO-DATE|FAILED|SKIPPED|NO-SOURCE|FROM-CACHE))?\s*$")
_LISTED = re.compile(r"^(?P<name>[\w:.\-]+)(?:\s+-\s+(?P<desc>.+))?$")
_DASHES = re.compile(r"^-{3,}$")
_INSIGHT_HEAD = re.compile(
    r"^(?P<group>[\w.\-]+):(?P<artifact>[\w.\-]+):(?P<version>[\w.\-+]+)"
    r"(?:\s+->\s+(?P<resolved>[\w.\-+]+))?(?:\s+\((?P<reason>[^)]+)\))?\s*$"
)
_PROJECT = re.compile(r"^[ |]*(?:\+---|\\---) Project '(?P<name>[^']+)'(?:\s+-\s+(?P<desc>.+))?")
_ROOT_PROJECT = re.compile(r"^Root project '(?P<name>[^']+)'")
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
            if (
                s
                and s[0].isalpha()
                and not s.startswith(("Root project", "Project ", "BUILD", "Deprecated", "No dependencies"))
            ):
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
        deps.append(
            Dependency(
                group=group,
                artifact=artifact,
                version=version,
                requested=version,
                resolved=final,
                scope=scope,
                depth=depth,
                conflict=bool(resolved) and resolved != version,
                omitted=omitted,
            )
        )
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


def parse_gradle_outdated(json_text: str) -> list[Dependency]:
    """Parse a ben-manes dependencyUpdates report.json into outdated Dependencies.

    Mirrors the npm-outdated shape: version=current, resolved=latest, depth=1,
    conflict=True (an update exists by definition of the `outdated` bucket).
    """
    assert isinstance(json_text, str), "json_text must be str"
    try:
        data = json.loads(json_text)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(data, dict):
        return []
    out: list[Dependency] = []
    for entry in ((data.get("outdated") or {}).get("dependencies")) or []:
        if not isinstance(entry, dict):
            continue
        available = entry.get("available") or {}
        latest = str(available.get("release") or available.get("milestone") or available.get("integration") or "")
        current = str(entry.get("version", ""))
        out.append(
            Dependency(
                group=str(entry.get("group", "")),
                artifact=str(entry.get("name", "")),
                version=current,
                requested=current,
                resolved=latest,
                depth=1,
                conflict=bool(latest and latest != current),
            )
        )
    return out


def parse_gradle_insight(text: str) -> list[Dependency]:
    """Parse `gradle dependencyInsight` output.

    Header coordinate lines become depth-0 rows (scope = selection reason,
    conflict when a `-> resolved` arrow is present); the indented dependent
    chains reuse the tree shape, depth from the drawing prefix.
    """
    assert isinstance(text, str), "text must be str"
    deps: list[Dependency] = []
    for raw in text.splitlines()[:MAX_LINES]:
        m = _DEP.match(raw)
        if m:
            rest, omitted = _strip_markers(m.group("rest"))
            deps.append(
                Dependency(artifact=rest, depth=len(m.group("prefix")) // 5, omitted=omitted)
            )
            continue
        if raw.startswith((" ", ">", "-")):
            continue
        h = _INSIGHT_HEAD.match(raw.rstrip())
        if not h:
            continue
        version, resolved = h.group("version"), h.group("resolved") or ""
        deps.append(
            Dependency(
                group=h.group("group"),
                artifact=h.group("artifact"),
                version=version,
                requested=version,
                resolved=resolved or version,
                scope=h.group("reason") or "",
                depth=0,
                conflict=bool(resolved) and resolved != version,
            )
        )
    return deps


def parse_gradle_projects(text: str) -> list[BuildModule]:
    """Parse `gradle projects` into BuildModules (root + `Project ':x'` lines).

    The root name appears in both the banner and the tree — deduped by name.
    """
    assert isinstance(text, str), "text must be str"
    modules: list[BuildModule] = []
    seen: set[str] = set()
    for raw in text.splitlines()[:MAX_LINES]:
        m = _ROOT_PROJECT.match(raw) or _PROJECT.match(raw)
        if m and m.group("name") not in seen:
            seen.add(m.group("name"))
            modules.append(BuildModule(name=m.group("name")))
    return modules


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
