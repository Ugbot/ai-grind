"""Parsers for Maven console output: dependency:tree, reactor summary, resolve,
versions:display-dependency-updates, and the reactor build order."""

from __future__ import annotations

import re

from devtools_mcp.build.models import BuildModule, Dependency

_INFO = re.compile(r"^\[INFO\] ?")
_OUTDATED = re.compile(
    r"^\s*(?P<group>[\w.\-]+):(?P<artifact>[\w.\-]+)\s+\.{2,}\s+(?P<current>\S+)\s+->\s+(?P<latest>\S+)\s*$"
)
_ORDER_ENTRY = re.compile(r"^(?P<name>\S.*?)\s+\[(?P<packaging>[\w\-]+)\]\s*$")
# tree prefix (groups of 3 chars: "+- ", "\- ", "|  ", "   ") then a coordinate
_TREE = re.compile(
    r"^(?P<prefix>(?:[ |]{3}|[+\\]- )*)" r"(?P<coord>[\w.\-]+(?::[\w.\-+]+){3,})" r"(?:\s+\((?P<note>[^)]*)\))?$"
)
_MODULE = re.compile(r"^(?P<name>.+?)\s+\.{3,}\s+(?P<status>SUCCESS|FAILURE|SKIPPED)(?:\s+\[\s*(?P<dur>[\d.]+)\s*s\])?")
_RESOLVE = re.compile(r"^\s+(?P<coord>[\w.\-]+:[\w.\-]+:[\w.\-]+:[\w.\-]+(?::[\w.\-]+)?)\s*$")
_WITH = re.compile(r"with ([\w.\-]+)")
MAX_LINES = 500_000


def _split_coord(coord: str) -> tuple[str, str, str, str]:
    """group:artifact:packaging[:classifier]:version[:scope] -> (g, a, ver, scope)."""
    p = coord.split(":")
    if len(p) == 4:  # g:a:pkg:ver (root, no scope)
        return p[0], p[1], p[3], ""
    if len(p) == 5:  # g:a:pkg:ver:scope
        return p[0], p[1], p[3], p[4]
    if len(p) >= 6:  # g:a:pkg:classifier:ver:scope
        return p[0], p[1], p[4], p[5]
    return (p + ["", "", "", ""])[0], (p + ["", "", "", ""])[1], "", ""


def parse_maven_tree(text: str) -> list[Dependency]:
    """Parse `mvn dependency:tree` output into Dependency nodes."""
    assert isinstance(text, str), "text must be str"
    deps: list[Dependency] = []
    for raw in text.splitlines()[:MAX_LINES]:
        line = _INFO.sub("", raw).rstrip()
        if not line or ":" not in line:
            continue
        m = _TREE.match(line)
        if not m:
            continue
        coord = m.group("coord").strip()
        if coord.count(":") < 3:
            continue
        depth = len(m.group("prefix")) // 3
        group, artifact, version, scope = _split_coord(coord)
        note = m.group("note") or ""
        conflict = "conflict" in note or "managed" in note
        omitted = "omitted" in note
        resolved = version
        w = _WITH.search(note)
        if conflict and w:
            resolved = w.group(1)
        deps.append(
            Dependency(
                group=group,
                artifact=artifact,
                version=version,
                requested=version,
                resolved=resolved,
                scope=scope,
                depth=depth,
                conflict=conflict,
                omitted=omitted,
            )
        )
    return deps


def parse_maven_resolve(text: str) -> list[Dependency]:
    """Parse `mvn dependency:resolve` flat artifact list into direct deps."""
    deps: list[Dependency] = []
    for raw in text.splitlines()[:MAX_LINES]:
        line = _INFO.sub("", raw)
        m = _RESOLVE.match(line)
        if not m:
            continue
        group, artifact, version, scope = _split_coord(m.group("coord"))
        deps.append(Dependency(group=group, artifact=artifact, version=version, resolved=version, scope=scope, depth=1))
    return deps


def parse_maven_outdated(text: str) -> list[Dependency]:
    """Parse `versions:display-dependency-updates` console lines.

    `group:artifact ..... current -> latest` rows, mirroring the npm-outdated
    shape: version=current, resolved=latest, depth=1, conflict=True.
    """
    assert isinstance(text, str), "text must be str"
    deps: list[Dependency] = []
    seen: set[tuple[str, str, str]] = set()
    for raw in text.splitlines()[:MAX_LINES]:
        m = _OUTDATED.match(_INFO.sub("", raw))
        if not m:
            continue
        key = (m.group("group"), m.group("artifact"), m.group("current"))
        if key in seen:
            continue
        seen.add(key)
        deps.append(
            Dependency(
                group=m.group("group"),
                artifact=m.group("artifact"),
                version=m.group("current"),
                requested=m.group("current"),
                resolved=m.group("latest"),
                depth=1,
                conflict=m.group("latest") != m.group("current"),
            )
        )
    return deps


def parse_maven_projects(text: str) -> list[BuildModule]:
    """Parse the `Reactor Build Order:` block into BuildModules (multi-module)."""
    assert isinstance(text, str), "text must be str"
    modules: list[BuildModule] = []
    in_order = False
    for raw in text.splitlines()[:MAX_LINES]:
        line = _INFO.sub("", raw).rstrip()
        if line.startswith("Reactor Build Order:"):
            in_order = True
            continue
        if not in_order:
            continue
        if not line:
            if modules:  # blank line after entries ends the block; before them, skip
                break
            continue
        m = _ORDER_ENTRY.match(line)
        if m:
            modules.append(BuildModule(name=m.group("name")))
        elif modules:
            break
    return modules


def parse_maven_build(text: str) -> tuple[bool, list[BuildModule], list[str]]:
    """Parse reactor summary + build status + [ERROR] lines."""
    assert isinstance(text, str), "text must be str"
    success = "BUILD SUCCESS" in text and "BUILD FAILURE" not in text
    modules: list[BuildModule] = []
    failures: list[str] = []
    seen: set[str] = set()
    for raw in text.splitlines()[:MAX_LINES]:
        line = _INFO.sub("", raw).rstrip()
        m = _MODULE.match(line)
        if m and not m.group("name").startswith(("BUILD", "Total", "Finished")):
            modules.append(
                BuildModule(name=m.group("name").strip(), status=m.group("status"), duration=float(m.group("dur") or 0))
            )
        elif raw.startswith("[ERROR]"):
            msg = raw[len("[ERROR]") :].strip()
            if msg and not msg.startswith(("[Help", "To see", "Re-run", "For more")) and msg not in seen:
                seen.add(msg)
                failures.append(msg)
    return success, modules, failures
