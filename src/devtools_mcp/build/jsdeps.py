"""Parsers for JS package-manager JSON output (npm / pnpm / yarn).

`npm ls --json`, `pnpm list --json`, `yarn list --json` (NDJSON), the audit
formats (npm v6 advisories + v7 vulnerabilities, yarn NDJSON), `npm outdated
--json`, and package.json scripts — all normalized onto the shared models.
"""

from __future__ import annotations

import json
from pathlib import Path

from devtools_mcp.build.models import Dependency, Vulnerability

MAX_DEPS = 100_000  # bound: never materialize an unbounded dependency set
MAX_DEPTH = 64  # bound: guard against cyclic/pathological trees


def _walk_npm(node: dict, depth: int, scope: str, out: list[Dependency]) -> None:
    """Recurse an npm/pnpm `dependencies` map, one Dependency per package."""
    if depth > MAX_DEPTH or len(out) >= MAX_DEPS:
        return
    for name, info in node.items():
        if not isinstance(info, dict):
            continue
        version = str(info.get("version", ""))
        out.append(
            Dependency(
                artifact=name,
                version=version,
                resolved=version,
                scope=scope,
                depth=depth,
                conflict=bool(info.get("invalid")),
            )
        )
        _walk_npm(info.get("dependencies", {}) or {}, depth + 1, scope, out)


def parse_npm_ls(text: str) -> list[Dependency]:
    """Parse `npm ls --all --json` into a flat depth-tagged dependency list."""
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(data, dict):
        return []
    out: list[Dependency] = []
    _walk_npm(data.get("dependencies", {}) or {}, 1, "", out)
    return out


def parse_pnpm_list(text: str) -> list[Dependency]:
    """Parse `pnpm list --json` (array of projects) with prod/dev scopes."""
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return []
    projects = data if isinstance(data, list) else [data]
    out: list[Dependency] = []
    for project in projects:
        if not isinstance(project, dict):
            continue
        _walk_npm(project.get("dependencies", {}) or {}, 1, "prod", out)
        _walk_npm(project.get("devDependencies", {}) or {}, 1, "dev", out)
    return out


def _split_name_version(spec: str) -> tuple[str, str]:
    """ "express@4.18.0" -> (express, 4.18.0); "@scope/pkg@2.0.0" -> (@scope/pkg, 2.0.0)."""
    at = spec.rfind("@")
    if at <= 0:  # no version, or a bare leading-@ scope with no version
        return spec, ""
    return spec[:at], spec[at + 1 :]


def _walk_yarn(node: dict, depth: int, out: list[Dependency]) -> None:
    """Recurse a `yarn list --json` tree node."""
    if depth > MAX_DEPTH or len(out) >= MAX_DEPS:
        return
    name, version = _split_name_version(str(node.get("name", "")))
    out.append(Dependency(artifact=name, version=version, resolved=version, depth=depth))
    for child in node.get("children") or []:
        if isinstance(child, dict):
            _walk_yarn(child, depth + 1, out)


def parse_yarn_list(text: str) -> list[Dependency]:
    """Parse `yarn list --json` NDJSON; the `tree` record carries the graph."""
    out: list[Dependency] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict) or obj.get("type") != "tree":
            continue
        for tree in ((obj.get("data") or {}).get("trees")) or []:
            if isinstance(tree, dict):
                _walk_yarn(tree, 1, out)
    return out


def parse_npm_audit(text: str) -> list[Vulnerability]:
    """Parse `npm audit --json` — both v7 `vulnerabilities` and v6 `advisories`."""
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(data, dict):
        return []
    vulns: list[Vulnerability] = []
    v7 = data.get("vulnerabilities")
    if isinstance(v7, dict):
        for name, info in v7.items():
            if not isinstance(info, dict):
                continue
            title, url = "", ""
            for via in info.get("via") or []:
                if isinstance(via, dict):
                    title, url = via.get("title", ""), via.get("url", "")
                    break
            vulns.append(
                Vulnerability(
                    name=str(info.get("name", name)),
                    severity=str(info.get("severity", "unknown")),
                    vulnerable_range=str(info.get("range", "")),
                    title=title,
                    url=url,
                    fix_available=bool(info.get("fixAvailable")),
                )
            )
    advisories = data.get("advisories")
    if isinstance(advisories, dict):
        for adv in advisories.values():
            if not isinstance(adv, dict):
                continue
            vulns.append(
                Vulnerability(
                    name=str(adv.get("module_name", "")),
                    severity=str(adv.get("severity", "unknown")),
                    vulnerable_range=str(adv.get("vulnerable_versions", "")),
                    title=str(adv.get("title", "")),
                    url=str(adv.get("url", "")),
                    fix_available=bool(adv.get("patched_versions")),
                )
            )
    return vulns


def parse_yarn_audit(text: str) -> list[Vulnerability]:
    """Parse `yarn audit --json` NDJSON (`auditAdvisory` records)."""
    vulns: list[Vulnerability] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict) or obj.get("type") != "auditAdvisory":
            continue
        adv = (obj.get("data") or {}).get("advisory") or {}
        vulns.append(
            Vulnerability(
                name=str(adv.get("module_name", "")),
                severity=str(adv.get("severity", "unknown")),
                vulnerable_range=str(adv.get("vulnerable_versions", "")),
                title=str(adv.get("title", "")),
                url=str(adv.get("url", "")),
                fix_available=bool(adv.get("patched_versions")),
            )
        )
    return vulns


def parse_npm_outdated(text: str) -> list[Dependency]:
    """Parse `npm outdated --json`: current vs latest, conflict when they differ."""
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(data, dict):
        return []
    out: list[Dependency] = []
    for name, info in data.items():
        if not isinstance(info, dict):
            continue
        current = str(info.get("current", ""))
        latest = str(info.get("latest", "") or info.get("wanted", ""))
        out.append(
            Dependency(
                artifact=name,
                version=current,
                requested=str(info.get("wanted", "")),
                resolved=latest,
                depth=1,
                conflict=bool(latest and latest != current),
            )
        )
    return out


def parse_yarn_outdated(text: str) -> list[Dependency]:
    """Parse `yarn outdated --json` NDJSON (classic); the `table` record has the rows.

    Columns come from the `head` record (Package/Current/Wanted/Latest/...), so
    column order changes don't break the mapping.
    """
    out: list[Dependency] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict) or obj.get("type") != "table":
            continue
        data = obj.get("data") or {}
        head = [str(h).lower() for h in data.get("head") or []]
        for row in data.get("body") or []:
            cells = {head[i]: str(v) for i, v in enumerate(row) if i < len(head)}
            current = cells.get("current", "")
            latest = cells.get("latest", "")
            out.append(
                Dependency(
                    artifact=cells.get("package", ""),
                    version=current,
                    requested=cells.get("wanted", ""),
                    resolved=latest,
                    depth=1,
                    conflict=bool(latest and latest != current),
                )
            )
    return out


def parse_package_scripts(project: str) -> dict[str, str]:
    """Read package.json `scripts` (name -> command); {} if absent/unreadable."""
    assert isinstance(project, str), "project must be str"
    path = Path(project) / "package.json"
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    scripts = data.get("scripts", {}) if isinstance(data, dict) else {}
    if not isinstance(scripts, dict):
        return {}
    return {str(k): str(v) for k, v in list(scripts.items())[:MAX_DEPS]}
