"""Parsers for Cargo output: cargo tree, cargo test (libtest), build diagnostics."""

from __future__ import annotations

import json
import re

from devtools_mcp.build.models import Dependency, TestCase, Vulnerability

_TREE = re.compile(r"^(?P<prefix>[│├└─ ]*)(?P<name>[\w\-]+) v(?P<ver>[\w.\-+]+)")
_TEST = re.compile(r"^test (?P<name>\S+) \.\.\. (?P<res>ok|FAILED|ignored)")
_ERROR = re.compile(r"^error(?:\[[^\]]+\])?: (?P<msg>.+)$")
MAX_LINES = 500_000
_STATUS = {"ok": "passed", "FAILED": "failed", "ignored": "skipped"}


def parse_cargo_tree(text: str) -> list[Dependency]:
    """Parse `cargo tree` (box-drawing) into Dependency nodes; depth by indent."""
    assert isinstance(text, str), "text must be str"
    deps: list[Dependency] = []
    for raw in text.splitlines()[:MAX_LINES]:
        m = _TREE.match(raw)
        if not m:
            continue
        depth = len(m.group("prefix")) // 4
        deps.append(Dependency(
            artifact=m.group("name"), version=m.group("ver"), resolved=m.group("ver"),
            depth=depth, omitted="(*)" in raw,
        ))
    return deps


def parse_cargo_test(text: str) -> tuple[list[TestCase], bool]:
    """Parse libtest output (`test path ... ok|FAILED|ignored`) + overall result."""
    assert isinstance(text, str), "text must be str"
    cases: list[TestCase] = []
    for raw in text.splitlines()[:MAX_LINES]:
        m = _TEST.match(raw)
        if m:
            cases.append(TestCase(name=m.group("name"), classname="", status=_STATUS[m.group("res")]))
    success = "test result: FAILED" not in text and "error: test failed" not in text
    return cases, success


def parse_cargo_build(text: str) -> tuple[bool, list[str]]:
    """Extract error lines + success (no `error:`/`could not compile`)."""
    assert isinstance(text, str), "text must be str"
    failures: list[str] = []
    seen: set[str] = set()
    for raw in text.splitlines()[:MAX_LINES]:
        m = _ERROR.match(raw.strip())
        if m and m.group("msg") not in seen:
            seen.add(m.group("msg"))
            failures.append(m.group("msg"))
    success = "could not compile" not in text and not failures
    return success, failures


def parse_cargo_audit(text: str) -> list[Vulnerability]:
    """Parse `cargo audit --json` (RustSec) into Vulnerabilities."""
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []
    vulns: list[Vulnerability] = []
    for entry in (data.get("vulnerabilities", {}) or {}).get("list", []) or []:
        adv = entry.get("advisory", {}) or {}
        pkg = entry.get("package", {}) or {}
        patched = (entry.get("versions", {}) or {}).get("patched", [])
        vulns.append(Vulnerability(
            name=pkg.get("name", ""), severity=(adv.get("severity") or "unknown"),
            version=pkg.get("version", ""), vulnerable_range=adv.get("id", ""),
            title=adv.get("title", ""), url=adv.get("url", ""), fix_available=bool(patched),
        ))
    return vulns
