"""Shared JUnit XML parsing (Maven surefire/failsafe, Gradle test-results).

Both Maven and Gradle emit JUnit XML, so the report parsing lives here and each
runner just points it at the right glob.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

from devtools_mcp.build.models import TestCase

MAX_FILES = 5_000  # bound: never walk an unbounded report tree
MAX_CASES = 200_000  # bound: never materialize an unbounded case list


def _case_status(case: ET.Element) -> tuple[str, str]:
    """(status, message) for one <testcase>, from its failure/error/skipped child."""
    failure = case.find("failure")
    if failure is not None:
        return "failed", failure.get("message", "")
    error = case.find("error")
    if error is not None:
        return "failed", error.get("message", "")
    if case.find("skipped") is not None:
        return "skipped", ""
    return "passed", ""


def parse_junit_text(data: bytes | str) -> list[TestCase]:
    """Parse one JUnit XML document into TestCases; [] on malformed XML."""
    assert isinstance(data, (bytes, str)), "data must be bytes or str"
    raw = data if isinstance(data, bytes) else data.encode("utf-8")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return []
    suites = [root] if root.tag == "testsuite" else root.findall(".//testsuite")
    cases: list[TestCase] = []
    for suite in suites:
        for case in suite.findall("testcase"):
            if len(cases) >= MAX_CASES:
                return cases
            status, message = _case_status(case)
            try:
                seconds = float(case.get("time", "0") or 0)
            except ValueError:
                seconds = 0.0
            cases.append(
                TestCase(
                    name=case.get("name", ""),
                    classname=case.get("classname", ""),
                    status=status,
                    message=message,
                    time=seconds,
                )
            )
    return cases


def parse_junit_dir(project: str, patterns: list[str]) -> list[TestCase]:
    """Glob `patterns` under `project`, parse every JUnit file, aggregate cases."""
    assert isinstance(project, str) and project, "project required"
    assert patterns, "patterns required"
    base = Path(project)
    if not base.is_dir():
        return []
    cases: list[TestCase] = []
    seen = 0
    for pattern in patterns:
        for path in sorted(base.glob(pattern)):
            if seen >= MAX_FILES:
                return cases
            seen += 1
            try:
                cases.extend(parse_junit_text(path.read_bytes()))
            except OSError:
                continue
    return cases
