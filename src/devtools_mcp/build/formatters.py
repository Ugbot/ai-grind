"""Bounded markdown summary for a BuildResult.

Never dumps the full log: a status line, headline counts, and top-N slices of
whatever the run produced (failures, failed tests, conflicts, vulnerabilities,
tasks), then a pointer to query the frame.
"""

from __future__ import annotations

from devtools_mcp.build.models import BuildResult
from devtools_mcp.formatters.utils import format_run_header

_TOP = 15  # bound: top-N rows shown per section


def _failed_test_label(name: str, classname: str, message: str) -> str:
    """`Class::test — message`, trimming empty pieces."""
    label = f"{classname}::{name}" if classname else name
    return f"{label} — {message}" if message else label


def _section(parts: list[str], title: str, lines: list[str]) -> None:
    """Append a titled, top-N-bounded section if it has any content."""
    if not lines:
        return
    parts.append(f"\n**{title}:**")
    parts.extend(f"- {line}" for line in lines[:_TOP])
    if len(lines) > _TOP:
        parts.append(f"- … {len(lines) - _TOP} more")


def format_build_summary(result: BuildResult) -> str:
    """Bounded summary of a build/package-manager run."""
    assert isinstance(result, BuildResult), "result must be BuildResult"
    parts = [format_run_header(result), ""]
    status = "✅ success" if result.success else "❌ failure"
    parts.append(f"**Status:** {status}")
    if result.command:
        parts.append(f"**Command:** `{result.command}`")

    counts = []
    if result.dependencies:
        conflicts = sum(1 for d in result.dependencies if d.conflict)
        counts.append(f"{len(result.dependencies)} deps" + (f" ({conflicts} conflicts)" if conflicts else ""))
    if result.tests:
        failed = sum(1 for t in result.tests if t.status == "failed")
        counts.append(f"{len(result.tests)} tests ({failed} failed)")
    if result.vulnerabilities:
        counts.append(f"{len(result.vulnerabilities)} vulnerabilities")
    if result.modules:
        counts.append(f"{len(result.modules)} modules")
    if result.available_tasks:
        counts.append(f"{len(result.available_tasks)} tasks")
    if counts:
        parts.append("**Totals:** " + ", ".join(counts))

    _section(parts, "Build errors", result.failures)
    _section(
        parts,
        "Failed tests",
        [_failed_test_label(t.name, t.classname, t.message) for t in result.tests if t.status == "failed"],
    )
    _section(
        parts,
        "Dependency conflicts",
        [f"{d.artifact}: {d.requested or d.version} → {d.resolved}" for d in result.dependencies if d.conflict],
    )
    _section(
        parts,
        "Vulnerabilities",
        [f"[{v.severity}] {v.name} — {v.title}".rstrip(" —") for v in result.vulnerabilities],
    )
    _section(
        parts,
        "Failed modules",
        [f"{m.name}: {m.status}" for m in result.modules if m.status == "FAILURE"],
    )

    parts.append(f'\n_Query: devtools_analyze(run_id="{result.run_id}") or devtools_query(...)._')
    return "\n".join(parts)
