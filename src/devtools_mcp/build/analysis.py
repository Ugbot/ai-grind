"""Polars frames over a BuildResult — the queryable surface for build runs.

Every frame carries a `function` column (the row's primary name) and a `kind`
column (its primary category) so the generic devtools_analyze/query/compare/
correlate tools work uniformly across build tools and the profilers.
"""

from __future__ import annotations

import polars as pl

from devtools_mcp.build.models import BuildResult

MAX_ROWS = 1_000_000  # bound: frames are summaries, not unbounded dumps

# Severity ranking so audits sort worst-first regardless of tool vocabulary.
_SEVERITY_RANK = {
    "critical": 4,
    "high": 3,
    "moderate": 2,
    "medium": 2,
    "low": 1,
    "info": 0,
    "unknown": 0,
    "": 0,
}


def deps_df(result: BuildResult) -> pl.DataFrame:
    """One row per dependency: coordinates, depth, conflict/omitted flags."""
    assert isinstance(result, BuildResult), "result must be BuildResult"
    rows = result.dependencies
    assert len(rows) <= MAX_ROWS, f"too many dependencies: {len(rows)}"
    return pl.DataFrame(
        {
            "function": [d.artifact for d in rows],
            "artifact": [d.artifact for d in rows],
            "group": [d.group for d in rows],
            "version": [d.version for d in rows],
            "requested": [d.requested for d in rows],
            "resolved": [d.resolved for d in rows],
            "scope": [d.scope for d in rows],
            "kind": [d.scope or "dep" for d in rows],
            "depth": [d.depth for d in rows],
            "conflict": [d.conflict for d in rows],
            "omitted": [d.omitted for d in rows],
        },
        schema_overrides={"depth": pl.Int64, "conflict": pl.Boolean, "omitted": pl.Boolean},
    )


def tests_df(result: BuildResult) -> pl.DataFrame:
    """One row per test case; `kind` is the pass/fail/skip status."""
    assert isinstance(result, BuildResult), "result must be BuildResult"
    rows = result.tests
    assert len(rows) <= MAX_ROWS, f"too many test cases: {len(rows)}"
    return pl.DataFrame(
        {
            "function": [t.name for t in rows],
            "name": [t.name for t in rows],
            "classname": [t.classname for t in rows],
            "kind": [t.status for t in rows],
            "status": [t.status for t in rows],
            "message": [t.message for t in rows],
            "time": [t.time for t in rows],
        },
        schema_overrides={"time": pl.Float64},
    )


def vulns_df(result: BuildResult) -> pl.DataFrame:
    """One row per advisory, sorted worst-severity first; `kind` is the severity."""
    assert isinstance(result, BuildResult), "result must be BuildResult"
    rows = result.vulnerabilities
    assert len(rows) <= MAX_ROWS, f"too many vulnerabilities: {len(rows)}"
    df = pl.DataFrame(
        {
            "function": [v.name for v in rows],
            "name": [v.name for v in rows],
            "kind": [v.severity for v in rows],
            "severity": [v.severity for v in rows],
            "_rank": [_SEVERITY_RANK.get(v.severity.lower(), 0) for v in rows],
            "version": [v.version for v in rows],
            "range": [v.vulnerable_range for v in rows],
            "title": [v.title for v in rows],
            "url": [v.url for v in rows],
            "fix_available": [v.fix_available for v in rows],
        },
        schema_overrides={"_rank": pl.Int64, "fix_available": pl.Boolean},
    )
    return df.sort("_rank", descending=True).drop("_rank")


def _tasks_df(rows: list) -> pl.DataFrame:
    """Shared frame for executed/available build tasks; `kind` is outcome or group."""
    assert len(rows) <= MAX_ROWS, f"too many tasks: {len(rows)}"
    return pl.DataFrame(
        {
            "function": [t.name for t in rows],
            "name": [t.name for t in rows],
            "kind": [t.outcome or t.group or "task" for t in rows],
            "outcome": [t.outcome for t in rows],
            "group": [t.group for t in rows],
            "description": [t.description for t in rows],
        }
    )


def executed_tasks_df(result: BuildResult) -> pl.DataFrame:
    """One row per executed task (Gradle `> Task` lines)."""
    assert isinstance(result, BuildResult), "result must be BuildResult"
    return _tasks_df(result.executed_tasks)


def available_tasks_df(result: BuildResult) -> pl.DataFrame:
    """One row per available task/script (`gradle tasks`, package.json scripts)."""
    assert isinstance(result, BuildResult), "result must be BuildResult"
    return _tasks_df(result.available_tasks)


def modules_df(result: BuildResult) -> pl.DataFrame:
    """One row per reactor module (Maven multi-module build); `kind` is status."""
    assert isinstance(result, BuildResult), "result must be BuildResult"
    rows = result.modules
    assert len(rows) <= MAX_ROWS, f"too many modules: {len(rows)}"
    return pl.DataFrame(
        {
            "function": [m.name for m in rows],
            "name": [m.name for m in rows],
            "kind": [m.status for m in rows],
            "status": [m.status for m in rows],
            "duration": [m.duration for m in rows],
        },
        schema_overrides={"duration": pl.Float64},
    )
