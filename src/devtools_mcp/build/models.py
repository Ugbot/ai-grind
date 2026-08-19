"""Normalized models for the build/package-manager backends.

Maven, Gradle, npm, pnpm, yarn and Cargo all map their output onto these shapes,
so one query/analysis surface serves every build tool.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from devtools_mcp.models import RunBase


class Dependency(BaseModel):
    """One dependency-graph node, normalized across all build tools."""

    artifact: str
    group: str = ""
    version: str = ""
    requested: str = ""
    resolved: str = ""
    scope: str = ""
    depth: int = 0
    conflict: bool = False
    omitted: bool = False


class TestCase(BaseModel):
    """One test result (JUnit XML, libtest, ...). status: passed|failed|skipped."""

    name: str
    classname: str = ""
    status: str = ""
    message: str = ""
    time: float = 0.0


class Vulnerability(BaseModel):
    """One advisory from an audit (npm/yarn/cargo)."""

    name: str
    severity: str = ""  # critical | high | moderate | low | unknown
    version: str = ""
    vulnerable_range: str = ""
    title: str = ""
    url: str = ""
    fix_available: bool = False


class BuildModule(BaseModel):
    """One reactor module (Maven multi-module build)."""

    name: str
    status: str = ""  # SUCCESS | FAILURE | SKIPPED
    duration: float = 0.0


class BuildTask(BaseModel):
    """One build task, executed (`> Task`) or available (`gradle tasks`)."""

    name: str
    outcome: str = ""  # EXECUTED | UP-TO-DATE | FAILED | NO-SOURCE | ...
    group: str = ""
    description: str = ""


class BuildResult(RunBase):
    """Normalized result of any build/package-manager run.

    Inherits run_id/suite/tool/binary/args/timestamp/exit_code/duration_seconds
    from RunBase; `suite` gets a default so lightweight construction in tests and
    ad-hoc frames works without naming a concrete build tool.
    """

    suite: str = "build"
    command: str = ""
    success: bool = False
    dependencies: list[Dependency] = Field(default_factory=list)
    tests: list[TestCase] = Field(default_factory=list)
    vulnerabilities: list[Vulnerability] = Field(default_factory=list)
    modules: list[BuildModule] = Field(default_factory=list)
    executed_tasks: list[BuildTask] = Field(default_factory=list)
    available_tasks: list[BuildTask] = Field(default_factory=list)
    failures: list[str] = Field(default_factory=list)
    raw_output: str = ""
