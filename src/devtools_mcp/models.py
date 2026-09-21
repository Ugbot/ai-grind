"""Shared base models for all tool suites."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from pydantic import BaseModel, Field


class RunBase(BaseModel):
    """Base model for all tool results, batch runs and debug snapshots alike."""

    run_id: str
    suite: str  # "valgrind", "lldb", "dtrace", "perf"
    tool: str  # "memcheck", "backtrace", "trace", "stat"
    binary: str
    args: list[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    exit_code: int = 0
    duration_seconds: float = 0.0
    # Enrichment (persisted in meta.json / optional on RunBase)
    label: str = ""
    notes: str = ""
    tags: list[str] = Field(default_factory=list)
    task_key: str = ""
    parent_run_id: str = ""
    batch_id: str = ""
    workspace_id: str = ""
    workspace_name: str = ""
    git_commit: str = ""
    git_branch: str = ""
    git_dirty: bool = False
    cwd: str = ""
    hostname: str = ""
    tool_version: str = ""
    tool_path: str = ""
    stored_summary: str = ""


def create_run_base(
    suite: str,
    tool: str,
    binary: str,
    args: list[str] | None = None,
    duration_seconds: float = 0.0,
    exit_code: int = 0,
) -> RunBase:
    """Factory for creating a RunBase used by parsers."""
    return RunBase(
        run_id=str(uuid.uuid4()),
        suite=suite,
        tool=tool,
        binary=binary,
        args=args or [],
        exit_code=exit_code,
        duration_seconds=duration_seconds,
    )


class StackSample(BaseModel):
    """One aggregated call stack with a weight (sample count, bytes, etc.).

    The universal flame-graph input: any sampling backend (perf, dtrace profile,
    ETW, JFR, async-profiler, CDB thread stacks) maps its stacks onto this. Frames
    are ordered root-first (outermost caller at index 0, leaf last), the same
    order Brendan-Gregg folded stacks use.
    """

    frames: list[str] = Field(default_factory=list)
    weight: int = 1


# ToolResult is the union of all concrete result types.
# Each backend defines its own result types inheriting from RunBase.
# This gets populated by the registry after all backends are imported.
# For now, it's defined as RunBase, backends extend it.
