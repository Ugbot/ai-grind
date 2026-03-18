"""Shared base models for all tool suites."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from pydantic import BaseModel, Field


class RunBase(BaseModel):
    """Base model for all tool results — batch runs and debug snapshots alike."""

    run_id: str
    suite: str  # "valgrind", "lldb", "dtrace", "perf"
    tool: str  # "memcheck", "backtrace", "trace", "stat"
    binary: str
    args: list[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    exit_code: int = 0
    duration_seconds: float = 0.0


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


# ToolResult is the union of all concrete result types.
# Each backend defines its own result types inheriting from RunBase.
# This gets populated by the registry after all backends are imported.
# For now, it's defined as RunBase — backends extend it.
