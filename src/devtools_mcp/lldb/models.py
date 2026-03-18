"""Structured models for LLDB debug state snapshots."""

from __future__ import annotations

from pydantic import BaseModel, Field

from devtools_mcp.models import RunBase


class LldbStackFrame(BaseModel):
    """A single frame in a thread's call stack."""

    index: int = 0
    address: str = ""
    module: str | None = None
    function: str | None = None
    file: str | None = None
    line: int | None = None
    column: int | None = None

    @property
    def location(self) -> str:
        parts: list[str] = []
        if self.function:
            parts.append(self.function)
        if self.file and self.line is not None:
            parts.append(f"({self.file}:{self.line})")
        elif self.module:
            parts.append(f"(in {self.module})")
        if not parts:
            return self.address
        return " ".join(parts)


class LldbThread(BaseModel):
    """A thread in the debugged process."""

    thread_id: int
    index: int = 0
    name: str | None = None
    queue: str | None = None
    stop_reason: str | None = None
    frames: list[LldbStackFrame] = Field(default_factory=list)


class LldbVariable(BaseModel):
    """A variable in the current frame."""

    name: str
    type: str = ""
    value: str = ""
    summary: str | None = None
    children: list[LldbVariable] = Field(default_factory=list)


class LldbBreakpoint(BaseModel):
    """A breakpoint in the debug session."""

    id: int
    name: str | None = None
    file: str | None = None
    line: int | None = None
    address: str | None = None
    hit_count: int = 0
    enabled: bool = True
    condition: str | None = None
    resolved: bool = False


class LldbSnapshot(RunBase):
    """A point-in-time capture of LLDB debug state.

    Stored as a workspace run so it's queryable via devtools_analyze/devtools_search.
    """

    suite: str = "lldb"
    session_id: str = ""
    snapshot_type: str = ""  # "backtrace", "variables", "threads", "breakpoints", etc.
    threads: list[LldbThread] = Field(default_factory=list)
    variables: list[LldbVariable] = Field(default_factory=list)
    breakpoints: list[LldbBreakpoint] = Field(default_factory=list)
    registers: dict[str, str] = Field(default_factory=dict)
    raw_output: str = ""
