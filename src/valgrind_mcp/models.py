"""Pydantic data models for all Valgrind tool output."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from pydantic import BaseModel, Field


class StackFrame(BaseModel):
    ip: str = ""
    obj: str | None = None
    fn: str | None = None
    dir: str | None = None
    file: str | None = None
    line: int | None = None

    @property
    def location(self) -> str:
        parts: list[str] = []
        if self.fn:
            parts.append(self.fn)
        if self.file and self.line is not None:
            parts.append(f"({self.file}:{self.line})")
        elif self.file:
            parts.append(f"({self.file})")
        elif self.obj:
            parts.append(f"(in {self.obj})")
        if not parts:
            return self.ip
        return " ".join(parts)


class ValgrindRun(BaseModel):
    run_id: str
    tool: str
    binary: str
    args: list[str] = Field(default_factory=list)
    valgrind_args: list[str] = Field(default_factory=list)
    timestamp: datetime
    exit_code: int
    duration_seconds: float


# --- Memcheck ---


class MemcheckError(BaseModel):
    unique_id: str
    kind: str
    what: str
    bytes_leaked: int | None = None
    blocks_leaked: int | None = None
    stack: list[StackFrame] = Field(default_factory=list)
    auxstack: list[StackFrame] | None = None
    auxwhat: str | None = None


class MemcheckResult(ValgrindRun):
    tool: str = "memcheck"
    errors: list[MemcheckError] = Field(default_factory=list)
    leak_summary: dict[str, int] = Field(default_factory=dict)
    error_summary: dict[str, int] = Field(default_factory=dict)


# --- Helgrind / DRD ---


class ThreadError(BaseModel):
    unique_id: str
    kind: str
    what: str
    stack: list[StackFrame] = Field(default_factory=list)
    auxstack: list[StackFrame] | None = None
    auxwhat: str | None = None
    thread_id: int | None = None


class ThreadCheckResult(ValgrindRun):
    errors: list[ThreadError] = Field(default_factory=list)
    error_summary: dict[str, int] = Field(default_factory=dict)


# --- Callgrind ---


class CallgrindCall(BaseModel):
    target: str
    target_file: str | None = None
    count: int = 0
    cost: dict[str, int] = Field(default_factory=dict)


class CallgrindFunction(BaseModel):
    name: str
    file: str | None = None
    object: str | None = None
    self_cost: dict[str, int] = Field(default_factory=dict)
    inclusive_cost: dict[str, int] = Field(default_factory=dict)
    callees: list[CallgrindCall] = Field(default_factory=list)


class CallgrindResult(ValgrindRun):
    tool: str = "callgrind"
    events: list[str] = Field(default_factory=list)
    functions: list[CallgrindFunction] = Field(default_factory=list)
    totals: dict[str, int] = Field(default_factory=dict)


# --- Cachegrind ---


class CachegrindLine(BaseModel):
    file: str
    function: str
    line: int
    ir: int = 0
    i1mr: int = 0
    ilmr: int = 0
    dr: int = 0
    d1mr: int = 0
    dlmr: int = 0
    dw: int = 0
    d1mw: int = 0
    dlmw: int = 0


class CachegrindResult(ValgrindRun):
    tool: str = "cachegrind"
    events: list[str] = Field(default_factory=list)
    summary: dict[str, int] = Field(default_factory=dict)
    lines: list[CachegrindLine] = Field(default_factory=list)


# --- Massif ---


class MassifAllocation(BaseModel):
    bytes: int = 0
    function: str | None = None
    file: str | None = None
    line: int | None = None
    children: list[MassifAllocation] = Field(default_factory=list)


class MassifSnapshot(BaseModel):
    index: int
    time: int
    heap_bytes: int = 0
    heap_extra_bytes: int = 0
    stacks_bytes: int = 0
    is_peak: bool = False
    is_detailed: bool = False
    heap_tree: MassifAllocation | None = None


class MassifResult(ValgrindRun):
    tool: str = "massif"
    snapshots: list[MassifSnapshot] = Field(default_factory=list)
    peak_snapshot_index: int = -1
    command: str = ""
    time_unit: str = "i"


# --- Runner result (internal) ---


class RunResult(BaseModel):
    exit_code: int
    stdout: str = ""
    stderr: str = ""
    output_path: str = ""
    duration_seconds: float = 0.0
    valgrind_args_used: list[str] = Field(default_factory=list)


# Union type for any parsed result
ValgrindResult = MemcheckResult | ThreadCheckResult | CallgrindResult | CachegrindResult | MassifResult


def create_run_base(
    tool: str,
    binary: str,
    args: list[str] | None = None,
    valgrind_args: list[str] | None = None,
    duration_seconds: float = 0.0,
    exit_code: int = 0,
) -> ValgrindRun:
    """Factory for creating a ValgrindRun base used by parsers."""
    return ValgrindRun(
        run_id=str(uuid.uuid4()),
        tool=tool,
        binary=binary,
        args=args or [],
        valgrind_args=valgrind_args or [],
        timestamp=datetime.now(UTC),
        exit_code=exit_code,
        duration_seconds=duration_seconds,
    )
