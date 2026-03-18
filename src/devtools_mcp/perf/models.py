"""perf result models."""

from __future__ import annotations

from pydantic import BaseModel, Field

from devtools_mcp.models import RunBase


class PerfCounter(BaseModel):
    """A hardware counter measurement from perf stat."""

    event: str
    value: float = 0.0
    unit: str = ""
    variance_pct: float | None = None
    enabled_pct: float | None = None


class PerfStatResult(RunBase):
    """Result from perf stat."""

    suite: str = "perf"
    tool: str = "stat"
    counters: list[PerfCounter] = Field(default_factory=list)
    duration_time: float = 0.0
    ipc: float | None = None
    raw_output: str = ""


class PerfSample(BaseModel):
    """A sampling entry from perf report."""

    overhead_pct: float = 0.0
    command: str = ""
    shared_object: str = ""
    symbol: str = ""
    children_pct: float | None = None


class PerfRecordResult(RunBase):
    """Result from perf record + perf report."""

    suite: str = "perf"
    tool: str = "record"
    samples: list[PerfSample] = Field(default_factory=list)
    total_samples: int = 0
    perf_data_path: str = ""
    raw_output: str = ""


class PerfAnnotationLine(BaseModel):
    """A line from perf annotate."""

    percent: float = 0.0
    address: str = ""
    instruction: str = ""
    source_line: str = ""
    file: str | None = None
    line_number: int | None = None


class PerfAnnotationResult(RunBase):
    """Result from perf annotate."""

    suite: str = "perf"
    tool: str = "annotate"
    symbol: str = ""
    lines: list[PerfAnnotationLine] = Field(default_factory=list)
    raw_output: str = ""
