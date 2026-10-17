"""DTrace result models."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from devtools_mcp.models import RunBase


class DTraceAggregation(BaseModel):
    """A single aggregation entry from @count, @sum, @avg, etc."""

    keys: list[str] = Field(default_factory=list)
    value: int = 0
    agg_type: str = "count"


class DTraceStackTrace(BaseModel):
    """A captured stack trace with occurrence count."""

    frames: list[str] = Field(default_factory=list)
    count: int = 1


class DTraceQuantizeBucket(BaseModel):
    """A single bucket in a quantize/lquantize distribution."""

    low: int = 0
    high: int = 0
    count: int = 0


class DTraceQuantization(BaseModel):
    """A full quantize/lquantize distribution."""

    key: str = ""
    buckets: list[DTraceQuantizeBucket] = Field(default_factory=list)
    total: int = 0


class DTraceProbeHit(BaseModel):
    """A single probe firing event from printf output."""

    probe: str = ""
    timestamp: int | None = None
    pid: int | None = None
    tid: int | None = None
    execname: str | None = None
    cpu: int | None = None
    args: str = ""


class DTraceCollectionDiagnostics(BaseModel):
    """Observed stderr diagnostics, never proof that probe collection was complete."""

    captured_stderr_bytes: int = Field(default=0, ge=0)
    scanned_stderr_bytes: int = Field(default=0, ge=0)
    capture_complete: bool = False
    scan_complete: bool = False
    diagnostic_lines: int = Field(default=0, ge=0)
    error_lines: int = Field(default=0, ge=0)
    drop_lines: int = Field(default=0, ge=0)
    advisory_lines: int = Field(default=0, ge=0)
    examples: list[Annotated[str, Field(max_length=1024)]] = Field(default_factory=list, max_length=8)
    examples_truncated: bool = False


class DTraceResult(RunBase):
    """Result from a DTrace run."""

    suite: str = "dtrace"
    script: str = ""
    one_liner: str = ""
    aggregations: list[DTraceAggregation] = Field(default_factory=list)
    stacks: list[DTraceStackTrace] = Field(default_factory=list)
    quantizations: list[DTraceQuantization] = Field(default_factory=list)
    probe_hits: list[DTraceProbeHit] = Field(default_factory=list)
    raw_output: str = ""
    # None preserves legacy/parser-only results: no stderr inspection is implied.
    collection_diagnostics: DTraceCollectionDiagnostics | None = None
