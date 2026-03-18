"""DTrace result models."""

from __future__ import annotations

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
