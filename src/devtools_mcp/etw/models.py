"""Windows ETW (PerfView) result models."""

from __future__ import annotations

from pydantic import BaseModel, Field

from devtools_mcp.models import RunBase, StackSample


class EtwSample(BaseModel):
    """One symbol-resolved CPU node from PerfView's flat stack CSV.

    `exc` is exclusive (leaf) samples, `inc` is inclusive (subtree). Percentages
    are of total CPU samples in the focused process.
    """

    name: str  # raw "module!function" as PerfView prints it
    module: str = ""
    function: str = ""
    exc: float = 0.0
    exc_pct: float = 0.0
    inc: float = 0.0
    inc_pct: float = 0.0
    first_ms: float = 0.0
    last_ms: float = 0.0


class EtwResult(RunBase):
    """Result from an ETW CPU profile via PerfView."""

    suite: str = "etw"
    tool: str = "cpu"
    process: str = ""
    samples: list[EtwSample] = Field(default_factory=list)
    stack_samples: list[StackSample] = Field(default_factory=list)
    etl_path: str = ""
    csv_path: str = ""
    raw_output: str = ""
