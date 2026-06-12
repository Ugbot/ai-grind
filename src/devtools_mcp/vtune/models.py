"""Intel VTune Profiler result models."""

from __future__ import annotations

from pydantic import BaseModel, Field

from devtools_mcp.models import RunBase, StackSample


class VtuneFunction(BaseModel):
    """One function row from a VTune CSV report.

    VTune's columns vary by analysis type (hotspots has CPU times, memory-access
    has loads/stores/misses, ...), so numeric columns land in `metrics` keyed by
    the normalized column name. `primary` is the first metric — the one the
    report sorts by (e.g. cpu_time for hotspots).
    """

    function: str
    module: str = ""
    source_file: str = ""
    metrics: dict[str, float] = Field(default_factory=dict)
    primary: float = 0.0


class VtuneResult(RunBase):
    """Result of one `vtune -collect <type>` run plus its decoded reports."""

    suite: str = "vtune"
    tool: str = "cpu"
    analysis_type: str = "hotspots"  # the vtune -collect name actually used
    result_dir: str = ""
    summary_text: str = ""  # bounded `vtune -report summary` output
    functions: list[VtuneFunction] = Field(default_factory=list)
    stack_samples: list[StackSample] = Field(default_factory=list)
    csv_path: str = ""
