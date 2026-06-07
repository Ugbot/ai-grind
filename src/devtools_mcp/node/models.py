"""Node.js / V8 profiling result models."""

from __future__ import annotations

from pydantic import Field

from devtools_mcp.models import RunBase, StackSample


class NodeResult(RunBase):
    """Result from a Node profile (CPU or heap)."""

    suite: str = "node"
    stack_samples: list[StackSample] = Field(default_factory=list)
    total_weight: int = 0  # samples (cpu) or bytes (heap)
    weight_unit: str = "samples"  # "samples" | "bytes"
    profile_path: str = ""
    raw_output: str = ""
