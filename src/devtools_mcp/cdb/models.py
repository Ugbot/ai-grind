"""CDB (Windows debugger) batch-mode result models."""

from __future__ import annotations

from pydantic import BaseModel, Field

from devtools_mcp.models import RunBase


class CdbStackFrame(BaseModel):
    """One frame from a `k` / `~*k` backtrace."""

    index: int = 0
    module: str = ""
    function: str = ""
    offset: str = ""  # "+0x20"
    file: str | None = None
    line: int | None = None

    @property
    def symbol(self) -> str:
        sym = f"{self.module}!{self.function}" if self.module else self.function
        return sym or "<unknown>"


class CdbThread(BaseModel):
    """A thread from `~*k`."""

    index: int = 0
    tid: str = ""
    frames: list[CdbStackFrame] = Field(default_factory=list)


class CdbSnapshot(RunBase):
    """Batch CDB output captured as a workspace run (queryable like any other)."""

    suite: str = "cdb"
    threads: list[CdbThread] = Field(default_factory=list)
    analysis: dict[str, str] = Field(default_factory=dict)  # !analyze -v key fields
    registers: dict[str, str] = Field(default_factory=dict)
    exception: str = ""
    raw_output: str = ""
