"""Python profiling/inspection result models."""

from __future__ import annotations

from pydantic import BaseModel, Field

from devtools_mcp.models import RunBase, StackSample


class PyFuncStat(BaseModel):
    """One row from cProfile/pstats."""

    function: str  # "file.py:line(name)"
    ncalls: int = 0
    tottime: float = 0.0  # time in the function itself
    cumtime: float = 0.0  # time including callees
    percall_tot: float = 0.0
    percall_cum: float = 0.0


class PyThread(BaseModel):
    """One thread from `py-spy dump`."""

    tid: str = ""
    name: str = ""
    state: str = ""  # active / idle
    frames: list[str] = Field(default_factory=list)  # "func (file.py:line)"


class PyResult(RunBase):
    """Result from a Python tool (fields populated per tool)."""

    suite: str = "py"
    pid: str = ""
    stack_samples: list[StackSample] = Field(default_factory=list)  # pyspy / memray
    total_samples: int = 0
    func_stats: list[PyFuncStat] = Field(default_factory=list)  # cprofile
    threads: list[PyThread] = Field(default_factory=list)  # dump
    profile_path: str = ""
    raw_output: str = ""
