"""JVM result models (JFR / threads / heap / async-profiler)."""

from __future__ import annotations

from pydantic import BaseModel, Field

from devtools_mcp.models import RunBase, StackSample


class JvmThread(BaseModel):
    """One thread from a jstack / Thread.print dump."""

    name: str
    tid: str = ""
    nid: str = ""
    daemon: bool = False
    priority: int | None = None
    state: str = ""  # RUNNABLE, BLOCKED, WAITING, TIMED_WAITING, ...
    frames: list[str] = Field(default_factory=list)  # "pkg.Class.method(File.java:42)"


class JvmHeapClass(BaseModel):
    """One class row from a class histogram (jmap -histo / GC.class_histogram)."""

    rank: int = 0
    instances: int = 0
    bytes: int = 0
    class_name: str = ""


class JvmResult(RunBase):
    """Result from a JVM tool. Fields populated per tool (like DTraceResult)."""

    suite: str = "jvm"
    pid: str = ""
    # jfr / asprof
    stack_samples: list[StackSample] = Field(default_factory=list)
    total_samples: int = 0
    event_counts: dict[str, int] = Field(default_factory=dict)
    jfr_path: str = ""
    # threads
    threads: list[JvmThread] = Field(default_factory=list)
    deadlock: bool = False
    # heap
    heap_classes: list[JvmHeapClass] = Field(default_factory=list)
    total_bytes: int = 0
    raw_output: str = ""
