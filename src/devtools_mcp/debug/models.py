"""Models for the unified debug layer, protocol-agnostic.

Nothing in here mentions DAP: the same models are populated by DapSession
today and by non-DAP implementations (SAP ADT) later.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field

from devtools_mcp.models import RunBase, StackSample

# Bounds (Tiger Style: everything bounded, fail loud on overflow).
MAX_WATCHES = 16
MAX_PLAN_BREAKPOINTS = 32
MAX_PLAN_STOPS = 50
MAX_PLAN_SECONDS = 300
MAX_FRAMES_PER_STOP = 32
MAX_VARIABLE_NODES = 400
MAX_VARIABLE_DEPTH = 2
MAX_CHILDREN_PER_CONTAINER = 100
MAX_THREADS_CAPTURED = 16
MAX_OUTPUT_LINES = 400


class SessionState(StrEnum):
    """Lifecycle of a debug session."""

    created = "created"
    configuring = "configuring"
    running = "running"
    stopped = "stopped"
    terminated = "terminated"


class DebugCapabilities(BaseModel):
    """What a given implementation supports. Verbs gate on these and return
    a one-line 'adapter X does not support Y' instead of a protocol error."""

    conditional_breakpoints: bool = False
    hit_condition_breakpoints: bool = False
    log_points: bool = False
    function_breakpoints: bool = False
    exception_filters: list[str] = Field(default_factory=list)
    set_variable: bool = False
    set_expression: bool = False
    read_memory: bool = False
    disassemble: bool = False
    step_instruction: bool = False
    step_back: bool = False
    terminate_request: bool = False
    restart_request: bool = False
    evaluate_for_hovers: bool = False
    attach: bool = True


class BreakpointSpec(BaseModel):
    """One requested breakpoint. Covers conditional, hit-count, function
    breakpoints, and logpoints in a single model."""

    source: str = ""  # file path; empty for function breakpoints
    line: int | None = None
    function: str | None = None
    condition: str | None = None
    hit_condition: str | None = None
    log_message: str | None = None


class BreakpointState(BaseModel):
    """One breakpoint as the debugger confirmed it."""

    id: int | None = None
    verified: bool = False
    source: str = ""
    line: int | None = None
    function: str | None = None
    condition: str | None = None
    hit_condition: str | None = None
    log_message: str | None = None
    message: str = ""  # verification failure reason


class StackFrame(BaseModel):
    """A single frame in a thread's call stack."""

    id: int = 0  # implementation handle, valid only while stopped
    index: int = 0
    function: str = ""
    file: str = ""
    line: int | None = None
    column: int | None = None
    module: str = ""

    @property
    def location(self) -> str:
        parts: list[str] = []
        if self.function:
            parts.append(self.function)
        if self.file and self.line is not None:
            parts.append(f"({self.file}:{self.line})")
        elif self.module:
            parts.append(f"(in {self.module})")
        return " ".join(parts) if parts else f"frame#{self.index}"


class ThreadInfo(BaseModel):
    """A thread in the debugged process."""

    thread_id: int
    name: str = ""
    stopped: bool = False
    stop_reason: str = ""
    frames: list[StackFrame] = Field(default_factory=list)


class Variable(BaseModel):
    """One flattened variable node. `path` is the dotted path from the scope
    root (e.g. 'config.retries'), the join key for diffing and querying."""

    path: str
    name: str
    type: str = ""
    value: str = ""
    ref: int = 0  # >0: expandable via variables(ref); valid only while stopped
    depth: int = 0
    scope: str = ""  # "locals", "arguments", "globals", "watch"


class Scope(BaseModel):
    """A variable scope of a stack frame."""

    name: str
    ref: int
    expensive: bool = False


class WatchResult(BaseModel):
    """One watch expression evaluated at a stop."""

    expression: str
    value: str = ""
    type: str = ""
    error: str = ""


class EvalResult(BaseModel):
    """Result of an expression evaluation."""

    value: str = ""
    type: str = ""
    ref: int = 0
    error: str = ""


class VarChange(BaseModel):
    """One variable delta between two consecutive stop snapshots."""

    path: str
    kind: str  # "added" | "removed" | "changed"
    old: str = ""
    new: str = ""


class ExceptionInfo(BaseModel):
    """Exception details when a stop was caused by a throw."""

    exception_id: str = ""
    description: str = ""
    break_mode: str = ""
    stack: str = ""


class StopInfo(BaseModel):
    """Why and where a session stopped."""

    reason: str = ""  # "breakpoint", "step", "exception", "entry", "pause"
    description: str = ""
    thread_id: int | None = None
    hit_breakpoint_ids: list[int] = Field(default_factory=list)
    all_threads_stopped: bool = True


class LaunchConfig(BaseModel):
    """How to start a debuggee. `extra` passes adapter-specific fields
    (e.g. python interpreter, main_class/classpath) without polluting the
    shared surface."""

    program: str
    args: list[str] = Field(default_factory=list)
    cwd: str = ""
    env: dict[str, str] = Field(default_factory=dict)
    stop_on_entry: bool = False
    extra: dict[str, object] = Field(default_factory=dict)


class AttachConfig(BaseModel):
    """How to attach to a running debuggee."""

    pid: int | None = None
    host: str = ""
    port: int | None = None
    program: str = ""  # for symbol resolution where needed
    extra: dict[str, object] = Field(default_factory=dict)


class Instruction(BaseModel):
    """One disassembled instruction."""

    address: str = ""
    bytes: str = ""
    text: str = ""
    function: str = ""
    line: int | None = None


class DebugPlan(BaseModel):
    """A server-side multi-stop capture plan. Each stop is auto-snapshotted
    by the same StopProcessor as interactive stops; the plan just decides
    what verb to issue next and when to hand control back."""

    breakpoints: list[BreakpointSpec] = Field(default_factory=list, max_length=MAX_PLAN_BREAKPOINTS)
    watches: list[str] = Field(default_factory=list, max_length=MAX_WATCHES)
    max_stops: int = Field(default=10, ge=1, le=MAX_PLAN_STOPS)
    per_stop: str = "continue"  # "continue" | "step" | "step_into" | "finish"
    until: str = ""  # expression; truthy at a stop → halt plan, go interactive
    time_budget_s: float = Field(default=120.0, gt=0, le=MAX_PLAN_SECONDS)


class PlanStopRow(BaseModel):
    """One row of a plan report."""

    stop_seq: int
    reason: str = ""
    location: str = ""
    watches: str = ""
    change_count: int = 0
    run_id: str = ""


class PlanReport(BaseModel):
    """Bounded result of a plan execution. The session stays live."""

    stops: list[PlanStopRow] = Field(default_factory=list)
    halted: str = ""  # why the plan ended: "max_stops", "until", "terminated", "time_budget"
    session_state: str = ""
    until_value: str = ""


class DebugSnapshot(RunBase):
    """A point-in-time capture of debug state at one stop (or inspection).

    Stored as a workspace run: queryable via devtools_analyze/query/search.
    parent_run_id links to the previous stop; batch_id = session_id groups
    a session's snapshots.
    """

    suite: str = "debug"
    session_id: str = ""
    adapter: str = ""
    node_id: str = ""  # which session-tree node stopped (root id if single)
    stop_seq: int = 0
    stop_reason: str = ""
    thread_id: int | None = None
    hit_breakpoint_ids: list[int] = Field(default_factory=list)
    threads: list[ThreadInfo] = Field(default_factory=list)
    variables: list[Variable] = Field(default_factory=list)
    watches: list[WatchResult] = Field(default_factory=list)
    breakpoints: list[BreakpointState] = Field(default_factory=list)
    changes: list[VarChange] = Field(default_factory=list)
    exception: ExceptionInfo | None = None
    raw_output: str = ""  # recent debuggee output tail
    # Session-summary runs (tool="session") aggregate one root-first stack
    # per stop, the input for "where did this session stop" flame graphs.
    session_stacks: list[StackSample] = Field(default_factory=list)
