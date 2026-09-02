"""DebugSession ABC, session tree, and DebugSessionManager.

DebugSession is the one interface every debugger implementation provides.
Nothing in it mentions DAP, DapSession is one implementation; a SAP ADT
session implements the same surface without DAP anywhere.

Sessions form a TREE because some implementations (js-debug) spawn one
child session per real process. The manager keys trees by root id, the
only id the tool layer exposes, and keeps a focus pointer on the most
recently stopped descendant so callers never need to know children exist.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

from devtools_mcp.debug.models import (
    MAX_OUTPUT_LINES,
    MAX_WATCHES,
    AttachConfig,
    BreakpointSpec,
    BreakpointState,
    DebugCapabilities,
    DebugSnapshot,
    EvalResult,
    Instruction,
    LaunchConfig,
    Scope,
    SessionState,
    StackFrame,
    StopInfo,
    ThreadInfo,
    Variable,
)

# Bounds.
MAX_SESSION_TREES = 16
MAX_CHILD_SESSIONS = 32

SnapshotSink = Callable[[DebugSnapshot], str]  # returns run_id


class DebugSession(ABC):
    """One debugger conversation. Shared concrete state lives here so every
    implementation gets watches, output buffering, and stop bookkeeping
    for free; the abstract methods are the per-protocol surface."""

    def __init__(self, session_id: str, adapter_name: str) -> None:
        self.session_id = session_id
        self.adapter_name = adapter_name
        self.state: SessionState = SessionState.created
        self.capabilities = DebugCapabilities()
        self.watches: list[str] = []
        self.last_snapshot: DebugSnapshot | None = None
        self.last_stop: StopInfo | None = None
        self.output: deque[str] = deque(maxlen=MAX_OUTPUT_LINES)
        self.stop_seq = 0
        self.breakpoints: dict[str, list[BreakpointState]] = {}  # source -> confirmed states
        self.breakpoint_specs: dict[str, list[BreakpointSpec]] = {}  # source -> requested
        self.function_breakpoint_specs: list[BreakpointSpec] = []
        self.selected_thread_id: int | None = None
        self.selected_frame_id: int | None = None
        self._state_cond = asyncio.Condition()

    # -- shared concrete behavior ------------------------------------------

    async def set_state(self, state: SessionState) -> None:
        async with self._state_cond:
            self.state = state
            self._state_cond.notify_all()

    async def wait_until(self, states: set[SessionState], timeout: float) -> SessionState | None:
        """Wait until the session enters one of `states`. Returns the state
        reached, or None on timeout."""

        async def _wait() -> SessionState:
            async with self._state_cond:
                await self._state_cond.wait_for(lambda: self.state in states)
                return self.state

        try:
            return await asyncio.wait_for(_wait(), timeout=timeout)
        except TimeoutError:
            return None

    def add_watch(self, expression: str) -> str | None:
        """Add a watch expression. Returns an error string if rejected."""
        expression = expression.strip()
        if not expression:
            return "empty watch expression"
        if expression in self.watches:
            return None
        if len(self.watches) >= MAX_WATCHES:
            return f"watch limit reached ({MAX_WATCHES}); remove one first"
        self.watches.append(expression)
        return None

    def remove_watch(self, expression: str) -> bool:
        try:
            self.watches.remove(expression.strip())
        except ValueError:
            return False
        return True

    def append_output(self, text: str) -> None:
        for line in text.splitlines():
            self.output.append(line)

    def output_tail(self, lines: int = 20) -> str:
        return "\n".join(list(self.output)[-lines:])

    async def add_breakpoint(self, spec: BreakpointSpec) -> list[BreakpointState]:
        """Add one breakpoint, merging with the existing set for its source
        (set_breakpoints REPLACES a source's breakpoints wholesale)."""
        if spec.function and not spec.source:
            self.function_breakpoint_specs.append(spec)
            return await self.set_function_breakpoints(self.function_breakpoint_specs)
        specs = self.breakpoint_specs.setdefault(spec.source, [])
        specs.append(spec)
        return await self.set_breakpoints(spec.source, specs)

    async def remove_breakpoint(self, bp_id: int) -> bool:
        """Remove a breakpoint by its confirmed id. Returns False if unknown."""
        for source, states in list(self.breakpoints.items()):
            for index, state in enumerate(states):
                if state.id != bp_id:
                    continue
                if source == "<functions>":
                    if index < len(self.function_breakpoint_specs):
                        del self.function_breakpoint_specs[index]
                    await self.set_function_breakpoints(self.function_breakpoint_specs)
                else:
                    specs = self.breakpoint_specs.get(source, [])
                    if index < len(specs):
                        del specs[index]
                    await self.set_breakpoints(source, specs)
                return True
        return False

    # -- abstract surface (protocol-agnostic) ------------------------------

    @abstractmethod
    async def launch(self, config: LaunchConfig) -> None: ...

    @abstractmethod
    async def attach(self, config: AttachConfig) -> None: ...

    @abstractmethod
    async def set_breakpoints(self, source: str, bps: list[BreakpointSpec]) -> list[BreakpointState]: ...

    @abstractmethod
    async def set_function_breakpoints(self, bps: list[BreakpointSpec]) -> list[BreakpointState]: ...

    @abstractmethod
    async def set_exception_breakpoints(self, filters: list[str]) -> None: ...

    @abstractmethod
    async def continue_(self, thread_id: int | None = None) -> None: ...

    @abstractmethod
    async def pause(self, thread_id: int | None = None) -> None: ...

    @abstractmethod
    async def step(
        self,
        kind: Literal["over", "into", "out"],
        thread_id: int | None = None,
        granularity: Literal["statement", "instruction"] = "statement",
    ) -> None: ...

    @abstractmethod
    async def threads(self) -> list[ThreadInfo]: ...

    @abstractmethod
    async def stack_trace(self, thread_id: int, start: int = 0, levels: int = 32) -> list[StackFrame]: ...

    @abstractmethod
    async def scopes(self, frame_id: int) -> list[Scope]: ...

    @abstractmethod
    async def variables(self, container_ref: int, start: int = 0, count: int = 100) -> list[Variable]: ...

    @abstractmethod
    async def evaluate(self, expression: str, frame_id: int | None = None, context: str = "repl") -> EvalResult: ...

    @abstractmethod
    async def set_variable(self, container_ref: int, name: str, value: str) -> Variable: ...

    @abstractmethod
    async def read_memory(self, address: str, count: int) -> bytes: ...

    @abstractmethod
    async def disassemble(self, address: str, count: int) -> list[Instruction]: ...

    @abstractmethod
    async def raw_command(self, command: str) -> str:
        """Implementation-native escape hatch (DAP: evaluate context='repl')."""

    @abstractmethod
    async def disconnect(self, terminate: bool = True) -> None: ...


@dataclass
class SessionNode:
    """One node of a session tree."""

    node_id: str
    session: DebugSession
    parent: SessionNode | None = None
    children: list[SessionNode] = field(default_factory=list)
    label: str = ""

    def walk(self) -> list[SessionNode]:
        nodes = [self]
        for child in self.children:
            nodes.extend(child.walk())
        return nodes


class DebugSessionManager:
    """Owns every session tree. Keys trees by root session id, the only id
    the tool layer sees, and keeps a per-tree focus pointer on the most
    recently stopped node so verbs auto-target the right session."""

    def __init__(self) -> None:
        self.trees: dict[str, SessionNode] = {}
        self._focus: dict[str, SessionNode] = {}
        self._child_counters: dict[str, itertools.count] = {}
        self._tree_conds: dict[str, asyncio.Condition] = {}

    def register_root(self, session: DebugSession) -> SessionNode:
        assert len(self.trees) < MAX_SESSION_TREES, f"too many debug sessions (max {MAX_SESSION_TREES})"
        assert session.session_id not in self.trees, f"duplicate session {session.session_id}"
        node = SessionNode(node_id=session.session_id, session=session)
        self.trees[session.session_id] = node
        self._focus[session.session_id] = node
        self._child_counters[session.session_id] = itertools.count(1)
        self._tree_conds[session.session_id] = asyncio.Condition()
        return node

    def add_child(self, parent: SessionNode, session: DebugSession, label: str = "") -> SessionNode:
        root = self._root_of(parent)
        existing = len(root.walk()) - 1
        assert existing < MAX_CHILD_SESSIONS, f"too many child sessions (max {MAX_CHILD_SESSIONS})"
        suffix = next(self._child_counters[root.node_id])
        node = SessionNode(
            node_id=f"{root.node_id}/{suffix}",
            session=session,
            parent=parent,
            label=label,
        )
        parent.children.append(node)
        return node

    def _root_of(self, node: SessionNode) -> SessionNode:
        while node.parent is not None:
            node = node.parent
        return node

    def resolve(self, session_id: str, child: str | None = None) -> SessionNode:
        """session_id → focused node (or explicit child). Raises KeyError."""
        root = self.trees.get(session_id)
        if root is None:
            raise KeyError(f"No active debug session '{session_id}'. Active: {list(self.trees.keys()) or 'none'}")
        if child:
            wanted = child if child.startswith(session_id) else f"{session_id}/{child}"
            for node in root.walk():
                if node.node_id == wanted:
                    return node
            raise KeyError(f"No child '{child}' in session {session_id}: {[n.node_id for n in root.walk()]}")
        return self._focus.get(session_id, root)

    def on_stopped(self, node: SessionNode) -> None:
        """A node stopped. It becomes the tree's focus."""
        root = self._root_of(node)
        self._focus[root.node_id] = node

    async def notify_tree(self, node: SessionNode) -> None:
        """Wake anyone waiting on this node's tree (stop/termination)."""
        root = self._root_of(node)
        cond = self._tree_conds.get(root.node_id)
        if cond is not None:
            async with cond:
                cond.notify_all()

    def _tree_progress(self, session_id: str) -> SessionNode | None:
        """A stopped node (focus first), or the root if every node
        terminated, else None (still running)."""
        root = self.trees.get(session_id)
        if root is None:
            return None
        focus = self._focus.get(session_id, root)
        if focus.session.state == SessionState.stopped:
            return focus
        for node in root.walk():
            if node.session.state == SessionState.stopped:
                return node
        if all(n.session.state == SessionState.terminated for n in root.walk()):
            return root
        return None

    async def wait_for_stop(self, session_id: str, timeout: float) -> SessionNode | None:
        """Tree-aware wait: returns the stopped node, the root if the whole
        tree terminated, or None on timeout. Multi-session adapters
        (js-debug) stop on CHILD sessions. Never wait on one session's
        state directly."""
        cond = self._tree_conds.get(session_id)
        if cond is None:
            return self._tree_progress(session_id)

        async def _wait() -> SessionNode:
            async with cond:
                while True:
                    node = self._tree_progress(session_id)
                    if node is not None:
                        return node
                    await cond.wait()

        try:
            return await asyncio.wait_for(_wait(), timeout=timeout)
        except TimeoutError:
            return self._tree_progress(session_id)

    def children_of(self, session_id: str) -> list[SessionNode]:
        root = self.trees.get(session_id)
        return root.walk()[1:] if root else []

    async def stop_tree(self, session_id: str, terminate: bool = True) -> int:
        """Disconnect every node of a tree, leaves first. Returns node count."""
        root = self.trees.pop(session_id, None)
        self._focus.pop(session_id, None)
        self._child_counters.pop(session_id, None)
        self._tree_conds.pop(session_id, None)
        if root is None:
            return 0
        nodes = root.walk()
        for node in reversed(nodes):
            with contextlib.suppress(Exception):  # best-effort teardown
                await node.session.disconnect(terminate=terminate)
        return len(nodes)

    async def stop_all(self) -> None:
        for session_id in list(self.trees.keys()):
            await self.stop_tree(session_id)
