"""DapSession: the DAP implementation of DebugSession.

Owns the adapter conversation: the initialize → (launch|attach) →
initialized → setBreakpoints → configurationDone dance, event handling,
reverse requests (runInTerminal, startDebugging), and the stop pipeline
hook. Everything adapter-specific comes from the AdapterSpec.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import os
import uuid
from typing import Literal

from devtools_mcp.debug.adapters.base import AdapterSpec
from devtools_mcp.debug.models import (
    AttachConfig,
    BreakpointSpec,
    BreakpointState,
    DebugCapabilities,
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
from devtools_mcp.debug.protocol import (
    DapConnection,
    DapError,
    DapRequestError,
    DapTransport,
)
from devtools_mcp.debug.session import (
    DebugSession,
    DebugSessionManager,
    SessionNode,
    SnapshotSink,
)

_CONFIGURE_TIMEOUT = 30.0
_LAUNCH_TIMEOUT = 60.0
_DISCONNECT_TIMEOUT = 5.0


class UnsupportedCapability(Exception):
    """Raised when a verb needs a capability the adapter lacks."""


def caps_from_initialize(body: dict) -> DebugCapabilities:
    filters = [f.get("filter", "") for f in body.get("exceptionBreakpointFilters", [])]
    return DebugCapabilities(
        conditional_breakpoints=bool(body.get("supportsConditionalBreakpoints")),
        hit_condition_breakpoints=bool(body.get("supportsHitConditionalBreakpoints")),
        log_points=bool(body.get("supportsLogPoints")),
        function_breakpoints=bool(body.get("supportsFunctionBreakpoints")),
        exception_filters=[f for f in filters if f],
        set_variable=bool(body.get("supportsSetVariable")),
        set_expression=bool(body.get("supportsSetExpression")),
        read_memory=bool(body.get("supportsReadMemoryRequest")),
        disassemble=bool(body.get("supportsDisassembleRequest")),
        step_instruction=bool(body.get("supportsSteppingGranularity")),
        step_back=bool(body.get("supportsStepBack")),
        terminate_request=bool(body.get("supportsTerminateRequest")),
        restart_request=bool(body.get("supportsRestartRequest")),
        evaluate_for_hovers=bool(body.get("supportsEvaluateForHovers")),
    )


class DapSession(DebugSession):
    """One DAP conversation with one adapter process/socket."""

    def __init__(
        self,
        session_id: str,
        adapter: AdapterSpec,
        manager: DebugSessionManager,
        snapshot_sink: SnapshotSink | None = None,
    ) -> None:
        super().__init__(session_id=session_id, adapter_name=adapter.name)
        self.adapter = adapter
        self.manager = manager
        self.snapshot_sink = snapshot_sink
        self.node: SessionNode | None = None  # set right after registration
        self.binary = ""  # what we're debugging, for RunBase provenance
        self.conn: DapConnection | None = None
        self.transport: DapTransport | None = None
        self._config: LaunchConfig | AttachConfig | None = None
        self._initialized = asyncio.Event()
        self._exc_filters: list[str] | None = None  # None = adapter defaults
        self._exit_code: int | None = None
        self._terminal_procs: list[asyncio.subprocess.Process] = []
        self._output_pumps: list[asyncio.Task] = []

    # -- connection lifecycle ----------------------------------------------

    async def _connect(self, config: LaunchConfig | AttachConfig) -> None:
        self._config = config
        self.transport = await self.adapter.transport(config)
        self.conn = DapConnection(self.transport, self._on_event, self._on_reverse_request)
        self.conn.start()
        body = await self.conn.request(
            "initialize",
            {
                "clientID": "devtools-mcp",
                "clientName": "devtools-mcp",
                "adapterID": self.adapter.name,
                "pathFormat": "path",
                "linesStartAt1": True,
                "columnsStartAt1": True,
                "supportsRunInTerminalRequest": True,
                "supportsStartDebuggingRequest": True,
                "supportsVariableType": True,
                "supportsMemoryReferences": True,
                "locale": "en",
            },
        )
        self.capabilities = caps_from_initialize(body)

    async def _configure_and_run(self, request: str, arguments: dict) -> None:
        """Send launch/attach, service the initialized event (breakpoints +
        configurationDone), then await the launch/attach response."""
        assert self.conn is not None
        await self.set_state(SessionState.configuring)
        run_future = asyncio.ensure_future(self.conn.request(request, arguments, timeout=_LAUNCH_TIMEOUT))
        try:
            await asyncio.wait_for(self._initialized.wait(), timeout=_CONFIGURE_TIMEOUT)
        except TimeoutError:
            if run_future.done() and run_future.exception() is not None:
                raise run_future.exception() from None  # the real failure
            run_future.cancel()
            raise DapError(f"{self.adapter.name}: adapter never sent 'initialized'") from None
        for source, specs in self.breakpoint_specs.items():
            await self._send_breakpoints(source, specs)
        if self.function_breakpoint_specs and self.capabilities.function_breakpoints:
            await self._send_function_breakpoints()
        await self._send_exception_filters()
        # A few adapters don't accept configurationDone; the launch response decides.
        with contextlib.suppress(DapRequestError):
            await self.conn.request("configurationDone")
        await run_future
        async with self._state_cond:
            configuring = self.state == SessionState.configuring
        if configuring:
            await self.set_state(SessionState.running)

    async def launch(self, config: LaunchConfig) -> None:
        self.binary = config.program
        await self._connect(config)
        try:
            await self._configure_and_run("launch", self.adapter.launch_template(config))
        except BaseException:
            # A failed launch must not leak the adapter process (or a
            # suspended inferior it already spawned).
            await self.disconnect(terminate=True)
            raise

    async def attach(self, config: AttachConfig) -> None:
        self.binary = config.program or (f"pid:{config.pid}" if config.pid else f"{config.host}:{config.port}")
        await self._connect(config)
        try:
            await self._configure_and_run("attach", self.adapter.attach_template(config))
        except BaseException:
            await self.disconnect(terminate=True)
            raise

    async def start_child(self, request: str, configuration: dict) -> None:
        """Start this session as a startDebugging child: the configuration
        dict from the parent adapter is passed through verbatim."""
        assert self._config is not None, "child session needs the parent config for transport"
        self.binary = str(configuration.get("program") or configuration.get("name") or "child")
        self.transport = await self.adapter.transport(self._config)
        self.conn = DapConnection(self.transport, self._on_event, self._on_reverse_request)
        self.conn.start()
        body = await self.conn.request(
            "initialize",
            {
                "clientID": "devtools-mcp",
                "adapterID": self.adapter.name,
                "pathFormat": "path",
                "linesStartAt1": True,
                "columnsStartAt1": True,
                "supportsRunInTerminalRequest": True,
                "supportsStartDebuggingRequest": True,
                "supportsVariableType": True,
            },
        )
        self.capabilities = caps_from_initialize(body)
        await self._configure_and_run(request, configuration)

    # -- events --------------------------------------------------------------

    async def _on_event(self, event: str, body: dict) -> None:
        if event == "initialized":
            self._initialized.set()
        elif event == "output":
            category = body.get("category", "console")
            if category in ("stdout", "stderr", "console", "important"):
                self.append_output(body.get("output", ""))
        elif event == "stopped":
            await self._on_stopped(body)
        elif event == "continued":
            await self.set_state(SessionState.running)
        elif event == "terminated":
            await self.set_state(SessionState.terminated)
            if self.node is not None:
                await self.manager.notify_tree(self.node)
        elif event == "exited":
            self._exit_code = body.get("exitCode")
            self.append_output(f"[process exited with code {self._exit_code}]")

    async def _on_stopped(self, body: dict) -> None:
        stop = StopInfo(
            reason=body.get("reason", ""),
            description=body.get("description", "") or body.get("text", ""),
            thread_id=body.get("threadId"),
            hit_breakpoint_ids=list(body.get("hitBreakpointIds") or []),
            all_threads_stopped=bool(body.get("allThreadsStopped", True)),
        )
        self.last_stop = stop
        # Frame/thread selections are handles into the previous stop, stale now.
        self.selected_thread_id = None
        self.selected_frame_id = None
        if self.node is not None:
            self.manager.on_stopped(self.node)
        # Capture the snapshot BEFORE flipping to stopped so anyone woken by
        # wait_until(stopped) can read last_snapshot immediately.
        try:
            from devtools_mcp.debug.snapshot import StopProcessor

            await StopProcessor().process(self, stop)
        except Exception as exc:  # noqa: BLE001  # a failed capture must not wedge the session
            self.append_output(f"[snapshot capture failed: {exc}]")
        await self.set_state(SessionState.stopped)
        if self.node is not None:
            await self.manager.notify_tree(self.node)

    # -- reverse requests ----------------------------------------------------

    async def _on_reverse_request(self, command: str, arguments: dict) -> dict:
        if command == "runInTerminal":
            return await self._run_in_terminal(arguments)
        if command == "startDebugging":
            child_request = arguments.get("request", "attach")
            configuration = arguments.get("configuration") or {}
            await spawn_child(self, child_request, configuration)
            return {}
        raise DapError(f"unsupported reverse request: {command}")

    async def _run_in_terminal(self, arguments: dict) -> dict:
        """We have no terminal: spawn the debuggee ourselves and pipe its
        output into the session ring."""
        argv = [str(a) for a in arguments.get("args", [])]
        if not argv:
            raise DapError("runInTerminal with empty args")
        env = {str(k): str(v) for k, v in (arguments.get("env") or {}).items() if v is not None}
        import os

        full_env = dict(os.environ)
        full_env.update(env)
        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=arguments.get("cwd") or None,
            env=full_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )
        self._terminal_procs.append(process)
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                self._output_pumps.append(asyncio.ensure_future(self._pump_output(stream)))
        return {"processId": process.pid}

    async def _pump_output(self, stream: asyncio.StreamReader) -> None:
        try:
            while True:
                line = await stream.readline()
                if not line:
                    return
                self.append_output(line.decode("utf-8", errors="replace"))
        except (asyncio.CancelledError, OSError):
            return

    # -- breakpoints -----------------------------------------------------------

    def _check_bp_capabilities(self, specs: list[BreakpointSpec]) -> None:
        if self.conn is None:
            return  # pre-launch: capabilities unknown until initialize
        caps = self.capabilities
        for spec in specs:
            if spec.condition and not caps.conditional_breakpoints:
                raise UnsupportedCapability(f"adapter {self.adapter.name} does not support conditional breakpoints")
            if spec.hit_condition and not caps.hit_condition_breakpoints:
                raise UnsupportedCapability(f"adapter {self.adapter.name} does not support hit-count conditions")
            if spec.log_message and not caps.log_points:
                raise UnsupportedCapability(f"adapter {self.adapter.name} does not support logpoints")

    async def set_breakpoints(self, source: str, bps: list[BreakpointSpec]) -> list[BreakpointState]:
        self._check_bp_capabilities(bps)
        self.breakpoint_specs[source] = bps
        if self.conn is None or self.state == SessionState.created:
            # Pre-launch: applied at the initialized event.
            return [BreakpointState(source=source, line=bp.line, condition=bp.condition, verified=False) for bp in bps]
        return await self._send_breakpoints(source, bps)

    async def _send_breakpoints(self, source: str, bps: list[BreakpointSpec]) -> list[BreakpointState]:
        assert self.conn is not None
        payload = []
        for bp in bps:
            entry: dict[str, object] = {"line": bp.line}
            if bp.condition:
                entry["condition"] = bp.condition
            if bp.hit_condition:
                entry["hitCondition"] = bp.hit_condition
            if bp.log_message:
                entry["logMessage"] = bp.log_message
            payload.append(entry)
        # source.name is required alongside path: kotlin-debug-adapter NPEs
        # internally without it, and no adapter minds it being present.
        body = await self.conn.request(
            "setBreakpoints",
            {"source": {"path": source, "name": os.path.basename(source)}, "breakpoints": payload},
        )
        states = []
        for spec, result in zip(bps, body.get("breakpoints", []), strict=False):
            states.append(
                BreakpointState(
                    id=result.get("id"),
                    verified=bool(result.get("verified")),
                    source=source,
                    line=result.get("line", spec.line),
                    condition=spec.condition,
                    hit_condition=spec.hit_condition,
                    log_message=spec.log_message,
                    message=result.get("message", ""),
                )
            )
        self.breakpoints[source] = states
        return states

    async def set_function_breakpoints(self, bps: list[BreakpointSpec]) -> list[BreakpointState]:
        if self.conn is not None and not self.capabilities.function_breakpoints:
            raise UnsupportedCapability(f"adapter {self.adapter.name} does not support function breakpoints")
        self._check_bp_capabilities(bps)
        self.function_breakpoint_specs = bps
        if self.conn is None or self.state == SessionState.created:
            return [BreakpointState(function=bp.function, condition=bp.condition) for bp in bps]
        return await self._send_function_breakpoints()

    async def _send_function_breakpoints(self) -> list[BreakpointState]:
        assert self.conn is not None
        payload = []
        for bp in self.function_breakpoint_specs:
            entry: dict[str, object] = {"name": bp.function}
            if bp.condition:
                entry["condition"] = bp.condition
            if bp.hit_condition:
                entry["hitCondition"] = bp.hit_condition
            payload.append(entry)
        body = await self.conn.request("setFunctionBreakpoints", {"breakpoints": payload})
        states = []
        for spec, result in zip(self.function_breakpoint_specs, body.get("breakpoints", []), strict=False):
            states.append(
                BreakpointState(
                    id=result.get("id"),
                    verified=bool(result.get("verified")),
                    function=spec.function,
                    line=result.get("line"),
                    condition=spec.condition,
                    message=result.get("message", ""),
                )
            )
        self.breakpoints["<functions>"] = states
        return states

    async def set_exception_breakpoints(self, filters: list[str]) -> None:
        known = set(self.capabilities.exception_filters)
        unknown = [f for f in filters if f not in known]
        if unknown:
            raise UnsupportedCapability(
                f"adapter {self.adapter.name} has no exception filters {unknown}; available: {sorted(known)}"
            )
        self._exc_filters = filters
        if self.conn is not None and self.state != SessionState.created:
            await self._send_exception_filters()

    async def _send_exception_filters(self) -> None:
        assert self.conn is not None
        filters = self._exc_filters if self._exc_filters is not None else []
        # Adapters without exception support may reject this; not fatal.
        with contextlib.suppress(DapRequestError):
            await self.conn.request("setExceptionBreakpoints", {"filters": filters})

    # -- execution -------------------------------------------------------------

    def _require_conn(self) -> DapConnection:
        if self.conn is None:
            raise DapError("session not started. Call launch or attach first")
        return self.conn

    def _default_thread(self) -> int | None:
        if self.selected_thread_id is not None:
            return self.selected_thread_id
        if self.last_stop is not None and self.last_stop.thread_id is not None:
            return self.last_stop.thread_id
        return None

    async def continue_(self, thread_id: int | None = None) -> None:
        conn = self._require_conn()
        tid = thread_id if thread_id is not None else self._default_thread()
        await conn.request("continue", {"threadId": tid if tid is not None else 0})
        await self.set_state(SessionState.running)

    async def pause(self, thread_id: int | None = None) -> None:
        conn = self._require_conn()
        tid = thread_id if thread_id is not None else self._default_thread()
        if tid is None:
            threads = await self.threads()
            tid = threads[0].thread_id if threads else 0
        await conn.request("pause", {"threadId": tid})

    _STEP_COMMANDS = {"over": "next", "into": "stepIn", "out": "stepOut"}

    async def step(
        self,
        kind: Literal["over", "into", "out"],
        thread_id: int | None = None,
        granularity: Literal["statement", "instruction"] = "statement",
    ) -> None:
        conn = self._require_conn()
        if granularity == "instruction" and not self.capabilities.step_instruction:
            raise UnsupportedCapability(f"adapter {self.adapter.name} does not support instruction stepping")
        tid = thread_id if thread_id is not None else self._default_thread()
        if tid is None:
            raise DapError("no stopped thread to step, is the session stopped?")
        arguments: dict[str, object] = {"threadId": tid}
        if granularity == "instruction":
            arguments["granularity"] = "instruction"
        await conn.request(self._STEP_COMMANDS[kind], arguments)
        await self.set_state(SessionState.running)

    # -- inspection --------------------------------------------------------------

    async def threads(self) -> list[ThreadInfo]:
        conn = self._require_conn()
        body = await conn.request("threads")
        stopped = self.state == SessionState.stopped or (
            self.last_stop is not None and self.last_stop.all_threads_stopped
        )
        return [
            ThreadInfo(thread_id=t.get("id", 0), name=t.get("name", ""), stopped=stopped)
            for t in body.get("threads", [])
        ]

    async def stack_trace(self, thread_id: int, start: int = 0, levels: int = 32) -> list[StackFrame]:
        conn = self._require_conn()
        body = await conn.request("stackTrace", {"threadId": thread_id, "startFrame": start, "levels": levels})
        frames = []
        for index, raw in enumerate(body.get("stackFrames", []), start=start):
            source = raw.get("source") or {}
            frames.append(
                StackFrame(
                    id=raw.get("id", 0),
                    index=index,
                    function=raw.get("name", ""),
                    file=source.get("path", "") or source.get("name", ""),
                    line=raw.get("line"),
                    column=raw.get("column"),
                    module=raw.get("moduleId", "") or "",
                )
            )
        return frames

    async def scopes(self, frame_id: int) -> list[Scope]:
        conn = self._require_conn()
        body = await conn.request("scopes", {"frameId": frame_id})
        return [
            Scope(
                name=s.get("name", ""),
                ref=s.get("variablesReference", 0),
                expensive=bool(s.get("expensive")),
            )
            for s in body.get("scopes", [])
        ]

    async def variables(self, container_ref: int, start: int = 0, count: int = 100) -> list[Variable]:
        conn = self._require_conn()
        arguments: dict[str, object] = {"variablesReference": container_ref}
        if start:
            arguments["start"] = start
        if count:
            arguments["count"] = count
        body = await conn.request("variables", arguments)
        return [
            Variable(
                path=v.get("name", ""),
                name=v.get("name", ""),
                type=v.get("type", "") or "",
                value=v.get("value", ""),
                ref=v.get("variablesReference", 0),
            )
            for v in body.get("variables", [])
        ]

    async def evaluate(self, expression: str, frame_id: int | None = None, context: str = "repl") -> EvalResult:
        conn = self._require_conn()
        arguments: dict[str, object] = {"expression": expression, "context": context}
        if frame_id is not None:
            arguments["frameId"] = frame_id
        try:
            body = await conn.request("evaluate", arguments)
        except DapRequestError as exc:
            return EvalResult(error=exc.message)
        return EvalResult(
            value=body.get("result", ""),
            type=body.get("type", "") or "",
            ref=body.get("variablesReference", 0),
        )

    async def set_variable(self, container_ref: int, name: str, value: str) -> Variable:
        if not self.capabilities.set_variable:
            raise UnsupportedCapability(f"adapter {self.adapter.name} does not support setVariable")
        conn = self._require_conn()
        body = await conn.request("setVariable", {"variablesReference": container_ref, "name": name, "value": value})
        return Variable(
            path=name,
            name=name,
            type=body.get("type", "") or "",
            value=body.get("value", ""),
            ref=body.get("variablesReference", 0),
        )

    async def read_memory(self, address: str, count: int) -> bytes:
        if not self.capabilities.read_memory:
            raise UnsupportedCapability(f"adapter {self.adapter.name} does not support memory reads")
        conn = self._require_conn()
        body = await conn.request("readMemory", {"memoryReference": address, "count": count})
        data = body.get("data", "")
        return base64.b64decode(data) if data else b""

    async def disassemble(self, address: str, count: int) -> list[Instruction]:
        if not self.capabilities.disassemble:
            raise UnsupportedCapability(f"adapter {self.adapter.name} does not support disassembly")
        conn = self._require_conn()
        body = await conn.request("disassemble", {"memoryReference": address, "instructionCount": count})
        return [
            Instruction(
                address=i.get("address", ""),
                bytes=i.get("instructionBytes", "") or "",
                text=i.get("instruction", ""),
                function=(i.get("symbol") or ""),
                line=(i.get("location") or {}).get("line") if i.get("location") else i.get("line"),
            )
            for i in body.get("instructions", [])
        ]

    async def raw_command(self, command: str) -> str:
        frame_id = None
        if self.last_snapshot is not None and self.last_snapshot.threads:
            frames = self.last_snapshot.threads[0].frames
            if frames:
                frame_id = frames[0].id
        result = await self.evaluate(command, frame_id=frame_id, context="repl")
        return result.error if result.error else result.value

    # -- teardown ------------------------------------------------------------------

    async def disconnect(self, terminate: bool = True) -> None:
        if self.conn is not None:
            with contextlib.suppress(DapError, DapRequestError):
                await self.conn.request("disconnect", {"terminateDebuggee": terminate}, timeout=_DISCONNECT_TIMEOUT)
            await self.conn.close()
            self.conn = None
        for task in self._output_pumps:
            task.cancel()
        for process in self._terminal_procs:
            if process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    process.terminate()
        await self.set_state(SessionState.terminated)
        if self.node is not None:
            await self.manager.notify_tree(self.node)


async def spawn_child(parent: DapSession, request: str, configuration: dict) -> DapSession:
    """Service a startDebugging reverse request: create a sibling DapSession
    as a child tree node, inheriting the parent's breakpoints and watches."""
    child = DapSession(
        session_id=f"{parent.session_id}:child:{uuid.uuid4().hex[:8]}",
        adapter=parent.adapter,
        manager=parent.manager,
        snapshot_sink=parent.snapshot_sink,
    )
    child._config = parent._config
    child.breakpoint_specs = {source: list(specs) for source, specs in parent.breakpoint_specs.items()}
    child.function_breakpoint_specs = list(parent.function_breakpoint_specs)
    child._exc_filters = parent._exc_filters
    child.watches = list(parent.watches)
    assert parent.node is not None, "parent session has no tree node"
    node = parent.manager.add_child(parent.node, child, label=str(configuration.get("name", "")))
    child.node = node
    child.session_id = parent.session_id  # snapshots group under the root id
    await child.start_child(request, configuration)
    return child
