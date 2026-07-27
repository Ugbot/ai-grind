"""Unit + integration tests for the DAP client layer.

Covers, bottom-up:
- Content-Length framing (encode_message / read_message).
- DapConnection request/response matching, events, and reverse requests
  over an in-memory transport (no subprocesses).
- The full DapSession lifecycle against the scripted fake adapter
  (tests/fixtures/fake_adapter.py) over a real StdioTransport.
- Adapter-crash detection (AdapterCrashed on pending + subsequent requests).
- DebugSessionManager tree/focus/resolve semantics.
- diff_snapshots deltas.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Literal

import pytest

from devtools_mcp.debug.adapters.base import AdapterSpec
from devtools_mcp.debug.dap_session import DapSession
from devtools_mcp.debug.models import (
    AttachConfig,
    BreakpointSpec,
    BreakpointState,
    DebugSnapshot,
    EvalResult,
    Instruction,
    LaunchConfig,
    Scope,
    SessionState,
    StackFrame,
    ThreadInfo,
    Variable,
    WatchResult,
)
from devtools_mcp.debug.protocol import (
    MAX_MESSAGE_BYTES,
    AdapterCrashed,
    DapConnection,
    DapError,
    DapRequestError,
    DapTransport,
    StdioTransport,
    encode_message,
    read_message,
)
from devtools_mcp.debug.session import DebugSession, DebugSessionManager
from devtools_mcp.debug.snapshot import diff_snapshots
from devtools_mcp.registry import InstalledTool

FAKE_ADAPTER = Path(__file__).parent / "fixtures" / "fake_adapter.py"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_frames(data: bytes | bytearray) -> list[dict]:
    """Decode every complete Content-Length framed message in `data`."""
    view = bytes(data)
    messages: list[dict] = []
    while True:
        idx = view.find(b"\r\n\r\n")
        if idx < 0:
            break
        length = -1
        for line in view[:idx].split(b"\r\n"):
            name, _, value = line.partition(b":")
            if name.strip().lower() == b"content-length":
                length = int(value.strip())
        assert length >= 0, f"frame without Content-Length: {view[:idx]!r}"
        body = view[idx + 4 : idx + 4 + length]
        if len(body) < length:
            break
        messages.append(json.loads(body))
        view = view[idx + 4 + length :]
    return messages


async def wait_for(predicate, timeout: float = 5.0) -> None:
    """Poll until predicate() is truthy; assert on timeout."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        assert loop.time() < deadline, "condition not reached within timeout"
        await asyncio.sleep(0.005)


class MemoryWriter:
    """Duck-typed StreamWriter that captures written bytes."""

    def __init__(self) -> None:
        self.buffer = bytearray()

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


class MemoryTransport(DapTransport):
    """In-memory transport: tests feed `reader`, inspect `writer.buffer`."""

    def __init__(self) -> None:
        self.reader = asyncio.StreamReader()
        self.writer = MemoryWriter()  # type: ignore[assignment]

    async def close(self) -> None:
        return None

    def alive(self) -> bool:
        return True


async def _no_event(event: str, body: dict) -> None:
    return None


async def _no_reverse(command: str, arguments: dict) -> dict:
    raise DapError(f"unexpected reverse request: {command}")


def make_fake_spec(crash_after: str = "") -> AdapterSpec:
    """AdapterSpec that spawns tests/fixtures/fake_adapter.py over stdio."""

    async def transport(config: LaunchConfig | AttachConfig) -> StdioTransport:
        env = {"FAKE_ADAPTER_CRASH_AFTER": crash_after} if crash_after else None
        stdio = StdioTransport([sys.executable, str(FAKE_ADAPTER)], env=env)
        await stdio.start()
        return stdio

    async def detect() -> InstalledTool:
        return InstalledTool(suite="debug", name="fake", path=sys.executable, version="1", available=True)

    return AdapterSpec(
        name="fake",
        languages=("fake",),
        transport=transport,
        launch_template=lambda config: {"program": config.program},
        attach_template=lambda config: {},
        detect=detect,
        sniff=lambda program: 0,
    )


# ---------------------------------------------------------------------------
# Framing: encode_message / read_message
# ---------------------------------------------------------------------------


async def test_round_trip_single_message():
    payload = {"seq": 1, "type": "request", "command": "initialize", "arguments": {"locale": "en"}}
    reader = asyncio.StreamReader()
    reader.feed_data(encode_message(payload))
    assert await read_message(reader) == payload


async def test_round_trip_split_across_feeds():
    payload = {"seq": 2, "type": "event", "event": "output", "body": {"output": "héllo\n" * 10}}
    data = encode_message(payload)
    reader = asyncio.StreamReader()
    task = asyncio.ensure_future(read_message(reader))
    # Split mid-header, then mid-body.
    reader.feed_data(data[:7])
    await asyncio.sleep(0.01)
    assert not task.done()
    reader.feed_data(data[7:-5])
    await asyncio.sleep(0.01)
    assert not task.done()
    reader.feed_data(data[-5:])
    assert await asyncio.wait_for(task, timeout=5) == payload


async def test_multiple_messages_in_one_buffer():
    first = {"seq": 1, "type": "response", "request_seq": 1, "command": "a", "success": True}
    second = {"seq": 2, "type": "event", "event": "stopped", "body": {"threadId": 1}}
    reader = asyncio.StreamReader()
    reader.feed_data(encode_message(first) + encode_message(second))
    assert await read_message(reader) == first
    assert await read_message(reader) == second


async def test_oversized_content_length_raises():
    reader = asyncio.StreamReader()
    reader.feed_data(b"Content-Length: %d\r\n\r\n" % (MAX_MESSAGE_BYTES + 1))
    with pytest.raises(DapError, match="too large"):
        await read_message(reader)


async def test_missing_content_length_raises():
    reader = asyncio.StreamReader()
    reader.feed_data(b"X-Whatever: 1\r\n\r\n")
    with pytest.raises(DapError, match="missing Content-Length"):
        await read_message(reader)


async def test_eof_mid_header_raises():
    reader = asyncio.StreamReader()
    reader.feed_data(b"Content-Len")
    reader.feed_eof()
    with pytest.raises(asyncio.IncompleteReadError):
        await read_message(reader)


def test_encode_message_rejects_oversized_payload():
    huge = {"data": "x" * (MAX_MESSAGE_BYTES + 1)}
    with pytest.raises(AssertionError):
        encode_message(huge)


# ---------------------------------------------------------------------------
# DapConnection over an in-memory transport
# ---------------------------------------------------------------------------


async def test_out_of_order_responses_resolve_right_futures():
    transport = MemoryTransport()
    conn = DapConnection(transport, _no_event, _no_reverse)
    conn.start()
    try:
        first = asyncio.ensure_future(conn.request("alpha"))
        second = asyncio.ensure_future(conn.request("beta"))
        await wait_for(lambda: len(parse_frames(transport.writer.buffer)) == 2)
        sent = parse_frames(transport.writer.buffer)
        assert [m["command"] for m in sent] == ["alpha", "beta"]
        seq_alpha, seq_beta = sent[0]["seq"], sent[1]["seq"]
        assert seq_alpha != seq_beta

        # Answer beta first — it must resolve while alpha stays pending.
        transport.reader.feed_data(
            encode_message(
                {
                    "seq": 1,
                    "type": "response",
                    "request_seq": seq_beta,
                    "command": "beta",
                    "success": True,
                    "body": {"tag": "B"},
                }
            )
        )
        assert (await asyncio.wait_for(second, timeout=5))["tag"] == "B"
        assert not first.done()
        transport.reader.feed_data(
            encode_message(
                {
                    "seq": 2,
                    "type": "response",
                    "request_seq": seq_alpha,
                    "command": "alpha",
                    "success": True,
                    "body": {"tag": "A"},
                }
            )
        )
        assert (await asyncio.wait_for(first, timeout=5))["tag"] == "A"
    finally:
        await conn.close()


async def test_success_false_raises_dap_request_error():
    transport = MemoryTransport()
    conn = DapConnection(transport, _no_event, _no_reverse)
    conn.start()
    try:
        future = asyncio.ensure_future(conn.request("evaluate", {"expression": "x"}))
        await wait_for(lambda: len(parse_frames(transport.writer.buffer)) == 1)
        seq = parse_frames(transport.writer.buffer)[0]["seq"]
        transport.reader.feed_data(
            encode_message(
                {
                    "seq": 1,
                    "type": "response",
                    "request_seq": seq,
                    "command": "evaluate",
                    "success": False,
                    "message": "name 'x' is not defined",
                    "body": {"error": {"id": 17}},
                }
            )
        )
        with pytest.raises(DapRequestError) as excinfo:
            await asyncio.wait_for(future, timeout=5)
        assert excinfo.value.command == "evaluate"
        assert "not defined" in excinfo.value.message
        assert excinfo.value.body == {"error": {"id": 17}}
    finally:
        await conn.close()


async def test_event_dispatches_to_handler():
    events: list[tuple[str, dict]] = []

    async def on_event(name: str, body: dict) -> None:
        events.append((name, body))

    transport = MemoryTransport()
    conn = DapConnection(transport, on_event, _no_reverse)
    conn.start()
    try:
        transport.reader.feed_data(
            encode_message({"seq": 5, "type": "event", "event": "output", "body": {"output": "hi"}})
        )
        await wait_for(lambda: events)
        assert events == [("output", {"output": "hi"})]
    finally:
        await conn.close()


async def test_reverse_request_dispatches_and_responds():
    seen: list[tuple[str, dict]] = []

    async def on_reverse(command: str, arguments: dict) -> dict:
        seen.append((command, arguments))
        return {"processId": 4242}

    transport = MemoryTransport()
    conn = DapConnection(transport, _no_event, on_reverse)
    conn.start()
    try:
        transport.reader.feed_data(
            encode_message({"seq": 9, "type": "request", "command": "runInTerminal", "arguments": {"args": ["prog"]}})
        )
        await wait_for(lambda: len(parse_frames(transport.writer.buffer)) == 1)
        assert seen == [("runInTerminal", {"args": ["prog"]})]
        response = parse_frames(transport.writer.buffer)[0]
        assert response["type"] == "response"
        assert response["request_seq"] == 9
        assert response["command"] == "runInTerminal"
        assert response["success"] is True
        assert response["body"] == {"processId": 4242}
    finally:
        await conn.close()


async def test_reverse_request_handler_failure_sends_error_response():
    async def on_reverse(command: str, arguments: dict) -> dict:
        raise RuntimeError("boom")

    transport = MemoryTransport()
    conn = DapConnection(transport, _no_event, on_reverse)
    conn.start()
    try:
        transport.reader.feed_data(
            encode_message({"seq": 3, "type": "request", "command": "startDebugging", "arguments": {}})
        )
        await wait_for(lambda: len(parse_frames(transport.writer.buffer)) == 1)
        response = parse_frames(transport.writer.buffer)[0]
        assert response["request_seq"] == 3
        assert response["success"] is False
        assert "boom" in response["message"]
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# DapSession lifecycle against the fake adapter (real subprocess + stdio)
# ---------------------------------------------------------------------------


async def test_dap_session_full_lifecycle():
    manager = DebugSessionManager()
    session = DapSession("fake-1", make_fake_spec(), manager)
    session.node = manager.register_root(session)
    try:
        assert session.add_watch("x + 1") is None
        # Pre-launch breakpoint: stored, applied at the initialized event.
        pre = await session.set_breakpoints("/tmp/fake.py", [BreakpointSpec(source="/tmp/fake.py", line=3)])
        assert len(pre) == 1 and not pre[0].verified

        await session.launch(LaunchConfig(program="/tmp/fake.py"))
        caps = session.capabilities
        assert caps.conditional_breakpoints
        assert caps.set_variable
        assert caps.exception_filters == ["uncaught"]

        state = await session.wait_until({SessionState.stopped}, timeout=10)
        assert state == SessionState.stopped

        snapshot = session.last_snapshot
        assert snapshot is not None
        assert snapshot.stop_seq == 1
        assert snapshot.stop_reason == "breakpoint"
        assert snapshot.thread_id == 1
        assert snapshot.hit_breakpoint_ids == [1]

        # Threads + frames from the stop walk.
        assert [t.thread_id for t in snapshot.threads] == [1]
        frames = snapshot.threads[0].frames
        assert [f.function for f in frames] == ["inner", "main"]
        assert frames[0].file == "/tmp/fake.py" and frames[0].line == 3

        # Variables: flattened Locals, nested child expanded by path.
        values = {v.path: v.value for v in snapshot.variables}
        assert values["x"] == "1"
        assert values["obj.field"] == "42"
        assert all(v.scope == "locals" for v in snapshot.variables)
        nested = next(v for v in snapshot.variables if v.path == "obj.field")
        assert nested.depth == 1

        # Watch was evaluated via the adapter's evaluate echo.
        assert len(snapshot.watches) == 1
        assert snapshot.watches[0].expression == "x + 1"
        assert snapshot.watches[0].value == "eval:x + 1"
        assert not snapshot.watches[0].error

        # Diff machinery ran: first stop diffs against nothing.
        assert snapshot.changes == []

        # Breakpoints were sent during the configuration dance and verified.
        confirmed = session.breakpoints["/tmp/fake.py"]
        assert len(confirmed) == 1 and confirmed[0].verified and confirmed[0].id == 1
        assert snapshot.breakpoints and snapshot.breakpoints[0].verified

        await session.continue_()
        state = await session.wait_until({SessionState.terminated}, timeout=10)
        assert state == SessionState.terminated
    finally:
        await session.disconnect(terminate=True)


async def test_adapter_crash_fails_pending_and_subsequent_requests():
    transport = StdioTransport(
        [sys.executable, str(FAKE_ADAPTER)],
        env={"FAKE_ADAPTER_CRASH_AFTER": "threads"},
    )
    await transport.start()
    conn = DapConnection(transport, _no_event, _no_reverse)
    conn.start()
    try:
        body = await conn.request("initialize", {"adapterID": "fake"})
        assert body.get("supportsConfigurationDoneRequest") is True

        # `threads` gets a response and then the adapter exits(1); the
        # stackTrace request queued behind it is left pending forever.
        answered = asyncio.ensure_future(conn.request("threads", timeout=10))
        orphaned = asyncio.ensure_future(conn.request("stackTrace", {"threadId": 1}, timeout=10))
        assert (await answered)["threads"][0]["id"] == 1
        with pytest.raises(AdapterCrashed):
            await orphaned

        # Once crashed, new requests fail immediately with the same error.
        with pytest.raises(AdapterCrashed):
            await conn.request("scopes", {"frameId": 1})
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# DebugSessionManager: trees, focus, resolve, teardown
# ---------------------------------------------------------------------------


class DummySession(DebugSession):
    """Minimal concrete DebugSession for manager tests."""

    def __init__(self, session_id: str) -> None:
        super().__init__(session_id=session_id, adapter_name="dummy")
        self.disconnect_calls: list[bool] = []

    async def launch(self, config: LaunchConfig) -> None:
        return None

    async def attach(self, config: AttachConfig) -> None:
        return None

    async def set_breakpoints(self, source: str, bps: list[BreakpointSpec]) -> list[BreakpointState]:
        return []

    async def set_function_breakpoints(self, bps: list[BreakpointSpec]) -> list[BreakpointState]:
        return []

    async def set_exception_breakpoints(self, filters: list[str]) -> None:
        return None

    async def continue_(self, thread_id: int | None = None) -> None:
        return None

    async def pause(self, thread_id: int | None = None) -> None:
        return None

    async def step(
        self,
        kind: Literal["over", "into", "out"],
        thread_id: int | None = None,
        granularity: Literal["statement", "instruction"] = "statement",
    ) -> None:
        return None

    async def threads(self) -> list[ThreadInfo]:
        return []

    async def stack_trace(self, thread_id: int, start: int = 0, levels: int = 32) -> list[StackFrame]:
        return []

    async def scopes(self, frame_id: int) -> list[Scope]:
        return []

    async def variables(self, container_ref: int, start: int = 0, count: int = 100) -> list[Variable]:
        return []

    async def evaluate(self, expression: str, frame_id: int | None = None, context: str = "repl") -> EvalResult:
        return EvalResult()

    async def set_variable(self, container_ref: int, name: str, value: str) -> Variable:
        return Variable(path=name, name=name)

    async def read_memory(self, address: str, count: int) -> bytes:
        return b""

    async def disassemble(self, address: str, count: int) -> list[Instruction]:
        return []

    async def raw_command(self, command: str) -> str:
        return ""

    async def disconnect(self, terminate: bool = True) -> None:
        self.disconnect_calls.append(terminate)
        await self.set_state(SessionState.terminated)


async def test_manager_register_root_and_child_ids():
    manager = DebugSessionManager()
    root_session = DummySession("root")
    root = manager.register_root(root_session)
    assert root.node_id == "root"
    assert manager.resolve("root") is root

    child_one = manager.add_child(root, DummySession("root-child-a"), label="worker")
    child_two = manager.add_child(root, DummySession("root-child-b"))
    assert child_one.node_id == "root/1"
    assert child_two.node_id == "root/2"
    assert child_one.parent is root
    assert child_one.label == "worker"
    assert [n.node_id for n in root.walk()] == ["root", "root/1", "root/2"]
    assert [n.node_id for n in manager.children_of("root")] == ["root/1", "root/2"]


async def test_manager_focus_follows_stops_and_resolve_selectors():
    manager = DebugSessionManager()
    root = manager.register_root(DummySession("root"))
    child = manager.add_child(root, DummySession("child"))

    # Default focus is the root.
    assert manager.resolve("root") is root
    # A stop moves focus to the stopped node.
    manager.on_stopped(child)
    assert manager.resolve("root") is child
    # Explicit child selectors: short and fully-qualified forms.
    assert manager.resolve("root", child="1") is child
    assert manager.resolve("root", child="root/1") is child
    with pytest.raises(KeyError):
        manager.resolve("root", child="99")
    with pytest.raises(KeyError):
        manager.resolve("nope")


async def test_manager_stop_tree_disconnects_leaves_first():
    manager = DebugSessionManager()
    order: list[str] = []

    class OrderedSession(DummySession):
        async def disconnect(self, terminate: bool = True) -> None:
            order.append(self.session_id)
            await super().disconnect(terminate)

    root_session, child_session = OrderedSession("root"), OrderedSession("kid")
    root = manager.register_root(root_session)
    manager.add_child(root, child_session)

    count = await manager.stop_tree("root")
    assert count == 2
    assert order == ["kid", "root"]  # leaves first
    assert root_session.disconnect_calls == [True]
    assert "root" not in manager.trees
    with pytest.raises(KeyError):
        manager.resolve("root")
    # Stopping again is a no-op.
    assert await manager.stop_tree("root") == 0


# ---------------------------------------------------------------------------
# diff_snapshots
# ---------------------------------------------------------------------------


def _snapshot(variables: list[Variable], watches: list[WatchResult]) -> DebugSnapshot:
    return DebugSnapshot(
        run_id="r",
        tool="stop",
        binary="prog",
        variables=variables,
        watches=watches,
    )


def test_diff_snapshots_first_stop_is_empty():
    cur = _snapshot([Variable(path="x", name="x", value="1", scope="locals")], [])
    assert diff_snapshots(None, cur) == []


def test_diff_snapshots_added_changed_removed_and_watches():
    prev = _snapshot(
        [
            Variable(path="x", name="x", value="1", scope="locals"),
            Variable(path="y", name="y", value="2", scope="locals"),
            Variable(path="gone", name="gone", value="9", scope="locals"),
        ],
        [
            WatchResult(expression="w", value="10"),
            WatchResult(expression="bad", value="", error="NameError"),
        ],
    )
    cur = _snapshot(
        [
            Variable(path="x", name="x", value="1", scope="locals"),  # unchanged
            Variable(path="y", name="y", value="3", scope="locals"),  # changed
            Variable(path="z", name="z", value="4", scope="locals"),  # added
        ],
        [WatchResult(expression="w", value="20")],  # watch changed
    )
    changes = {(c.path, c.kind): c for c in diff_snapshots(prev, cur)}
    assert ("x", "changed") not in changes and ("x", "added") not in changes
    assert changes[("y", "changed")].old == "2" and changes[("y", "changed")].new == "3"
    assert changes[("z", "added")].new == "4"
    assert changes[("gone", "removed")].old == "9"
    assert changes[("w", "changed")].old == "10" and changes[("w", "changed")].new == "20"
    # Errored watches never participate in the diff.
    assert not any(path == "bad" for path, _ in changes)
    assert len(changes) == 4


def test_diff_snapshots_same_path_different_scope_is_distinct():
    prev = _snapshot([Variable(path="x", name="x", value="1", scope="locals")], [])
    cur = _snapshot([Variable(path="x", name="x", value="1", scope="globals")], [])
    kinds = {(c.path, c.kind) for c in diff_snapshots(prev, cur)}
    assert kinds == {("x", "added"), ("x", "removed")}
