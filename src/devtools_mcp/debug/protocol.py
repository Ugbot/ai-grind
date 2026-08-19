"""DAP wire layer: Content-Length framing, request futures, event dispatch,
reverse requests, transports.

This is the only module that knows the Debug Adapter Protocol's message
encoding. The framing helpers are protocol-name-agnostic on purpose: LSP
uses the identical Content-Length framing, so jdtls.py reuses them.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any

# Bounds.
MAX_MESSAGE_BYTES = 16 * 1024 * 1024  # fail loud on anything bigger
MAX_PENDING_REQUESTS = 128
MAX_STDERR_LINES = 200
_HEADER_TERMINATOR = b"\r\n\r\n"


class DapError(Exception):
    """Base error for the DAP layer."""


class DapRequestError(DapError):
    """The adapter answered a request with success=false."""

    def __init__(self, command: str, message: str, body: dict | None = None) -> None:
        super().__init__(f"{command}: {message}")
        self.command = command
        self.message = message
        self.body = body or {}


class AdapterCrashed(DapError):
    """The adapter process died (or the socket closed) mid-session."""

    def __init__(self, returncode: int | None, stderr_tail: str) -> None:
        detail = f"adapter exited with code {returncode}" if returncode is not None else "adapter connection closed"
        if stderr_tail:
            detail += f"\nstderr tail:\n{stderr_tail}"
        super().__init__(detail)
        self.returncode = returncode
        self.stderr_tail = stderr_tail


def encode_message(payload: dict) -> bytes:
    """Encode one protocol message with Content-Length framing."""
    body = json.dumps(payload).encode("utf-8")
    assert len(body) <= MAX_MESSAGE_BYTES, f"outgoing message too large: {len(body)} bytes"
    return b"Content-Length: %d\r\n\r\n%b" % (len(body), body)


async def read_message(reader: asyncio.StreamReader) -> dict:
    """Read one Content-Length framed JSON message. Raises on EOF/overflow."""
    header = await reader.readuntil(_HEADER_TERMINATOR)
    length = -1
    for raw_line in header.split(b"\r\n"):
        name, _, value = raw_line.partition(b":")
        if name.strip().lower() == b"content-length":
            length = int(value.strip())
    if length < 0:
        raise DapError(f"missing Content-Length header: {header!r}")
    if length > MAX_MESSAGE_BYTES:
        raise DapError(f"incoming message too large: {length} bytes (max {MAX_MESSAGE_BYTES})")
    body = await reader.readexactly(length)
    return json.loads(body)


class DapTransport:
    """A byte pipe to an adapter: stdio subprocess or TCP socket."""

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    async def close(self) -> None:
        raise NotImplementedError

    def returncode(self) -> int | None:
        """Exit code if the underlying process died, else None."""
        return None

    def stderr_tail(self) -> str:
        return ""

    def alive(self) -> bool:
        raise NotImplementedError


class StdioTransport(DapTransport):
    """Spawn an adapter process and talk DAP over its stdin/stdout."""

    def __init__(self, argv: list[str], cwd: str | None = None, env: dict[str, str] | None = None) -> None:
        assert argv, "adapter argv must not be empty"
        self.argv = argv
        self.cwd = cwd or None
        self.env = env
        self.process: asyncio.subprocess.Process | None = None
        self._stderr_lines: deque[str] = deque(maxlen=MAX_STDERR_LINES)
        self._stderr_task: asyncio.Task | None = None

    async def start(self) -> None:
        full_env = dict(os.environ)
        if self.env:
            full_env.update(self.env)
        self.process = await asyncio.create_subprocess_exec(
            *self.argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.cwd,
            env=full_env,
        )
        assert self.process.stdout is not None and self.process.stdin is not None
        self.reader = self.process.stdout
        self.writer = self.process.stdin
        self._stderr_task = asyncio.ensure_future(self._drain_stderr())

    async def _drain_stderr(self) -> None:
        assert self.process is not None and self.process.stderr is not None
        try:
            while True:
                line = await self.process.stderr.readline()
                if not line:
                    return
                self._stderr_lines.append(line.decode("utf-8", errors="replace").rstrip())
        except (asyncio.CancelledError, OSError):
            return

    def returncode(self) -> int | None:
        return self.process.returncode if self.process else None

    def stderr_tail(self) -> str:
        return "\n".join(list(self._stderr_lines)[-20:])

    def alive(self) -> bool:
        return self.process is not None and self.process.returncode is None

    async def close(self) -> None:
        if self._stderr_task is not None:
            self._stderr_task.cancel()
        if self.process is None:
            return
        if self.process.returncode is None:
            try:
                self.process.terminate()
                await asyncio.wait_for(self.process.wait(), timeout=3.0)
            except (TimeoutError, ProcessLookupError):
                with contextlib.suppress(ProcessLookupError):
                    self.process.kill()


class SocketTransport(DapTransport):
    """Connect to an adapter listening on host:port (js-debug's DAP server,
    jdt.ls-provided java-debug ports)."""

    def __init__(self, host: str, port: int, connect_timeout: float = 10.0) -> None:
        assert port > 0, f"bad port {port}"
        self.host = host or "127.0.0.1"
        self.port = port
        self.connect_timeout = connect_timeout
        self._closed = False

    async def start(self) -> None:
        deadline = asyncio.get_event_loop().time() + self.connect_timeout
        delay = 0.05
        while True:
            try:
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
                return
            except OSError:
                if asyncio.get_event_loop().time() + delay > deadline:
                    raise
                await asyncio.sleep(delay)
                delay = min(delay * 2, 1.0)

    def alive(self) -> bool:
        return not self._closed and not self.writer.is_closing()

    async def close(self) -> None:
        self._closed = True
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except (OSError, AttributeError):
            pass


EventHandler = Callable[[str, dict], Awaitable[None]]
ReverseHandler = Callable[[str, dict], Awaitable[dict]]


class DapConnection:
    """One DAP conversation over a transport.

    - request(): send a request, await the matching response as a future.
    - Events are dispatched to on_event.
    - Requests FROM the adapter (reverse requests: runInTerminal,
      startDebugging) are dispatched to on_reverse_request; its return value
      is sent back as the response body.
    """

    def __init__(
        self,
        transport: DapTransport,
        on_event: EventHandler,
        on_reverse_request: ReverseHandler,
    ) -> None:
        self.transport = transport
        self.on_event = on_event
        self.on_reverse_request = on_reverse_request
        self._seq = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._pump_task: asyncio.Task | None = None
        self._side_tasks: set[asyncio.Task] = set()
        self._write_lock = asyncio.Lock()
        self._closed = False

    def start(self) -> None:
        assert self._pump_task is None, "connection already started"
        self._pump_task = asyncio.ensure_future(self._pump())

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    async def _send(self, payload: dict) -> None:
        data = encode_message(payload)
        async with self._write_lock:
            self.transport.writer.write(data)
            await self.transport.writer.drain()

    async def request(self, command: str, arguments: dict | None = None, timeout: float = 30.0) -> dict:
        """Send a request, return the response body. Raises DapRequestError on
        success=false, AdapterCrashed if the adapter dies while we wait."""
        if self._closed:
            raise AdapterCrashed(self.transport.returncode(), self.transport.stderr_tail())
        assert len(self._pending) < MAX_PENDING_REQUESTS, "too many in-flight DAP requests"
        seq = self._next_seq()
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[seq] = future
        payload: dict[str, Any] = {"seq": seq, "type": "request", "command": command}
        if arguments is not None:
            payload["arguments"] = arguments
        try:
            await self._send(payload)
            response = await asyncio.wait_for(future, timeout=timeout)
        except TimeoutError:
            raise DapError(f"{command}: no response within {timeout}s") from None
        finally:
            self._pending.pop(seq, None)
        if not response.get("success", False):
            message = response.get("message", "request failed")
            raise DapRequestError(command, message, response.get("body"))
        return response.get("body") or {}

    async def _respond(self, request: dict, body: dict | None, success: bool = True, message: str = "") -> None:
        payload: dict[str, Any] = {
            "seq": self._next_seq(),
            "type": "response",
            "request_seq": request["seq"],
            "command": request.get("command", ""),
            "success": success,
        }
        if body is not None:
            payload["body"] = body
        if message:
            payload["message"] = message
        await self._send(payload)

    async def _pump(self) -> None:
        try:
            while True:
                message = await read_message(self.transport.reader)
                kind = message.get("type")
                if kind == "response":
                    future = self._pending.get(message.get("request_seq", -1))
                    if future is not None and not future.done():
                        future.set_result(message)
                elif kind == "event":
                    self._spawn(self.on_event(message.get("event", ""), message.get("body") or {}))
                elif kind == "request":
                    self._spawn(self._handle_reverse(message))
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError, OSError):
            self._fail_pending()
        except asyncio.CancelledError:
            raise
        except DapError:
            self._fail_pending()

    def _spawn(self, coro: Awaitable[None]) -> None:
        task = asyncio.ensure_future(coro)
        self._side_tasks.add(task)
        task.add_done_callback(self._side_tasks.discard)

    async def _handle_reverse(self, request: dict) -> None:
        command = request.get("command", "")
        try:
            body = await self.on_reverse_request(command, request.get("arguments") or {})
            await self._respond(request, body, success=True)
        except Exception as exc:  # noqa: BLE001  # report to adapter, don't die
            with contextlib.suppress(OSError, DapError):
                await self._respond(request, None, success=False, message=str(exc))

    def _fail_pending(self) -> None:
        self._closed = True
        crash = AdapterCrashed(self.transport.returncode(), self.transport.stderr_tail())
        for future in self._pending.values():
            if not future.done():
                future.set_exception(crash)
        self._pending.clear()

    async def close(self) -> None:
        self._closed = True
        if self._pump_task is not None:
            self._pump_task.cancel()
        for task in list(self._side_tasks):
            task.cancel()
        self._fail_pending()
        await self.transport.close()
