"""Small bounded native producer harness. Python controls processes/telemetry only."""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import os
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import MODE, HarnessLimits, arguments, configuration, control

MAX_ACTIVE, MAX_RETAINED, MAX_LINE, MAX_RECEIPTS = 4, 16, 65536, 8


def error(code: str, detail: str, **extra: Any) -> dict[str, Any]:
    return {
        "status": "error",
        "code": code,
        "detail": detail[:512],
        "mode": MODE,
        "durable": False,
        **extra,
    }


def finite_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value):
        raise ValueError("non-finite JSON number")
    return value


def reject_constant(text: str) -> None:
    raise ValueError("nonstandard JSON constant: " + text)


@dataclass
class Run:
    run_id: str
    session_id: str
    config: dict[str, Any]
    limits: dict[str, int]
    process: asyncio.subprocess.Process | None = None
    state: str = "starting"
    status: dict[str, Any] | None = None
    final: dict[str, Any] | None = None
    book: dict[str, Any] | None = None
    definition: dict[str, Any] | None = None
    receipts: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=MAX_RECEIPTS))
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    finished: asyncio.Event = field(default_factory=asyncio.Event)
    readers: list[asyncio.Task[None]] = field(default_factory=list)
    watcher: asyncio.Task[None] | None = None
    creation: asyncio.Task[None] | None = None
    force_task: asyncio.Task[None] | None = None
    stop_requested: bool = False
    pending: asyncio.Future[dict[str, Any]] | None = None
    pending_command: str = ""
    busy: bool = False
    revision: int = 0
    malformed_lines: int = 0
    dropped_diagnostics: int = 0
    definition_count: int = 0
    stderr_bytes: int = 0
    discarded_output_bytes: int = 0
    stderr_tail: bytes = b""
    failure: str = ""

    def snapshot(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "session_id": self.session_id,
            "state": self.state,
            "mode": MODE,
            "synthetic": True,
            "durable": False,
            "platform_job": False,
            "config": dict(self.config),
            "limits": dict(self.limits),
            "telemetry": self.status,
            "final": self.final,
            "book": self.book,
            "definition": self.definition,
            "definition_count": self.definition_count,
            "command_receipts": list(self.receipts),
            "pending_command": self.pending_command,
            "malformed_lines": self.malformed_lines,
            "dropped_diagnostics": self.dropped_diagnostics,
            "stderr_bytes": self.stderr_bytes,
            "discarded_output_bytes": self.discarded_output_bytes,
            "stderr_tail": self.stderr_tail.decode("utf-8", "replace"),
            "failure": self.failure,
            "exit_code": self.process.returncode if self.process else None,
        }


class MarketFeedManager:
    def __init__(
        self,
        binary: str | Path | None,
        *,
        start_timeout: float = 5.0,
        command_timeout: float = 2.0,
        stop_timeout: float = 2.0,
        limits: HarnessLimits | None = None,
    ) -> None:
        self.binary = Path(binary).resolve() if binary and Path(binary).is_absolute() else None
        self.start_timeout, self.command_timeout, self.stop_timeout = (
            start_timeout,
            command_timeout,
            stop_timeout,
        )
        self.limits = limits or HarnessLimits.from_env()
        self.runs: dict[str, Run] = {}
        self.closed = False

    def _fail(self, run: Run, reason: str) -> None:
        run.failure = reason[:512]
        run.ready.set()
        if run.pending and not run.pending.done():
            run.pending.set_result({"ok": False, "error": run.failure})
        if run.process and run.process.returncode is None and (run.force_task is None or run.force_task.done()):
            run.force_task = asyncio.create_task(self._terminate_process(run))

    async def _stdout(self, run: Run) -> None:
        assert run.process and run.process.stdout
        stream = run.process.stdout
        while True:
            try:
                line = await stream.readline()
            except ValueError:
                self._fail(run, "protocol_line_limit")
                await self._discard_stdout(run, stream)
                return
            if not line:
                return
            if len(line) > MAX_LINE:
                self._fail(run, "protocol_line_limit")
                await self._discard_stdout(run, stream)
                return
            try:
                item = json.loads(line, parse_float=finite_float, parse_constant=reject_constant)
                if not isinstance(item, dict) or item.get("type") not in {
                    "status",
                    "command",
                    "book",
                    "definition",
                    "final",
                    "error",
                }:
                    raise ValueError("unknown record type")
            except (ValueError, UnicodeError, RecursionError):
                run.malformed_lines += 1
                run.dropped_diagnostics += 1
                if run.malformed_lines >= 8:
                    self._fail(run, "protocol_malformed_output")
                    await self._discard_stdout(run, stream)
                    return
                continue
            if type(item.get("schema_version")) is not int or item["schema_version"] != 1:
                self._fail(run, "protocol_schema_version")
                await self._discard_stdout(run, stream)
                return
            kind = item["type"]
            if kind == "command":
                revision, command, ok = (
                    item.get("revision"),
                    item.get("command"),
                    item.get("ok"),
                )
                if (
                    type(revision) is not int
                    or not run.revision < revision <= 0xFFFFFFFFFFFFFFFF
                    or type(ok) is not bool
                ):
                    self._fail(run, "protocol_command_revision")
                    await self._discard_stdout(run, stream)
                    return
                if command != run.pending_command:
                    self._fail(run, "protocol_command_mismatch")
                    await self._discard_stdout(run, stream)
                    return
                run.revision = revision
                receipt = {
                    "command": command,
                    "revision": revision,
                    "ok": ok,
                    "error": str(item.get("error", ""))[:256],
                }
                if len(run.receipts) == MAX_RECEIPTS:
                    run.dropped_diagnostics += 1
                run.receipts.append(receipt)
                if run.pending and not run.pending.done():
                    run.pending.set_result(receipt)
                run.pending_command, run.pending = "", None
            elif kind == "status":
                run.status = item
                run.ready.set()
            elif kind == "final":
                run.final = item
            elif kind == "book":
                run.book = item
            elif kind == "definition":
                run.definition_count += 1
                if run.definition is None or item.get("scope") == "feed":
                    run.definition = item
                else:
                    run.dropped_diagnostics += 1
            else:
                self._fail(
                    run,
                    "native_error: " + str(item.get("error", item.get("detail", "unknown")))[:256],
                )
                await self._discard_stdout(run, stream)
                return

    async def _discard_stdout(self, run: Run, stream: asyncio.StreamReader) -> None:
        # Only terminal telemetry refusal reaches here. Draining lets the
        # already-requested TERM/KILL reap even if stdout was pipe-buffer-full.
        while chunk := await stream.read(4096):
            run.discarded_output_bytes += len(chunk)

    async def _stderr(self, run: Run) -> None:
        assert run.process and run.process.stderr
        while chunk := await run.process.stderr.read(4096):
            run.stderr_bytes += len(chunk)
            run.stderr_tail = (run.stderr_tail + chunk)[-4096:]

    async def _terminate_process(self, run: Run) -> None:
        process = run.process
        if process is None or process.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), self.stop_timeout)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            await asyncio.wait_for(process.wait(), self.stop_timeout)

    async def _force(self, run: Run) -> None:
        if run.force_task is None:
            run.force_task = asyncio.create_task(self._terminate_process(run))
        await asyncio.shield(run.force_task)

    async def _spawn(self, run: Run) -> None:
        # This task owns creation even when the originating MCP request is
        # cancelled before asyncio hands the Process object back to it.
        try:
            run.process = await asyncio.create_subprocess_exec(
                str(self.binary),
                *arguments(run.config),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=MAX_LINE,
            )
        except OSError as exc:
            run.failure, run.state = str(exc)[:512], "failed"
            run.ready.set()
            run.finished.set()
            return
        run.readers = [asyncio.create_task(self._stdout(run)), asyncio.create_task(self._stderr(run))]
        run.watcher = asyncio.create_task(self._watch(run))
        if run.stop_requested:
            assert run.process.stdin
            run.process.stdin.close()
            try:
                await asyncio.wait_for(run.finished.wait(), self.stop_timeout)
            except TimeoutError:
                run.failure = run.failure or "cancelled_start"
                await self._force(run)
                await asyncio.wait_for(run.finished.wait(), self.stop_timeout + 1)

    async def _watch(self, run: Run) -> None:
        assert run.process
        try:
            await asyncio.wait_for(run.process.wait(), run.config["seconds"] + self.start_timeout + 2)
        except TimeoutError:
            run.failure = "runtime_timeout"
            await self._force(run)
        if run.force_task is not None:
            await asyncio.shield(run.force_task)
        try:
            await asyncio.wait_for(asyncio.gather(*run.readers), self.stop_timeout)
        except TimeoutError:
            run.failure = run.failure or "protocol_pipe_eof_timeout"
        run.state = "failed" if run.failure or run.process.returncode != 0 or run.final is None else "finished"
        if run.pending and not run.pending.done():
            run.pending.set_result({"ok": False, "error": "process_exited"})
        run.ready.set()
        run.finished.set()

    async def start(self, values: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            config = configuration(values, self.limits)
        except ValueError as exc:
            return error("invalid_config", str(exc))
        if self.closed:
            return error("closed", "harness is shutting down")
        if self.binary is None or not self.binary.is_file() or not os.access(self.binary, os.X_OK):
            return error(
                "binary_unavailable",
                "set DEVTOOLS_MARKET_FEED_BINARY to the exact absolute native executable",
            )
        if sum(not run.finished.is_set() for run in self.runs.values()) >= MAX_ACTIVE:
            return error("active_limit", "at most four native producers may run")
        while len(self.runs) >= MAX_RETAINED:
            oldest = next(key for key, run in self.runs.items() if run.finished.is_set())
            del self.runs[oldest]
        run = Run(
            uuid.uuid4().hex,
            uuid.uuid4().hex,
            config,
            {"max_memory_mib": self.limits.max_memory_mib, "max_seconds": self.limits.max_seconds},
        )
        self.runs[run.run_id] = run
        run.creation = asyncio.create_task(self._spawn(run))
        try:
            await asyncio.shield(run.creation)
            if run.process is None:
                return error("spawn_failed", run.failure, data=run.snapshot())
            await asyncio.wait_for(run.ready.wait(), self.start_timeout)
        except asyncio.CancelledError:
            run.stop_requested = True
            await asyncio.shield(self.stop(run.run_id))
            raise
        except TimeoutError:
            run.failure = "startup_timeout"
            await self._force(run)
            await asyncio.wait_for(run.finished.wait(), self.stop_timeout + 1)
            return error(
                "startup_timeout",
                "native producer did not emit initial status",
                data=run.snapshot(),
            )
        if run.failure or run.status is None:
            await self._force(run)
            await asyncio.wait_for(run.finished.wait(), self.stop_timeout + 1)
            return error(
                "startup_failed",
                run.failure or "no initial native status",
                data=run.snapshot(),
            )
        if not run.finished.is_set():
            run.state = "running"
        return {"status": "ok", "data": run.snapshot()}

    async def control(self, run_id: str, command: str) -> dict[str, Any]:
        run = self.runs.get(run_id)
        if run is None:
            return error("not_found", "unknown run ID")
        try:
            command = control(command, run.config["symbols"], self.limits)
        except ValueError as exc:
            return error("invalid_command", str(exc))
        if run.finished.is_set() or run.process is None:
            return error("not_running", "native producer already exited", data=run.snapshot())
        if run.busy or run.pending_command:
            return error(
                "command_pending",
                "wait for the exact pending command receipt",
                data=run.snapshot(),
            )
        assert run.process.stdin
        run.busy = True
        run.pending_command = command
        pending = run.pending = asyncio.get_running_loop().create_future()
        try:
            run.process.stdin.write((command + "\n").encode("ascii"))
            await asyncio.wait_for(run.process.stdin.drain(), self.command_timeout)
            receipt = await asyncio.wait_for(asyncio.shield(pending), self.command_timeout)
            if not receipt["ok"]:
                return error(
                    "protocol_error" if run.failure.startswith("protocol_") else "command_refused",
                    receipt.get("error", "native command refused"),
                    data=run.snapshot(),
                )
            return {"status": "ok", "receipt": receipt, "data": run.snapshot()}
        except TimeoutError:
            return error(
                "command_timeout",
                "command may have applied; do not resend until its receipt arrives",
                data=run.snapshot(),
            )
        except (BrokenPipeError, ConnectionResetError):
            return error("process_exited", "native command pipe closed", data=run.snapshot())
        finally:
            run.busy = False

    def status(self, run_id: str) -> dict[str, Any]:
        run = self.runs.get(run_id)
        return {"status": "ok", "data": run.snapshot()} if run else error("not_found", "unknown run ID")

    def list(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "mode": MODE,
            "durable": False,
            "limits": {"max_memory_mib": self.limits.max_memory_mib, "max_seconds": self.limits.max_seconds},
            "runs": [run.snapshot() for run in self.runs.values()],
        }

    async def stop(self, run_id: str) -> dict[str, Any]:
        run = self.runs.get(run_id)
        if run is None:
            return error("not_found", "unknown run ID")
        run.stop_requested = True
        if run.creation is not None and not run.creation.done():
            await asyncio.shield(run.creation)
        if not run.finished.is_set() and run.process:
            if not run.busy and not run.pending_command:
                await self.control(run_id, "stop")
            if run.process.stdin:
                run.process.stdin.close()  # EOF also requests native managed shutdown.
            try:
                await asyncio.wait_for(run.finished.wait(), self.stop_timeout)
            except TimeoutError:
                run.failure = run.failure or "forced_stop"
                await self._force(run)
                await asyncio.wait_for(run.finished.wait(), self.stop_timeout + 1)
        return {"status": "ok", "data": run.snapshot()}

    async def close(self) -> None:
        self.closed = True
        await asyncio.gather(*(self.stop(key) for key in tuple(self.runs)))
