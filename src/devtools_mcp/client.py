"""Bounded MCP batch client; HTTP by default, explicit argv-only stdio."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import CallToolRequest, CallToolRequestParams, CallToolResult, ClientRequest

from devtools_mcp.evidence import ArtifactError, write_json_record

DEFAULT_URL = "http://127.0.0.1:8010/mcp"


@dataclass(frozen=True)
class ToolRequest:
    tool: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class ClientLimits:
    call_timeout: float = 35.0
    batch_timeout: float = 55.0
    max_requests: int = 128
    max_input_bytes: int = 1_048_576
    max_result_bytes: int = 8_388_608
    max_receipt_bytes: int = 33_554_432
    max_inventory_pages: int = 16
    max_inventory_tools: int = 2048

    def validate(self) -> None:
        for seconds in (self.call_timeout, self.batch_timeout):
            if isinstance(seconds, bool) or not math.isfinite(seconds) or not 0 < seconds <= 86_400:
                raise ValueError("timeouts must be finite and within (0, 86400]")
        caps = (
            (self.max_requests, 128),
            (self.max_input_bytes, 1_048_576),
            (self.max_result_bytes, 8_388_608),
            (self.max_receipt_bytes, 33_554_432),
            (self.max_inventory_pages, 16),
            (self.max_inventory_tools, 2048),
        )
        for value, cap in caps:
            if type(value) is not int or not 0 < value <= cap:
                raise ValueError("invalid client size/count bound")
        assert self.call_timeout > 0
        assert self.max_requests > 0


@dataclass(frozen=True)
class ClientConfig:
    transport: Literal["http", "stdio"] = "http"
    url: str = field(default_factory=lambda: os.environ.get("DEVTOOLS_MCP_URL", DEFAULT_URL))
    stdio_argv: tuple[str, ...] = ()
    limits: ClientLimits = field(default_factory=ClientLimits)

    def validate(self) -> None:
        self.limits.validate()
        if self.transport == "http":
            parts = urlsplit(self.url)
            if len(self.url) > 8192 or parts.scheme not in ("http", "https") or not parts.hostname:
                raise ValueError("HTTP transport requires an http(s) URL")
            if parts.username or parts.password or self.stdio_argv:
                raise ValueError("HTTP URL userinfo and stdio argv are not allowed")
        elif self.transport == "stdio":
            if not 0 < len(self.stdio_argv) <= 128 or any(
                not isinstance(arg, str) or not arg or "\0" in arg or len(arg) > 8192 for arg in self.stdio_argv
            ):
                raise ValueError("stdio requires bounded explicit argv")
        else:
            raise ValueError("unknown transport")
        assert self.transport in ("http", "stdio")
        assert self.limits.max_requests <= 128


class BatchError(RuntimeError):
    """Batch failed; inspect receipt, especially any outcome='unknown' call."""

    def __init__(self, kind: str, receipt: dict[str, Any]):
        super().__init__(kind)
        self.kind = kind
        self.receipt = receipt


def _json_bytes(value: Any, cap: int) -> bytes:
    chunks: list[bytes] = []
    size = 1
    encoder = json.JSONEncoder(ensure_ascii=True, allow_nan=False, sort_keys=True, indent=2)
    for chunk in encoder.iterencode(value):
        encoded = chunk.encode("utf-8")
        size += len(encoded)
        if size > cap:
            raise ValueError("encoded JSON exceeds bound")
        chunks.append(encoded)
    assert size <= cap
    assert size == sum(map(len, chunks)) + 1
    return b"".join(chunks) + b"\n"


def _validate_json(value: Any) -> None:
    stack = [(value, 0)]
    for _ in range(65_536):
        if not stack:
            return
        item, depth = stack.pop()
        assert depth >= 0
        assert len(stack) < 65_536
        if depth > 32:
            raise ValueError("JSON nesting exceeds 32")
        if isinstance(item, dict):
            if len(item) + len(stack) > 65_536 or any(not isinstance(key, str) for key in item):
                raise ValueError("invalid or oversized JSON object")
            stack.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            if len(item) + len(stack) > 65_536:
                raise ValueError("oversized JSON array")
            stack.extend((child, depth + 1) for child in item)
        elif item is not None and type(item) not in (str, int, float, bool):
            raise ValueError("arguments must contain only JSON values")
        elif isinstance(item, float) and not math.isfinite(item):
            raise ValueError("non-finite JSON number")
    if stack:
        raise ValueError("JSON node count exceeds bound")


def validate_requests(requests: list[ToolRequest], limits: ClientLimits) -> None:
    limits.validate()
    if not isinstance(requests, list) or not 0 < len(requests) <= limits.max_requests:
        raise ValueError("request batch count out of bounds")
    for request in requests:
        if not isinstance(request, ToolRequest) or not isinstance(request.tool, str):
            raise ValueError("expected ToolRequest")
        if not request.tool or len(request.tool) > 256 or any(ord(char) < 32 for char in request.tool):
            raise ValueError("invalid tool name")
        if not isinstance(request.arguments, dict):
            raise ValueError("arguments must be an object")
        _validate_json(request.arguments)
    _json_bytes([{"tool": item.tool, "arguments": item.arguments} for item in requests], limits.max_input_bytes)
    assert len(requests) <= limits.max_requests
    assert all(isinstance(item.arguments, dict) for item in requests)


def read_requests(path: Path, limits: ClientLimits) -> list[ToolRequest]:
    limits.validate()
    with path.open("rb") as source:
        payload = source.read(limits.max_input_bytes + 1)
    if len(payload) > limits.max_input_bytes:
        raise ValueError("request file exceeds input bound")
    raw = json.loads(payload)
    if not isinstance(raw, list) or not 0 < len(raw) <= limits.max_requests:
        raise ValueError("expected bounded request array")
    requests: list[ToolRequest] = []
    for item in raw:
        if not isinstance(item, dict) or set(item) != {"tool", "arguments"}:
            raise ValueError("each request requires exactly tool and arguments")
        requests.append(ToolRequest(item["tool"], item["arguments"]))
    validate_requests(requests, limits)
    assert len(requests) == len(raw)
    assert len(payload) <= limits.max_input_bytes
    return requests


def _publish(path: Path, value: dict[str, Any], cap: int, *, replace: bool = True) -> None:
    try:
        binding = write_json_record(path, value, replace=replace, max_bytes=cap)
        assert binding.size_bytes <= cap
        assert len(binding.sha256) == 64
    except (OSError, ValueError, TypeError, ArtifactError) as error:
        raise BatchError("receipt_failure", value) from error


@asynccontextmanager
async def _transport(config: ClientConfig) -> AsyncIterator[tuple[Any, Any]]:
    assert config.transport in ("http", "stdio")
    assert config.transport != "stdio" or config.stdio_argv
    if config.transport == "http":
        async with streamable_http_client(config.url) as (reader, writer, _):
            yield reader, writer
    else:
        parameters = StdioServerParameters(command=config.stdio_argv[0], args=list(config.stdio_argv[1:]))
        with open(os.devnull, "w") as errlog:
            async with stdio_client(parameters, errlog=errlog) as (reader, writer):
                yield reader, writer


async def _export_inventory(session: ClientSession, path: Path, limits: ClientLimits) -> None:
    assert 0 < limits.max_inventory_pages <= 16
    assert 0 < limits.max_inventory_tools <= 2048
    pages: list[dict[str, Any]] = []
    count = 0
    cursor: str | None = None
    for _ in range(limits.max_inventory_pages):
        async with asyncio.timeout(limits.call_timeout):
            result = await session.list_tools(cursor=cursor)
        count += len(result.tools)
        if count > limits.max_inventory_tools:
            raise ValueError("inventory tool count exceeds bound")
        pages.append(result.model_dump(mode="json"))
        _json_bytes(pages, limits.max_result_bytes)
        cursor = result.nextCursor
        if cursor is None:
            _publish(path, {"pages": pages}, limits.max_result_bytes)
            return
    raise ValueError("inventory pagination exceeds bound")


async def _run_calls(
    session: ClientSession, requests: list[ToolRequest], path: Path, receipt: dict[str, Any], limits: ClientLimits
) -> None:
    assert 0 < len(requests) <= limits.max_requests
    assert receipt["status"] == "running"
    for index, request in enumerate(requests):
        entry: dict[str, Any] = {
            "index": index,
            "tool": request.tool,
            "arguments": request.arguments,
            "status": "in_flight",
            "outcome": "unknown",
        }
        receipt["calls"].append(entry)
        try:
            _publish(path, receipt, limits.max_receipt_bytes)
        except BatchError:
            entry.update(status="failed", outcome="not_started")
            raise
        async with asyncio.timeout(limits.call_timeout):
            # call_tool performs post-response schema lookup/validation before
            # returning. Receive the typed protocol result directly so evidence
            # cannot be discarded by a later optional schema request.
            result = await session.send_request(
                ClientRequest(
                    CallToolRequest(params=CallToolRequestParams(name=request.tool, arguments=request.arguments))
                ),
                CallToolResult,
                request_read_timeout_seconds=timedelta(seconds=limits.call_timeout),
            )
        structured = result.model_dump(mode="json")
        try:
            _json_bytes(structured, limits.max_result_bytes)
        except ValueError as error:
            raise BatchError("result_bound", receipt) from error
        completed = {**entry, "status": "completed", "outcome": "completed", "result": structured}
        candidate = {**receipt, "calls": [*receipt["calls"][:-1], completed]}
        try:
            _json_bytes(candidate, limits.max_receipt_bytes - 512)
        except ValueError as error:
            raise BatchError("receipt_bound", receipt) from error
        entry.update(completed)
        _publish(path, receipt, limits.max_receipt_bytes)
        if result.isError:
            raise BatchError("mcp_is_error", receipt)


async def call_batch(
    requests: list[ToolRequest],
    result_path: Path,
    *,
    config: ClientConfig | None = None,
    inventory_path: Path | None = None,
) -> dict[str, Any]:
    """Validate before connecting; publish partial receipts; never retry a call.

    Raises BatchError on failure, ValueError on invalid input, and CancelledError
    on caller cancellation. Completed results are returned and saved unmodified.
    Typed MCP parsing is enforced; per-tool outputSchema validation is omitted.
    """
    config = config or ClientConfig()
    config.validate()
    validate_requests(requests, config.limits)
    if inventory_path is not None and inventory_path.resolve() == result_path.resolve():
        raise ValueError("inventory and result paths must differ")
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "transport": config.transport,
        "output_schema_validation": "not_performed",
        "status": "running",
        "request_count": len(requests),
        "calls": [],
    }
    _publish(result_path, receipt, config.limits.max_receipt_bytes, replace=False)
    try:
        async with asyncio.timeout(config.limits.batch_timeout):
            async with _transport(config) as (reader, writer):
                async with ClientSession(
                    reader, writer, read_timeout_seconds=timedelta(seconds=config.limits.call_timeout)
                ) as session:
                    async with asyncio.timeout(config.limits.call_timeout):
                        await session.initialize()
                    if inventory_path is not None:
                        await _export_inventory(session, inventory_path, config.limits)
                    await _run_calls(session, requests, result_path, receipt, config.limits)
        receipt["status"] = "completed"
        _publish(result_path, receipt, config.limits.max_receipt_bytes)
    except BaseException as error:
        kind = error.kind if isinstance(error, BatchError) else type(error).__name__
        interrupted = isinstance(error, (asyncio.CancelledError, KeyboardInterrupt))
        if receipt["calls"] and receipt["calls"][-1]["status"] == "in_flight":
            receipt["calls"][-1]["status"] = "interrupted" if interrupted else "failed"
        receipt.update(status="interrupted" if interrupted else "failed", failure={"kind": kind})
        try:
            _publish(result_path, receipt, config.limits.max_receipt_bytes)
        except BatchError as write_error:
            raise BatchError("receipt_failure", receipt) from write_error
        if interrupted:
            raise
        if not isinstance(error, Exception):
            raise
        raise BatchError(kind, receipt) from error
    assert receipt["status"] == "completed"
    assert len(receipt["calls"]) == len(requests)
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("request_file", type=Path)
    parser.add_argument("result_file", type=Path)
    parser.add_argument("--url", default=os.environ.get("DEVTOOLS_MCP_URL", DEFAULT_URL))
    parser.add_argument("--stdio-argv", help="Explicit JSON argv array; selects stdio, never a shell string")
    parser.add_argument("--call-timeout", type=float, default=35.0)
    parser.add_argument("--batch-timeout", type=float, default=55.0)
    parser.add_argument("--inventory", type=Path)
    args = parser.parse_args(argv)
    try:
        limits = ClientLimits(call_timeout=args.call_timeout, batch_timeout=args.batch_timeout)
        command = json.loads(args.stdio_argv) if args.stdio_argv is not None else []
        if not isinstance(command, list):
            raise ValueError("stdio argv must be a JSON array")
        config = ClientConfig(
            transport="stdio" if args.stdio_argv is not None else "http",
            url=args.url,
            stdio_argv=tuple(command),
            limits=limits,
        )
        if args.request_file.resolve() == args.result_file.resolve() or (
            args.inventory is not None and args.inventory.resolve() == args.request_file.resolve()
        ):
            raise ValueError("input and output paths must differ")
        requests = read_requests(args.request_file, limits)
        receipt = asyncio.run(call_batch(requests, args.result_file, config=config, inventory_path=args.inventory))
    except (Exception, KeyboardInterrupt) as error:
        kind = error.kind if isinstance(error, BatchError) else type(error).__name__
        print(f"MCP batch failed ({kind}); inspect result receipt if present", file=sys.stderr)
        return 1
    assert receipt["status"] == "completed"
    assert len(receipt["calls"]) == len(requests)
    print(f"MCP batch completed: {len(receipt['calls'])} calls; receipt: {args.result_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
