"""Offline client tests: fake SDK sessions/transports, actual atomic receipts."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import Any

import pytest
from mcp.types import CallToolResult, ListToolsResult, TextContent, Tool

from devtools_mcp import client
from devtools_mcp.evidence import artifacts


class FakeSession:
    def __init__(self) -> None:
        self.responses: list[Any] = [CallToolResult(content=[TextContent(type="text", text="ok")])]
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.inventory = [ListToolsResult(tools=[])]
        self.inventory_calls = 0
        self.started = asyncio.Event()
        self.initialize_blocked = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def initialize(self):
        if self.initialize_blocked:
            await asyncio.Event().wait()

    async def send_request(self, sdk_request, result_type, **_):
        assert result_type is CallToolResult
        params = sdk_request.root.params
        self.calls.append((params.name, params.arguments))
        self.started.set()
        result = self.responses.pop(0)
        if result == "block":
            await asyncio.Event().wait()
        if isinstance(result, Exception):
            raise result
        return result

    async def list_tools(self, **_):
        self.inventory_calls += 1
        return self.inventory.pop(0)


@pytest.fixture
def sdk(monkeypatch):
    session = FakeSession()
    state: dict[str, Any] = {"http": [], "stdio": [], "exit_error": False}

    @asynccontextmanager
    async def http(url):
        state["http"].append(url)
        yield None, None, None
        if state["exit_error"]:
            raise OSError("secret transport detail")

    @asynccontextmanager
    async def stdio(params, **_):
        state["stdio"].append(params)
        yield None, None

    monkeypatch.setattr(client, "streamable_http_client", http)
    monkeypatch.setattr(client, "stdio_client", stdio)
    monkeypatch.setattr(client, "ClientSession", lambda *_, **__: session)
    return session, state


def request(name="example", **arguments):
    return client.ToolRequest(name, arguments)


def receipt(path):
    return json.loads(path.read_text())


async def test_preserves_full_result_http_never_spawns_and_no_secrets(tmp_path, sdk, monkeypatch):
    session, state = sdk
    monkeypatch.setenv("DEVTOOLS_MCP_URL", "http://127.0.0.1:1234/mcp?token=environment-secret")
    result = CallToolResult(
        content=[TextContent(type="text", text="domain error despite isError false")],
        structuredContent={"rows": [1, 2], "failed": True},
        isError=False,
        _meta={"trace": "retain"},
        vendor_extension={"keep": True},
    )
    session.responses = [result]
    path = tmp_path / "receipt.json"
    saved = await client.call_batch([request(value=3)], path)
    assert saved == receipt(path)
    assert saved["calls"][0]["result"] == result.model_dump(mode="json")
    assert saved["status"] == "completed"
    assert state["stdio"] == []
    assert state["http"] == ["http://127.0.0.1:1234/mcp?token=environment-secret"]
    assert "environment-secret" not in path.read_text()
    assert list(tmp_path.glob(".evidence-*")) == []


async def test_mcp_error_stops_and_retains_full_partial_progress(tmp_path, sdk):
    session, _ = sdk
    failure = CallToolResult(content=[TextContent(type="text", text="failure payload")], isError=True)
    session.responses *= 2
    session.responses[1] = failure
    path = tmp_path / "result.json"
    with pytest.raises(client.BatchError, match="mcp_is_error"):
        await client.call_batch([request("one"), request("two"), request("three")], path)
    saved = receipt(path)
    assert saved["status"] == "failed"
    assert len(session.calls) == 2
    assert saved["calls"][0]["status"] == "completed"
    assert saved["calls"][1]["result"] == failure.model_dump(mode="json")


@pytest.mark.parametrize("second", ["block", OSError("credential-secret")])
async def test_timeout_or_transport_error_unknown_no_replay(tmp_path, sdk, second):
    session, _ = sdk
    session.responses.append(second)
    path = tmp_path / "result.json"
    limits = client.ClientLimits(call_timeout=0.02, batch_timeout=1)
    with pytest.raises(client.BatchError):
        await client.call_batch(
            [request("one"), request("mutation"), request("three")], path, config=client.ClientConfig(limits=limits)
        )
    saved = receipt(path)
    assert len(session.calls) == 2
    assert saved["calls"][0]["outcome"] == "completed"
    assert saved["calls"][1]["outcome"] == "unknown"
    assert saved["calls"][1]["status"] == "failed"
    assert "credential-secret" not in path.read_text()


async def test_total_timeout_during_initialization_no_calls(tmp_path, sdk):
    session, _ = sdk
    session.initialize_blocked = True
    path = tmp_path / "result.json"
    limits = client.ClientLimits(call_timeout=1, batch_timeout=0.02)
    with pytest.raises(client.BatchError):
        await client.call_batch([request()], path, config=client.ClientConfig(limits=limits))
    assert receipt(path)["status"] == "failed"
    assert session.calls == []


async def test_cancellation_retains_completed_and_unknown(tmp_path, sdk):
    session, _ = sdk
    session.responses.append("block")
    path = tmp_path / "result.json"
    task = asyncio.create_task(client.call_batch([request("one"), request("mutation")], path))
    await session.started.wait()
    assert len(session.calls) == 2
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    saved = receipt(path)
    assert saved["status"] == "interrupted"
    assert saved["calls"][0]["outcome"] == "completed"
    assert saved["calls"][1]["outcome"] == "unknown"
    assert saved["calls"][1]["status"] == "interrupted"


async def test_transport_cleanup_failure_not_success(tmp_path, sdk):
    session, state = sdk
    state["exit_error"] = True
    path = tmp_path / "result.json"
    with pytest.raises(client.BatchError):
        await client.call_batch([request()], path)
    saved = receipt(path)
    assert saved["status"] == "failed"
    assert saved["calls"][0]["result"]
    assert len(session.calls) == 1


@pytest.mark.parametrize(
    "requests",
    [
        [],
        [request("")],
        [request("x", value=float("nan"))],
        [request("x", value=object())],
        [request()] * 129,
        [client.ToolRequest("x", [])],
    ],
)
async def test_invalid_requests_rejected_before_connect(tmp_path, sdk, requests):
    session, state = sdk
    with pytest.raises(ValueError):
        await client.call_batch(requests, tmp_path / "result.json")
    assert not state["http"] and not state["stdio"]
    assert not session.calls


@pytest.mark.parametrize(
    "limits",
    [
        client.ClientLimits(call_timeout=float("inf")),
        client.ClientLimits(batch_timeout=0),
        client.ClientLimits(max_requests=129),
        client.ClientLimits(max_result_bytes=8_388_609),
        client.ClientLimits(max_input_bytes=True),
    ],
)
async def test_invalid_policy_before_connect(tmp_path, sdk, limits):
    _, state = sdk
    with pytest.raises(ValueError):
        await client.call_batch([request()], tmp_path / "result.json", config=client.ClientConfig(limits=limits))
    assert state["http"] == []


async def test_explicit_stdio_preserves_argv_without_shell(tmp_path, sdk):
    _, state = sdk
    command = ("explicit-server", "space argument", "$(not-a-shell)", ";literal")
    await client.call_batch(
        [request()], tmp_path / "result.json", config=client.ClientConfig(transport="stdio", stdio_argv=command)
    )
    assert not state["http"]
    assert state["stdio"][0].command == command[0]
    assert state["stdio"][0].args == list(command[1:])


async def test_inventory_export_bounded_paginated_and_optional(tmp_path, sdk):
    session, _ = sdk
    session.inventory = [
        ListToolsResult(tools=[Tool(name="one", inputSchema={})], nextCursor="next"),
        ListToolsResult(tools=[Tool(name="two", inputSchema={})]),
    ]
    await client.call_batch([request()], tmp_path / "result.json", inventory_path=tmp_path / "schemas.json")
    assert len(receipt(tmp_path / "schemas.json")["pages"]) == 2
    assert session.inventory_calls == 2


async def test_inventory_page_limit_stops_before_calls(tmp_path, sdk):
    session, _ = sdk
    session.inventory = [ListToolsResult(tools=[], nextCursor="next")]
    path = tmp_path / "result.json"
    with pytest.raises(client.BatchError):
        await client.call_batch(
            [request()],
            path,
            inventory_path=tmp_path / "schemas.json",
            config=client.ClientConfig(limits=client.ClientLimits(max_inventory_pages=1)),
        )
    assert session.calls == []
    assert receipt(path)["status"] == "failed"
    assert not (tmp_path / "schemas.json").exists()


async def test_oversized_result_retains_prior_receipt(tmp_path, sdk):
    session, _ = sdk
    session.responses.append(CallToolResult(content=[TextContent(type="text", text="x" * 5000)]))
    path = tmp_path / "result.json"
    with pytest.raises(client.BatchError, match="result_bound"):
        await client.call_batch(
            [request("one"), request("two")],
            path,
            config=client.ClientConfig(limits=client.ClientLimits(max_result_bytes=2048)),
        )
    saved = receipt(path)
    assert saved["calls"][0]["result"]
    assert saved["calls"][1]["outcome"] == "unknown"
    assert "result" not in saved["calls"][1]


async def test_receipt_total_bound_keeps_completed_and_unknown(tmp_path, sdk):
    session, _ = sdk
    session.responses = [CallToolResult(content=[TextContent(type="text", text="x" * 1200)])] * 2
    path = tmp_path / "result.json"
    with pytest.raises(client.BatchError, match="receipt_bound"):
        await client.call_batch(
            [request("one"), request("two")],
            path,
            config=client.ClientConfig(limits=client.ClientLimits(max_receipt_bytes=3000)),
        )
    saved = receipt(path)
    assert saved["calls"][0]["result"]
    assert saved["calls"][1]["outcome"] == "unknown"


async def test_failed_replace_retains_previous_receipt_no_retry(tmp_path, sdk, monkeypatch):
    session, _ = sdk
    session.responses.append(session.responses[0])
    path = tmp_path / "result.json"
    replace = artifacts.os.replace
    attempts = 0

    def fail_later(source, destination):
        nonlocal attempts
        attempts += 1
        if attempts >= 4:
            raise OSError("disk failure")
        return replace(source, destination)

    monkeypatch.setattr(artifacts.os, "replace", fail_later)
    with pytest.raises(client.BatchError, match="receipt_failure"):
        await client.call_batch([request("one"), request("two")], path)
    saved = receipt(path)
    assert saved["calls"][0]["result"]
    assert saved["calls"][1]["outcome"] == "unknown"
    assert len(session.calls) == 2
    assert not list(tmp_path.glob(".evidence-*"))


@pytest.mark.parametrize(
    "raw",
    [
        [{"tool": "x", "arguments": {}, "extra": 1}],
        [["x", {}]],
        {"tool": "x", "arguments": {}},
        [{"tool": "x", "arguments": {"x": float("nan")}}],
    ],
)
def test_request_file_rejects_invalid_shape(tmp_path, raw):
    path = tmp_path / "requests.json"
    path.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        client.read_requests(path, client.ClientLimits())


def test_file_input_bound_and_json_depth(tmp_path):
    path = tmp_path / "requests.json"
    path.write_text(" " * 100)
    with pytest.raises(ValueError, match="input bound"):
        client.read_requests(path, client.ClientLimits(max_input_bytes=32))
    value: Any = 0
    for _ in range(34):
        value = [value]
    with pytest.raises(ValueError, match="nesting"):
        client.validate_requests([request(value=value)], client.ClientLimits())


def test_cli_summary_only_and_error_nonzero(tmp_path, sdk, capsys):
    session, state = sdk
    source = tmp_path / "requests.json"
    output = tmp_path / "results.json"
    source.write_text(json.dumps([{"tool": "one", "arguments": {"private": "argument-secret"}}]))
    assert client.main([str(source), str(output)]) == 0
    printed = capsys.readouterr()
    assert "completed: 1 calls" in printed.out
    assert "argument-secret" not in printed.out
    session.responses = [CallToolResult(content=[], isError=True)]
    assert client.main([str(source), str(tmp_path / "failure.json")]) == 1
    assert "mcp_is_error" in capsys.readouterr().err
    assert state["stdio"] == []


def test_cli_stdio_and_path_collision(tmp_path, sdk):
    session, state = sdk
    source = tmp_path / "requests.json"
    source.write_text(json.dumps([{"tool": "one", "arguments": {}}]))
    assert client.main([str(source), str(tmp_path / "out.json"), "--stdio-argv", '["server", "literal argument"]']) == 0
    assert state["stdio"][0].args == ["literal argument"]
    before = source.read_bytes()
    assert client.main([str(source), str(source)]) == 1
    assert source.read_bytes() == before
    assert len(session.calls) == 1


async def test_existing_receipt_not_clobbered_or_connected(tmp_path, sdk):
    session, state = sdk
    path = tmp_path / "result.json"
    previous = b'{"status":"completed","calls":[{"old":"evidence"}]}'
    path.write_bytes(previous)
    with pytest.raises(client.BatchError, match="receipt_failure"):
        await client.call_batch([request()], path)
    assert path.read_bytes() == previous
    assert not state["http"] and not state["stdio"]
    assert not session.calls


async def test_result_unicode_bound_matches_shared_writer(tmp_path, sdk):
    session, _ = sdk
    session.responses = [CallToolResult(content=[TextContent(type="text", text="\U0001f600" * 300)])]
    path = tmp_path / "result.json"
    with pytest.raises(client.BatchError, match="result_bound"):
        await client.call_batch(
            [request()], path, config=client.ClientConfig(limits=client.ClientLimits(max_result_bytes=2048))
        )
    assert receipt(path)["calls"][0]["outcome"] == "unknown"


@pytest.mark.parametrize(
    "config",
    [
        client.ClientConfig(url="file:///tmp/server"),
        client.ClientConfig(url="http://user:password@localhost/mcp"),
        client.ClientConfig(transport="stdio", stdio_argv=()),
        client.ClientConfig(transport="stdio", stdio_argv=("server", "")),
        client.ClientConfig(stdio_argv=("server",)),
    ],
)
async def test_invalid_transport_config_does_not_connect(tmp_path, sdk, config):
    _, state = sdk
    with pytest.raises(ValueError):
        await client.call_batch([request()], tmp_path / "result.json", config=config)
    assert not state["http"] and not state["stdio"]


@pytest.mark.parametrize("response", ["block", OSError("transport-secret")])
def test_cli_timeout_and_transport_nonzero(tmp_path, sdk, response, capsys):
    session, _ = sdk
    session.responses = [response]
    source = tmp_path / "requests.json"
    path = tmp_path / "result.json"
    source.write_text(json.dumps([{"tool": "mutation", "arguments": {}}]))
    assert client.main([str(source), str(path), "--call-timeout", "0.02", "--batch-timeout", "1"]) == 1
    assert receipt(path)["calls"][0]["outcome"] == "unknown"
    assert "transport-secret" not in capsys.readouterr().err


def test_cli_receipt_failure_nonzero_no_connect(tmp_path, sdk):
    session, state = sdk
    source = tmp_path / "requests.json"
    source.write_text(json.dumps([{"tool": "mutation", "arguments": {}}]))
    assert client.main([str(source), str(tmp_path / "missing" / "result.json")]) == 1
    assert session.calls == []
    assert not state["http"] and not state["stdio"]


# Actual installed SDK regression boundary: synthetic server over memory streams.
# These exercise send_request/protocol parsing rather than FakeSession methods.
def _schema_fault_server(mode, stats, full_result):
    from mcp.server import Server
    from mcp.shared.exceptions import McpError
    from mcp.types import CallToolRequest, ErrorData, ListToolsRequest, ServerResult

    server = Server("client-response-retention-test")

    async def tool_handler(sdk_request):
        stats["calls"].append(sdk_request.params.name)
        if mode == "protocol_error":
            raise McpError(ErrorData(code=-32603, message="synthetic RPC error"))
        return ServerResult(full_result)

    async def schema_handler(_):
        stats["schema_calls"] += 1
        if mode == "lookup_error":
            raise McpError(ErrorData(code=-32603, message="synthetic schema lookup failure"))
        if mode == "lookup_timeout":
            await asyncio.Event().wait()
        return ServerResult(
            ListToolsResult(
                tools=[
                    Tool(
                        name="mutation",
                        inputSchema={},
                        outputSchema=(
                            {"type": "invalid-json-schema-type"}
                            if mode == "invalid_schema"
                            else {"type": "object", "required": ["missing_field"]}
                        ),
                    )
                ]
            )
        )

    server.request_handlers[CallToolRequest] = tool_handler
    server.request_handlers[ListToolsRequest] = schema_handler
    return server


def _sdk_memory_transport(server):
    import anyio
    from mcp.shared.memory import create_client_server_memory_streams

    @asynccontextmanager
    async def transport(_):
        async with (
            create_client_server_memory_streams() as (client_streams, server_streams),
            anyio.create_task_group() as group,
        ):
            group.start_soon(lambda: server.run(*server_streams, server.create_initialization_options()))
            try:
                yield client_streams
            finally:
                group.cancel_scope.cancel()

    return transport


@pytest.mark.parametrize("mode", ["invalid_content", "invalid_schema", "lookup_error", "lookup_timeout"])
async def test_actual_sdk_retains_response_despite_post_response_schema_fault(tmp_path, monkeypatch, mode):
    from mcp import ClientSession as InstalledClientSession
    from mcp.shared.exceptions import McpError

    full_result = CallToolResult(
        content=[TextContent(type="text", text="mutation completed")],
        structuredContent={"mutation_id": "returned-id"},
        isError=False,
        _meta={"trace": "retained"},
        vendor_extension={"receipt_id": "vendor-id"},
    )
    stats = {"calls": [], "schema_calls": 0}
    server = _schema_fault_server(mode, stats, full_result)
    transport = _sdk_memory_transport(server)
    # Causal baseline: high-level call_tool receives this result, then fails in
    # the real SDK's schema validation/lookup. No mutation is repeated there.
    async with transport(None) as (reader, writer), InstalledClientSession(reader, writer) as session:
        await session.initialize()
        with pytest.raises((RuntimeError, McpError, TimeoutError)):
            async with asyncio.timeout(0.05):
                await session.call_tool("mutation", {})
    assert stats["calls"] == ["mutation"]
    assert stats["schema_calls"] == 1

    # Separate synthetic run: the maintained client uses the real SDK's typed
    # send_request, persists both responses, and never initiates schema lookup.
    stats["calls"].clear()
    stats["schema_calls"] = 0
    monkeypatch.setattr(client, "_transport", transport)
    path = tmp_path / "result.json"
    saved = await client.call_batch([request("earlier"), request("mutation")], path)
    assert saved == receipt(path)
    assert saved["status"] == "completed"
    assert saved["output_schema_validation"] == "not_performed"
    assert stats["calls"] == ["earlier", "mutation"]
    assert stats["calls"].count("mutation") == 1
    assert stats["schema_calls"] == 0
    assert len(saved["calls"]) == 2
    for record in saved["calls"]:
        assert record["outcome"] == "completed"
        assert record["result"] == full_result.model_dump(mode="json")


async def test_actual_sdk_protocol_error_still_fails_without_replay(tmp_path, monkeypatch):
    from mcp.shared.exceptions import McpError

    stats = {"calls": [], "schema_calls": 0}
    server = _schema_fault_server("protocol_error", stats, CallToolResult(content=[]))
    monkeypatch.setattr(client, "_transport", _sdk_memory_transport(server))
    path = tmp_path / "result.json"
    with pytest.raises(client.BatchError) as failure:
        await client.call_batch([request("mutation"), request("never")], path)
    cause = failure.value.__cause__
    assert isinstance(cause, BaseExceptionGroup)
    assert cause.subgroup(McpError) is not None
    saved = receipt(path)
    assert saved["status"] == "failed"
    assert saved["calls"][0]["outcome"] == "unknown"
    assert "result" not in saved["calls"][0]
    assert stats["calls"] == ["mutation"]
    assert stats["schema_calls"] == 0


async def test_actual_sdk_is_error_retains_full_result_and_stops(tmp_path, monkeypatch):
    full_result = CallToolResult(
        content=[TextContent(type="text", text="returned tool failure")],
        structuredContent={"mutation_id": "reconcile-id"},
        isError=True,
    )
    stats = {"calls": [], "schema_calls": 0}
    server = _schema_fault_server("is_error", stats, full_result)
    monkeypatch.setattr(client, "_transport", _sdk_memory_transport(server))
    path = tmp_path / "result.json"
    with pytest.raises(client.BatchError):
        await client.call_batch([request("mutation"), request("never")], path)
    saved = receipt(path)
    assert saved["status"] == "failed"
    assert saved["calls"][0]["result"] == full_result.model_dump(mode="json")
    assert saved["calls"][0]["outcome"] == "completed"
    assert stats["calls"] == ["mutation"]
    assert stats["schema_calls"] == 0
