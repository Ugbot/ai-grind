"""Fixture HTTP gateway tests for bounded, non-destructive reconstruction."""

from __future__ import annotations

import asyncio
import copy
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from devtools_mcp.gestalt_workspace import Bundle, canonical, digest, limits_config, reconstruct


@pytest.fixture
def bundle(tmp_path):
    notebooks = []
    for index in range(1, 5):
        key, name = f"0{index}-example", f"CME Research / 0{index} Example"
        body = {
            "cells": [
                {"id": 1, "lang": "markdown", "code": "Synthetic research"},
                {"id": 2, "lang": "python", "code": "raise RuntimeError('must never execute')"},
            ]
        }
        document = f"{key}.json"
        (tmp_path / document).write_bytes(canonical({"name": name, "body": body}))
        notebooks.append(
            {"key": key, "name": name, "document": document, "jupyter": f"{key}.ipynb", "body_sha256": digest(body)}
        )
    dashboard = {"name": "CME Research / Energy quant desk", "template": "dashboard.template.json"}
    template = {
        "name": dashboard["name"],
        "body": {
            "widgets": [
                {
                    "id": "example",
                    "type": "text",
                    "title": "Research",
                    "width": "full",
                    "content": " ".join("[open](/notebooks/{{" + item["key"] + "}})" for item in notebooks),
                }
            ]
        },
    }
    factories = {"frontend/example.ts": "a" * 64}
    manifest = {
        "schema_version": 1,
        "namespace": "cme.research",
        "name_prefix": "CME Research /",
        "synthetic": True,
        "notebooks": notebooks,
        "dashboard": dashboard,
        "source_factory_files": factories,
        "source_factory_sha256": digest(factories),
    }
    (tmp_path / "manifest.json").write_bytes(canonical(manifest))
    (tmp_path / "dashboard.template.json").write_bytes(canonical(template))
    return tmp_path


class State:
    def __init__(self):
        self.records = {"notebook": {}, "dashboard": {}}
        self.calls = []
        self.wire_requests = []
        self.creates = 0
        self.next_id = 1791929009050621001
        self.fault = None
        self.page_override = None
        self.wire_mode = None
        self.active = 0
        self.max_active = 0

    def add(self, kind, name, body):
        identifier = str(self.next_id)
        self.next_id += 1
        self.records[kind][identifier] = {"id": identifier, "kind": kind, "name": name, "body": body}
        return identifier

    def dispatch(self, action, params):
        self.calls.append((action, params))
        if self.fault:
            result = self.fault(action, params)
            if result is not None:
                return result
        kind, operation = action.split(".")
        if operation == "list":
            if self.page_override:
                return 200, {"ok": True, "data": self.page_override(action, params)}
            rows = [row for row in self.records[kind].values() if row["name"].startswith(params["name_prefix"])]
            rows.sort(key=lambda row: int(row["id"]), reverse=True)
            if "cursor" in params:
                rows = [row for row in rows if int(row["id"]) < int(params["cursor"])]
            selected = rows[: params["limit"]]
            cursor = selected[-1]["id"] if len(rows) > len(selected) else None
            return 200, {"ok": True, "data": {kind + "s": selected, "count": len(selected), "next_cursor": cursor}}
        if operation == "get":
            record = self.records[kind].get(params["id"])
            return (200, {"ok": True, "data": record}) if record else (404, {"ok": False, "error": {}})
        assert operation == "create", "no update/delete/execute permitted"
        self.creates += 1
        identifier = self.add(kind, params["name"], json.loads(params["body"]))
        return 200, {"ok": True, "data": {"id": identifier}}


@pytest.fixture
def gateway():
    state = State()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            assert self.path in {"/api/v1/compute", "/api/v1/admin", "/api/v1/storage", "/api/v1/catalog"}
            raw_request = self.rfile.read(int(self.headers["Content-Length"]))
            state.wire_requests.append((self.path, raw_request))
            request = json.loads(raw_request)
            if state.wire_mode:
                import time

                chunks = (
                    [b"HTTP/1.1 100 Continue\r\n\r\n"] * 200
                    if state.wire_mode == "repeated100"
                    else [b"HTTP/1.1 200 OK\r\nX-Slow: "] + [b"x"] * 200
                )
                for chunk in chunks:
                    try:
                        self.connection.sendall(chunk)
                    except OSError:
                        break
                    time.sleep(0.005)
                return
            status, result = state.dispatch(request["action"], request["params"])
            if status == 0:
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            raw = result if isinstance(result, bytes) else canonical(result)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield state, f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


async def install(bundle, gateway, **kwargs):
    return await reconstruct("install", str(bundle), gateway[1], kwargs or None)


async def test_empty_install_readback_and_second_install_no_creates(bundle, gateway):
    first = await install(bundle, gateway)
    assert first["ok"] and first["outcome"] == "complete"
    assert [row["state"] for row in first["assets"]] == ["created"] * 5
    assert all(type(row["id"]) is str and len(row["body_sha256"]) == 64 for row in first["assets"])
    assert gateway[0].creates == 5
    second = await install(bundle, gateway)
    assert second["ok"] and all(row["state"] == "reused" for row in second["assets"])
    assert gateway[0].creates == 5
    assert all("must never execute" not in str(row) for row in first["assets"])
    body = next(iter(gateway[0].records["dashboard"].values()))["body"]
    assert "{{" not in str(body)


@pytest.mark.parametrize("kind", ["notebook", "dashboard"])
async def test_edits_and_duplicates_refuse_before_creation(bundle, gateway, kind):
    assert (await install(bundle, gateway))["ok"]
    state, _ = gateway
    record = next(iter(state.records[kind].values()))
    record["body"]["user_edit"] = True
    before = state.creates
    result = await install(bundle, gateway)
    assert result["error"]["code"] == "conflict" and state.creates == before
    del record["body"]["user_edit"]
    state.add(kind, record["name"], copy.deepcopy(record["body"]))
    result = await install(bundle, gateway)
    assert result["error"]["code"] == "conflict" and state.creates == before


async def test_dashboard_conflict_with_missing_notebooks_is_preflight(bundle, gateway):
    loaded = Bundle(str(bundle), limits_config(None))
    gateway[0].add("dashboard", loaded.dashboard["name"], {"widgets": []})
    result = await install(bundle, gateway)
    assert result["error"]["code"] == "conflict" and gateway[0].creates == 0


async def test_verify_never_mutates_and_inspect_reports_missing(bundle, gateway):
    verify = await reconstruct("verify", str(bundle), gateway[1])
    inspect = await reconstruct("inspect", str(bundle), gateway[1])
    assert not verify["ok"] and verify["outcome"] == "incomplete"
    assert inspect["ok"] and inspect["outcome"] == "incomplete" and gateway[0].creates == 0
    assert (await install(bundle, gateway))["ok"]
    before = gateway[0].creates
    assert (await reconstruct("verify", str(bundle), gateway[1]))["ok"]
    assert gateway[0].creates == before


async def test_multipage_complete_and_exhaustion(bundle, gateway):
    assert (await install(bundle, gateway, page_size=1))["ok"]
    result = await reconstruct("verify", str(bundle), gateway[1], {"page_size": 1})
    assert result["ok"]
    before = gateway[0].creates
    result = await install(bundle, gateway, page_size=1, max_pages=1)
    assert result["error"]["code"] == "limit_exhausted" and gateway[0].creates == before


@pytest.mark.parametrize(
    "page",
    [
        {"notebooks": [], "count": 0},
        {"notebooks": [], "count": 0, "next_cursor": "123"},
        {"notebooks": [], "count": True, "next_cursor": None},
        {"notebooks": [{"id": 1, "name": "CME Research / X", "kind": "notebook"}], "count": 1, "next_cursor": None},
    ],
)
async def test_invalid_pages_never_create(bundle, gateway, page):
    gateway[0].page_override = lambda *_: page
    result = await install(bundle, gateway)
    assert not result["ok"] and result["error"]["code"] == "invalid_response"
    assert gateway[0].creates == 0


async def test_repeated_cursor(bundle, gateway):
    count = 0

    def page(*_):
        nonlocal count
        count += 1
        return {
            "notebooks": [{"id": str(count), "name": "CME Research / X", "kind": "notebook"}],
            "count": 1,
            "next_cursor": "123",
        }

    gateway[0].page_override = page
    result = await install(bundle, gateway)
    assert not result["ok"] and gateway[0].creates == 0


@pytest.mark.parametrize("reply", [b"not json", {"ok": 1}, {"ok": True, "data": []}, {"ok": False, "error": {}}])
async def test_malformed_envelope(bundle, gateway, reply):
    gateway[0].fault = lambda *_: (200, reply)
    result = await install(bundle, gateway)
    assert not result["ok"] and gateway[0].creates == 0


@pytest.mark.parametrize("status", [302, 400, 500])
async def test_http_error_no_redirect_or_retry(bundle, gateway, status):
    gateway[0].fault = lambda *_: (status, b"{}")
    result = await install(bundle, gateway)
    assert result["error"]["code"] == "http_error" and len(gateway[0].calls) == 1


async def test_partial_create_failure_preserves_receipts_then_resume(bundle, gateway):
    state = gateway[0]
    state.fault = lambda action, _: (400, {"ok": False}) if action == "notebook.create" and state.creates == 1 else None
    result = await install(bundle, gateway)
    assert result["outcome"] == "uncertain" and state.creates == 1
    assert result["assets"][0]["state"] == "created"
    state.fault = None
    inspected = await reconstruct("inspect", str(bundle), gateway[1])
    assert inspected["assets"][0]["state"] == "reused" and state.creates == 1
    assert (await install(bundle, gateway))["ok"] and state.creates == 5


async def test_readback_failure_retains_known_id(bundle, gateway):
    state = gateway[0]
    state.fault = lambda action, _: (500, b"{}") if action == "notebook.get" else None
    result = await install(bundle, gateway)
    assert result["outcome"] == "error" and state.creates == 1
    assert result["assets"][0]["state"] == "created_unverified" and result["assets"][0]["id"]
    state.fault = None
    assert (await install(bundle, gateway))["ok"] and state.creates == 5


async def test_lost_ack_discovered_without_blind_retry(bundle, gateway):
    state = gateway[0]

    def lost(action, params):
        if action == "notebook.create":
            state.creates += 1
            state.add("notebook", params["name"], json.loads(params["body"]))
            state.fault = None
            return 0, b""
        return None

    state.fault = lost
    result = await install(bundle, gateway)
    assert result["outcome"] == "uncertain" and state.creates == 1
    assert result["assets"][0]["state"] == "uncertain"
    inspected = await reconstruct("inspect", str(bundle), gateway[1])
    assert inspected["assets"][0]["state"] == "reused" and state.creates == 1
    assert (await install(bundle, gateway))["ok"] and state.creates == 5


@pytest.mark.parametrize("identifier", [1, 1.5, True, "0", "01", "18446744073709551616", "1e9"])
async def test_malformed_create_id_uncertain(bundle, gateway, identifier):
    gateway[0].fault = lambda action, _: (
        (200, {"ok": True, "data": {"id": identifier}}) if action.endswith("create") else None
    )
    result = await install(bundle, gateway)
    assert result["outcome"] == "uncertain" and result["error"]["code"] == "invalid_response"


@pytest.mark.parametrize(
    "limits",
    [
        {"unknown": 1},
        {"page_size": True},
        {"page_size": "1"},
        {"page_size": 1.0},
        {"page_size": 513},
        {"timeout_ms": 0},
        {"max_total_bytes": 1 << 100},
        [],
    ],
)
async def test_strict_limits(bundle, gateway, limits):
    result = await reconstruct("install", str(bundle), gateway[1], limits)
    assert not result["ok"] and not gateway[0].calls


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1:8080",
        "http://localhost:8080",
        "http://example.com:8080",
        "http://127.0.0.1",
        "http://127.0.0.1:65536",
        "http://127.0.0.1:1/x",
        "http://user@127.0.0.1:8080",
        "http://127.0.0.1:1?x=1",
    ],
)
async def test_explicit_loopback_only(bundle, url):
    assert not (await reconstruct("install", str(bundle), url))["ok"]


@pytest.mark.parametrize("mode", ["hash", "reference", "escape", "duplicate", "factory", "schema"])
async def test_bundle_validation_before_http(bundle, gateway, mode):
    path = bundle / "manifest.json"
    manifest = json.loads(path.read_bytes())
    if mode == "hash":
        manifest["notebooks"][0]["body_sha256"] = "0" * 64
    elif mode == "reference":
        template_path = bundle / "dashboard.template.json"
        template_path.write_text(template_path.read_text().replace("{{01-example}}", "{{unknown}}"))
    elif mode == "escape":
        manifest["notebooks"][0]["document"] = "../outside.json"
        (bundle.parent / "outside.json").write_text("{}")
    elif mode == "duplicate":
        manifest["notebooks"][1]["key"] = manifest["notebooks"][0]["key"]
    elif mode == "factory":
        manifest["source_factory_sha256"] = "0" * 64
    elif mode == "schema":
        manifest["schema_version"] = True
    path.write_bytes(canonical(manifest))
    result = await install(bundle, gateway)
    assert not result["ok"] and not gateway[0].calls


async def test_bounded_files_total_body_response_and_resource_count(bundle, gateway):
    for config in ({"max_file_bytes": 1}, {"max_total_bytes": 1}, {"max_body_bytes": 1}, {"max_resources": 4}):
        assert not (await reconstruct("install", str(bundle), gateway[1], config))["ok"]
    assert not gateway[0].calls
    assert not (await install(bundle, gateway, max_response_bytes=1))["ok"]
    assert gateway[0].creates == 0


async def test_malformed_get_body(bundle, gateway):
    assert (await install(bundle, gateway))["ok"]
    before = gateway[0].creates
    gateway[0].fault = lambda action, params: (
        (
            200,
            {
                "ok": True,
                "data": {"id": params["id"], "kind": "notebook", "name": "CME Research / 01 Example", "body": "{}"},
            },
        )
        if action == "notebook.get"
        else None
    )
    assert not (await install(bundle, gateway))["ok"]
    assert gateway[0].creates == before


async def test_async_admission_serializes_installs(bundle, gateway):
    results = await asyncio.gather(install(bundle, gateway), install(bundle, gateway))
    assert all(result["ok"] for result in results) and gateway[0].creates == 5


async def test_registered_callable():
    import devtools_mcp.tools  # noqa: F401
    from devtools_mcp.server import mcp
    from devtools_mcp.tools.gestalt_workspace_tools import devtools_gestalt_workspace

    assert "devtools_gestalt_workspace" in {tool.name for tool in await mcp.list_tools()}
    result = await devtools_gestalt_workspace("inspect", "relative", "http://127.0.0.1:1")
    assert not result["ok"]


@pytest.mark.parametrize("kind", ["notebook", "dashboard"])
async def test_canonical_numeric_type_edit_refused_before_create(bundle, gateway, kind):
    assert (await install(bundle, gateway))["ok"]
    state = gateway[0]
    if kind == "notebook":
        records = list(state.records["notebook"].values())
        del state.records["notebook"][records[0]["id"]]
        records[1]["body"]["cells"][0]["id"] = True
    else:
        record = next(iter(state.records["dashboard"].values()))
        # Add a scalar to both expected template and existing body, differing only in JSON numeric type.
        path = bundle / "dashboard.template.json"
        template = json.loads(path.read_bytes())
        template["body"]["number"] = 1
        path.write_bytes(canonical(template))
        record["body"]["number"] = 1.0
    before = state.creates
    result = await install(bundle, gateway)
    assert result["error"]["code"] == "conflict" and state.creates == before


async def test_symlink_escape_and_duplicate_json_rejected(bundle, gateway):
    path = bundle / "01-example.json"
    outside = bundle.parent / "outside-notebook.json"
    outside.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(outside)
    result = await install(bundle, gateway)
    assert not result["ok"] and not gateway[0].calls
    path.unlink()
    path.write_bytes(outside.read_bytes())
    (bundle / "manifest.json").write_bytes(b'{"schema_version":1,"schema_version":1}')
    result = await install(bundle, gateway)
    assert result["error"]["code"] == "invalid_json" and not gateway[0].calls


async def test_success_ack_body_readback_mismatch_retains_asset(bundle, gateway):
    state = gateway[0]

    def mismatched(action, params):
        if action == "notebook.get":
            record = copy.deepcopy(state.records["notebook"][params["id"]])
            record["body"]["cells"][0]["id"] = True
            return 200, {"ok": True, "data": record}
        return None

    state.fault = mismatched
    result = await install(bundle, gateway)
    assert result["error"]["code"] == "readback_mismatch" and state.creates == 1
    assert result["assets"][0]["state"] == "created_unverified"
    state.fault = None
    assert (await install(bundle, gateway))["ok"] and state.creates == 5


async def test_lost_ack_timeout_is_discovered(bundle, gateway):
    import time

    state = gateway[0]

    def delayed(action, params):
        if action == "notebook.create":
            state.creates += 1
            identifier = state.add("notebook", params["name"], json.loads(params["body"]))
            state.fault = None
            time.sleep(0.08)
            return 200, {"ok": True, "data": {"id": identifier}}
        return None

    state.fault = delayed
    result = await install(bundle, gateway, timeout_ms=30)
    assert result["outcome"] == "uncertain" and state.creates == 1
    assert (await install(bundle, gateway))["ok"] and state.creates == 5


async def test_cancellation_keeps_admission_until_worker_finishes(bundle, gateway, monkeypatch):
    import devtools_mcp.gestalt_workspace as module

    # Use a fresh lock because pytest gives each test its own event loop.
    monkeypatch.setattr(module, "_ADMISSION", asyncio.Lock())
    original = module._run
    entered = threading.Event()
    release = threading.Event()
    active = 0
    maximum = 0

    def blocked(*args):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        entered.set()
        assert release.wait(timeout=2)
        try:
            return original(*args)
        finally:
            active -= 1

    monkeypatch.setattr(module, "_run", blocked)
    first = asyncio.create_task(install(bundle, gateway))
    assert await asyncio.to_thread(entered.wait, 1)
    first.cancel()
    await asyncio.sleep(0)
    first.cancel()
    second = asyncio.create_task(install(bundle, gateway))
    await asyncio.sleep(0)
    assert maximum == 1
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert (await second)["ok"] and maximum == 1 and gateway[0].creates == 5


async def test_unsupported_safe_file_admission_is_structured_before_http(bundle, gateway, monkeypatch):
    import os

    monkeypatch.delattr(os, "O_NOFOLLOW")
    result = await install(bundle, gateway)
    assert result["error"]["code"] == "unsupported_platform" and not gateway[0].calls


async def test_scanned_resource_limit_exhaustion(bundle, gateway):
    for index in range(6):
        gateway[0].add("notebook", f"CME Research / Extra {index}", {"cells": []})
    result = await install(bundle, gateway, max_resources=5)
    assert result["error"]["code"] == "limit_exhausted" and gateway[0].creates == 0


async def test_changed_file_during_read_rejected(bundle, gateway, monkeypatch):
    import os
    from types import SimpleNamespace

    actual = os.fstat
    count = 0

    def changed(fd):
        nonlocal count
        count += 1
        info = actual(fd)
        if count == 3:
            return SimpleNamespace(
                st_dev=info.st_dev,
                st_ino=info.st_ino,
                st_size=info.st_size,
                st_mtime_ns=info.st_mtime_ns + 1,
                st_ctime_ns=info.st_ctime_ns,
            )
        return info

    monkeypatch.setattr(os, "fstat", changed)
    result = await install(bundle, gateway)
    assert result["error"]["message"] == "bundle file changed while reading" and not gateway[0].calls


@pytest.mark.parametrize("mode", ["repeated100", "trickle_headers"])
async def test_absolute_deadline_cuts_off_informational_or_trickled_headers(bundle, gateway, mode):
    import time

    gateway[0].wire_mode = mode
    start = time.monotonic()
    result = await install(bundle, gateway, timeout_ms=30)
    elapsed = time.monotonic() - start
    assert not result["ok"] and result["outcome"] == "error" and gateway[0].creates == 0
    assert elapsed < 0.5


async def test_worst_case_dashboard_resolution_overflow_before_any_http(bundle, gateway):
    path = bundle / "manifest.json"
    manifest = json.loads(path.read_bytes())
    for index, entry in enumerate(manifest["notebooks"]):
        entry["key"] = "abcd"[index]
    path.write_bytes(canonical(manifest))
    template_path = bundle / "dashboard.template.json"
    template = json.loads(template_path.read_bytes())
    template["body"]["widgets"][0]["content"] = " ".join(
        "{{" + entry["key"] + "}}" for entry in manifest["notebooks"] for _ in range(10)
    )
    template_path.write_bytes(canonical(template))
    limit = len(canonical(template["body"]))
    result = await install(bundle, gateway, max_body_bytes=limit)
    assert result["error"]["message"] == "resolved dashboard worst-case body byte limit exceeded"
    assert not gateway[0].calls


@pytest.mark.parametrize("domain", [None, "compute", "admin", "storage", "catalog"])
def test_gateway_explicit_domain_preserves_request_envelope(gateway, domain):
    from devtools_mcp.gestalt_workspace.gateway import Gateway

    state, endpoint = gateway
    state.dispatch = lambda action, params: (200, {"ok": True, "data": {"observed": action}})
    client = Gateway("127.0.0.1", int(endpoint.rsplit(":", 1)[1]), limits_config(None))
    arguments = {} if domain is None else {"domain": domain}
    assert client.call("table.list", {"limit": 3}, **arguments) == {"observed": "table.list"}
    assert state.wire_requests == [
        (f"/api/v1/{domain or 'compute'}", canonical({"action": "table.list", "resource": "", "params": {"limit": 3}}))
    ]


@pytest.mark.parametrize("domain", ["", "Compute", "../admin", "compute?x=1", "admin/", None, 1, [], {}])
def test_gateway_invalid_domain_refuses_before_connection(monkeypatch, domain):
    import http.client

    from devtools_mcp.gestalt_workspace.common import WorkspaceError
    from devtools_mcp.gestalt_workspace.gateway import Gateway

    def forbidden_connection(*args, **kwargs):
        pytest.fail("invalid domain must refuse before constructing a connection")

    monkeypatch.setattr(http.client, "HTTPConnection", forbidden_connection)
    client = Gateway("127.0.0.1", 1, limits_config(None))
    with pytest.raises(WorkspaceError) as error:
        client.call("org.create", {}, domain=domain)
    assert error.value.code == "invalid_input" and not error.value.uncertain


@pytest.mark.parametrize("domain", ["admin", "storage", "catalog"])
@pytest.mark.parametrize("mode", ["repeated100", "trickle_headers"])
def test_gateway_other_domains_keep_absolute_deadline(gateway, domain, mode):
    import time

    from devtools_mcp.gestalt_workspace.common import WorkspaceError
    from devtools_mcp.gestalt_workspace.gateway import Gateway

    state, endpoint = gateway
    state.wire_mode = mode
    client = Gateway("127.0.0.1", int(endpoint.rsplit(":", 1)[1]), limits_config({"timeout_ms": 30}))
    start = time.monotonic()
    with pytest.raises(WorkspaceError) as error:
        client.call("org.create", {}, domain=domain)
    assert time.monotonic() - start < 0.5
    assert error.value.code == "transport_error" and error.value.uncertain
    assert len(state.wire_requests) == 1 and state.creates == 0


@pytest.mark.parametrize("domain", ["admin", "storage", "catalog"])
def test_gateway_other_domains_keep_response_byte_limit(gateway, domain):
    from devtools_mcp.gestalt_workspace.common import WorkspaceError
    from devtools_mcp.gestalt_workspace.gateway import Gateway

    state, endpoint = gateway
    state.dispatch = lambda action, params: (200, b"x" * 1025)
    limits = limits_config(None)
    limits["max_response_bytes"] = 1024
    client = Gateway("127.0.0.1", int(endpoint.rsplit(":", 1)[1]), limits)
    with pytest.raises(WorkspaceError) as error:
        client.call("table.list", {}, domain=domain)
    assert error.value.code == "invalid_response" and not error.value.uncertain
    assert len(state.wire_requests) == 1
