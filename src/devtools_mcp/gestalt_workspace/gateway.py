"""Bounded loopback gateway transport; no redirects or automatic retries."""

from __future__ import annotations

import http.client
import socket
import threading
import time
from contextlib import suppress
from typing import Any

from .common import WorkspaceError, canonical, decode, require, resource_id


def _expire(transport: socket.socket) -> None:
    """Unblock even trickled headers and repeated informational responses."""
    with suppress(OSError):
        transport.shutdown(socket.SHUT_RDWR)


class Gateway:
    def __init__(self, host: str, port: int, limits: dict[str, int]):
        self.host, self.port, self.limits = host, port, limits

    def call(self, action: str, params: dict[str, Any], *, domain: str = "compute") -> dict[str, Any]:
        require(
            type(domain) is str and domain in {"compute", "admin", "storage", "catalog"},
            "invalid gateway domain",
        )
        mutation = action.endswith(".create")
        connection = http.client.HTTPConnection(self.host, self.port, timeout=self.limits["timeout_ms"] / 1000)
        deadline = time.monotonic() + self.limits["timeout_ms"] / 1000
        watchdog: threading.Timer | None = None
        try:
            payload = canonical({"action": action, "resource": "", "params": params})
            connection.connect()
            transport_socket = connection.sock
            require(transport_socket is not None, "gateway socket unavailable", "transport_error")
            remaining = deadline - time.monotonic()
            require(remaining > 0, "gateway connect deadline exceeded", "transport_error")
            watchdog = threading.Timer(remaining, _expire, args=(transport_socket,))
            watchdog.daemon = True
            watchdog.start()
            transport_socket.settimeout(remaining)
            connection.request("POST", f"/api/v1/{domain}", payload, {"Content-Type": "application/json"})
            remaining = deadline - time.monotonic()
            require(remaining > 0, "gateway request deadline exceeded", "transport_error")
            transport_socket.settimeout(remaining)
            response = connection.getresponse()
            chunks: list[bytes] = []
            size = 0
            while size <= self.limits["max_response_bytes"]:
                if response.isclosed():
                    break
                remaining = deadline - time.monotonic()
                require(remaining > 0, "gateway response deadline exceeded", "transport_error")
                transport_socket.settimeout(remaining)
                chunk = response.read1(min(65536, self.limits["max_response_bytes"] + 1 - size))
                if not chunk:
                    break
                chunks.append(chunk)
                size += len(chunk)
            require(
                size <= self.limits["max_response_bytes"], "gateway response byte limit exceeded", "invalid_response"
            )
            require(response.length in (None, 0), "truncated gateway response", "invalid_response")
            require(200 <= response.status < 300, f"gateway HTTP {response.status}", "http_error")
            envelope = decode(b"".join(chunks))
            require(
                type(envelope) is dict and type(envelope.get("ok")) is bool,
                "invalid gateway envelope",
                "invalid_response",
            )
            require(envelope["ok"] is True, "gateway rejected request", "gateway_error")
            require(type(envelope.get("data")) is dict, "gateway data object required", "invalid_response")
            return envelope["data"]
        except WorkspaceError as exc:
            if mutation and exc.code not in {"gateway_error"}:
                exc.uncertain = True
            raise
        except (OSError, http.client.HTTPException, ValueError) as exc:
            raise WorkspaceError(
                "transport_error", "gateway transport failed; inspect before resuming", mutation
            ) from exc
        finally:
            if watchdog is not None:
                watchdog.cancel()
                watchdog.join()
            connection.close()

    def get(self, kind: str, identifier: str, name: str) -> dict[str, Any]:
        record = self.call(f"{kind}.get", {"id": identifier})
        require(
            record.get("id") == identifier
            and record.get("kind") == kind
            and record.get("name") == name
            and type(record.get("body")) is dict,
            "get identity/body mismatch",
            "invalid_response",
        )
        return record["body"]

    def listed(self, kind: str, prefix: str) -> dict[str, list[str]]:
        found: dict[str, list[str]] = {}
        cursors: set[str] = set()
        ids: set[str] = set()
        params: dict[str, Any] = {"name_prefix": prefix, "limit": self.limits["page_size"]}
        for _ in range(self.limits["max_pages"]):
            page = self.call(f"{kind}.list", params)
            rows: Any = page.get(f"{kind}s")
            require(
                type(rows) is list
                and len(rows) <= self.limits["page_size"]
                and "next_cursor" in page
                and type(page.get("count")) is int
                and page["count"] == len(rows),
                "invalid list page",
                "invalid_response",
            )
            for row in rows:
                require(type(row) is dict, "invalid list resource", "invalid_response")
                identifier = resource_id(row.get("id"))
                name = row.get("name")
                require(
                    identifier not in ids and row.get("kind") == kind and type(name) is str and name.startswith(prefix),
                    "invalid or repeated list resource",
                    "invalid_response",
                )
                ids.add(identifier)
                require(len(ids) <= self.limits["max_resources"], "resource scan limit exhausted", "limit_exhausted")
                found.setdefault(name, []).append(identifier)
            cursor = page["next_cursor"]
            if cursor is None:
                return found
            cursor = resource_id(cursor)
            require(rows != [] and cursor not in cursors, "empty or repeated cursor page", "invalid_response")
            cursors.add(cursor)
            params["cursor"] = cursor
        raise WorkspaceError("limit_exhausted", "pagination incomplete: max_pages exhausted")
