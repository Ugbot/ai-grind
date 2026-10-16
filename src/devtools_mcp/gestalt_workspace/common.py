"""Strict limits, canonical JSON and public-input validation."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any
from urllib.parse import urlsplit

_DEFAULTS = {
    "max_resources": 4096,
    "max_pages": 128,
    "page_size": 128,
    "max_file_bytes": 8 * 1024 * 1024,
    "max_body_bytes": 8 * 1024 * 1024,
    "max_total_bytes": 32 * 1024 * 1024,
    "max_response_bytes": 16 * 1024 * 1024,
    "timeout_ms": 10000,
}
_CEILINGS = {
    "max_resources": 16384,
    "max_pages": 1024,
    "page_size": 512,
    "max_file_bytes": 32 * 1024 * 1024,
    "max_body_bytes": 32 * 1024 * 1024,
    "max_total_bytes": 128 * 1024 * 1024,
    "max_response_bytes": 64 * 1024 * 1024,
    "timeout_ms": 60000,
}
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_REF = re.compile(r"\{\{([^{}]+)\}\}")


class WorkspaceError(Exception):
    def __init__(self, code: str, message: str, uncertain: bool = False):
        super().__init__(message)
        self.code, self.uncertain = code, uncertain


def require(condition: bool, message: str, code: str = "invalid_input") -> None:
    if not condition:
        raise WorkspaceError(code, message)


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON object key", "invalid_json")
        result[key] = value
    return result


def decode(raw: bytes) -> Any:
    try:
        return json.loads(raw, object_pairs_hook=_pairs, parse_constant=lambda _: require(False, "nonfinite JSON"))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise WorkspaceError("invalid_json", "invalid or excessively nested JSON") from exc


def limits_config(values: dict[str, Any] | None) -> dict[str, int]:
    require(values is None or type(values) is dict, "limits must be an object")
    result = dict(_DEFAULTS)
    for key, value in (values or {}).items():
        require(key in result, "unknown limit")
        require(type(value) is int and 1 <= value <= _CEILINGS[key], "limit must be a bounded positive integer")
        result[key] = value
    return result


def endpoint(value: str) -> tuple[str, int, str]:
    require(type(value) is str, "base_url must be explicit HTTP loopback URL")
    require(
        re.fullmatch(r"http://(?:127\.0\.0\.1|\[::1\]):[1-9][0-9]{0,4}/?", value) is not None,
        "base_url must be HTTP 127.0.0.1 or [::1] with concrete port",
    )
    parsed = urlsplit(value)
    require(parsed.port is not None and parsed.port <= 65535, "invalid port")
    return str(parsed.hostname), int(parsed.port or 0), value.rstrip("/")


def resource_id(value: Any) -> str:
    require(
        type(value) is str and re.fullmatch(r"[1-9][0-9]{0,19}", value) is not None,
        "gateway ID/cursor must be a decimal string",
        "invalid_response",
    )
    require(int(value) <= (1 << 64) - 1, "gateway ID overflow", "invalid_response")
    return value
