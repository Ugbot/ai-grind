"""Pure parsers for RenderDoc bridge JSON and renderdoccmd output.

Bridge wire format (schema_version 1), written by scripts/bridge.py:

    {"schema_version": 1, "ok": true, "op": "replay", "api": "Vulkan",
     "frame_number": 120, "truncated": false,
     "actions":   [{"eid", "aid", "parent_eid", "depth", "name", "flags",
                    "num_indices", "num_instances", "dispatch"}],
     "resources": [{"id", "name", "type", "width", "height", "depth",
                    "mips", "format", "bytes"}],
     "counters":  [{"eid", "counter", "unit", "value"}],
     "stats":     {"draws", "dispatches", "copies", "markers"}}

Failure: {"schema_version": 1, "ok": false, "error": "...", "stage": "..."}.
"""

from __future__ import annotations

import json
from pathlib import Path

from devtools_mcp.models import create_run_base
from devtools_mcp.renderdoc.models import (
    RdcAction,
    RdcCounter,
    RdcResource,
    RenderdocReplayResult,
)

SCHEMA_VERSION = 1
MAX_ACTIONS = 100_000
MAX_RESOURCES = 50_000
MAX_COUNTERS = 200_000
MAX_NEW_RDCS = 64

# GPU Duration values arrive in microseconds (bridge converts from seconds).
DURATION_COUNTER = "GPU Duration"


def parse_bridge_json(text: str) -> dict:
    """Validate and return the bridge payload; raise ValueError on bad shape."""
    assert isinstance(text, str), f"expected str, got {type(text)}"
    if not text.strip():
        raise ValueError("bridge output is empty")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"bridge output is not JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"bridge output is not an object: {type(payload).__name__}")
    version = payload.get("schema_version")
    if version != SCHEMA_VERSION:
        raise ValueError(f"bridge schema_version {version!r} != {SCHEMA_VERSION}")
    if len(payload.get("actions") or []) > MAX_ACTIONS:
        raise ValueError(f"bridge actions exceed bound ({MAX_ACTIONS})")
    assert "ok" in payload, "bridge payload missing 'ok'"
    return payload


def classify_bridge_error(payload: dict, stderr_tail: str = "") -> str:
    """Map a failed bridge payload to an actionable user-facing message."""
    assert isinstance(payload, dict), f"expected dict, got {type(payload)}"
    error = str(payload.get("error", "")) or stderr_tail or "unknown bridge failure"
    stage = str(payload.get("stage", ""))
    lowered = error.lower()
    if "replaysupport" in lowered or "unsupported" in lowered or "incompatible" in lowered:
        hint = "capture unsupported or RenderDoc version mismatch — recapture with the installed RenderDoc version"
    elif "gpu" in lowered or "device" in lowered or "display" in lowered or "vulkan" in lowered:
        hint = "replay needs a GPU + interactive session — run the MCP server in your user session, not a service"
    elif stage == "open" or "failed to open" in lowered or "no such file" in lowered:
        hint = "could not open the capture file — check the .rdc path"
    elif "no actions" in lowered or "no frames" in lowered:
        hint = "capture contains no frames — the capture trigger never fired (try --frame or press F12 in-app)"
    else:
        hint = "see devtools_raw(run_id) for the full bridge output"
    return f"renderdoc bridge failed at stage '{stage or 'unknown'}': {error}\n{hint}"


def bridge_to_replay_result(
    payload: dict,
    tool: str,
    rdc_path: str,
    duration_seconds: float = 0.0,
) -> RenderdocReplayResult:
    """Map a successful bridge payload onto the result model."""
    assert payload.get("ok") is True, "bridge_to_replay_result requires ok payload"
    assert tool in ("analyze", "counters", "resources"), f"bad replay tool {tool!r}"
    actions = [
        RdcAction(
            event_id=int(a.get("eid", 0)),
            action_id=int(a.get("aid", 0)),
            parent_event_id=int(a.get("parent_eid", 0)),
            depth=int(a.get("depth", 0)),
            name=str(a.get("name", "")),
            flags=str(a.get("flags", "")),
            num_indices=int(a.get("num_indices", 0)),
            num_instances=int(a.get("num_instances", 0)),
            dispatch=[int(x) for x in (a.get("dispatch") or [0, 0, 0])[:3]],
        )
        for a in (payload.get("actions") or [])[:MAX_ACTIONS]
    ]
    resources = [
        RdcResource(
            resource_id=str(r.get("id", "")),
            name=str(r.get("name", "")),
            type=str(r.get("type", "")),
            width=int(r.get("width", 0)),
            height=int(r.get("height", 0)),
            depth=int(r.get("depth", 0)),
            mips=int(r.get("mips", 0)),
            format=str(r.get("format", "")),
            bytes=int(r.get("bytes", 0)),
        )
        for r in (payload.get("resources") or [])[:MAX_RESOURCES]
    ]
    counters = [
        RdcCounter(
            event_id=int(c.get("eid", 0)),
            counter=str(c.get("counter", "")),
            unit=str(c.get("unit", "")),
            value=float(c.get("value", 0.0)),
        )
        for c in (payload.get("counters") or [])[:MAX_COUNTERS]
    ]
    _merge_durations(actions, counters)
    base = create_run_base(suite="renderdoc", tool=tool, binary=rdc_path, duration_seconds=duration_seconds)
    stats = {str(k): int(v) for k, v in (payload.get("stats") or {}).items()}
    result = RenderdocReplayResult(
        **base.model_dump(),
        rdc_path=rdc_path,
        api=str(payload.get("api", "")),
        frame_number=int(payload.get("frame_number", 0)),
        actions=actions,
        resources=resources,
        counters=counters,
        stats=stats,
        truncated=bool(payload.get("truncated", False)),
    )
    assert result.suite == "renderdoc", "suite mismatch after model merge"
    return result


def _merge_durations(actions: list[RdcAction], counters: list[RdcCounter]) -> None:
    """Fill RdcAction.duration_us from GPU Duration counter samples, by event id."""
    assert len(actions) <= MAX_ACTIONS, "actions exceed bound"
    durations = {c.event_id: c.value for c in counters if c.counter == DURATION_COUNTER}
    if not durations:
        return
    for action in actions:
        value = durations.get(action.event_id)
        if value is not None:
            action.duration_us = value


def parse_renderdoccmd_version(text: str) -> str:
    """Extract 'v1.45' style version from `renderdoccmd version` output."""
    assert isinstance(text, str), f"expected str, got {type(text)}"
    for line in text.splitlines()[:10]:
        for token in line.split():
            if token.startswith("v") and token[1:2].isdigit():
                return token.lstrip("v")
    return ""


def find_new_rdcs(directory: str | Path, since: float) -> list[Path]:
    """Capture files under `directory` modified at/after `since` (epoch seconds)."""
    root = Path(directory)
    assert since >= 0, f"bad since timestamp {since}"
    if not root.is_dir():
        return []
    found = [p for p in sorted(root.glob("*.rdc")) if p.stat().st_mtime >= since]
    assert len(found) <= 10_000, "implausible number of capture files"
    return found[:MAX_NEW_RDCS]
