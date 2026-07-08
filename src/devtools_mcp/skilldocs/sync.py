"""Peer sync for live skills over the dashboard's /api/skilldoc/ endpoints.

Two-step exchange per skill, both directions in one call:
  1. POST /api/skilldoc/exchange {name, sv} — send my state vector, receive
     the peer's diff (everything I'm missing) plus the peer's state vector.
  2. POST /api/skilldoc/push {name, update} — send the diff the peer is
     missing, computed against the state vector from step 1.

CRDT merge makes the whole thing idempotent and order-independent; syncing
twice or through an intermediate machine converges to the same text.
"""

from __future__ import annotations

import base64
import json
import urllib.request

from devtools_mcp.skilldocs.store import SKILLS_MAX, SkillDocError, SkillDocStore

HTTP_TIMEOUT_S: float = 10.0
_EMPTY_DIFF_MAX: int = 16  # a pycrdt no-op diff is ~13 bytes; skip pushing those


def _post_json(url: str, payload: dict) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_S) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _unb64(data: str) -> bytes:
    return base64.b64decode(data.encode("ascii")) if data else b""


def sync_once(store: SkillDocStore, base_url: str) -> dict[str, int]:
    """Full bidirectional skill-doc sync with one peer. Returns counters."""
    base = (base_url or "").rstrip("/")
    if not base.startswith(("http://", "https://")):
        raise SkillDocError(f"peer url must be http(s), got {base_url!r}")
    try:
        remote_names = {s["name"] for s in _get_json(base + "/api/skilldoc/list").get("skills", [])}
    except (OSError, ValueError) as exc:
        raise SkillDocError(f"peer unreachable at {base}: {exc}") from exc
    local_names = {s["name"] for s in store.list_skills()}
    names = sorted(local_names | remote_names)[:SKILLS_MAX]
    assert len(names) <= SKILLS_MAX, "sync name set exceeded bound"

    counters = {"skills": len(names), "pulled": 0, "pushed": 0, "materialized": 0}
    for name in names:  # bounded
        my_sv = store.state(name) if name in local_names else b""
        reply = _post_json(base + "/api/skilldoc/exchange", {"name": name, "sv": _b64(my_sv)})
        their_diff = _unb64(reply.get("update", ""))
        their_sv = _unb64(reply.get("sv", ""))
        if len(their_diff) > _EMPTY_DIFF_MAX or (their_diff and name not in local_names):
            if store.apply(name, their_diff) is not None:
                counters["materialized"] += 1
            counters["pulled"] += 1
        if name in local_names or store.exists(name):
            my_diff = store.diff(name, their_sv if their_sv else None)
            if len(my_diff) > _EMPTY_DIFF_MAX or not their_sv:
                _post_json(base + "/api/skilldoc/push", {"name": name, "update": _b64(my_diff)})
                counters["pushed"] += 1
    return counters
