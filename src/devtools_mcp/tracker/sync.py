"""Peer sync over HTTP: exchange CRDT ops with another running replica.

The transport is deliberately simple and always-correct for v1: a full
bidirectional exchange (pull everything, merge, push everything). Merge is
idempotent and LWW, so re-sending ops costs bandwidth, never correctness,
and a tracker op-log is small. Watermarks are recorded per peer for status
display; incremental exchange (vector clocks) is the documented upgrade path.

Endpoints (served by the viz server):
    GET  /api/crdt/status        -> {site_id, latest_hlc, ops}
    GET  /api/crdt/ops[?after=h] -> {site_id, ops: [...]}
    POST /api/crdt/push          <- {site_id, ops} -> merge counters
"""

from __future__ import annotations

import httpx

from devtools_mcp.tracker import crdt
from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso

SYNC_TIMEOUT_SECONDS: float = 30.0


def _client(base_url: str, client: httpx.Client | None) -> httpx.Client:
    assert base_url.startswith(("http://", "https://")), f"bad peer url {base_url!r}"
    return client or httpx.Client(base_url=base_url, timeout=SYNC_TIMEOUT_SECONDS)


def _get_json(client: httpx.Client, path: str) -> dict:
    try:
        response = client.get(path)
    except httpx.HTTPError as exc:
        raise TrackerError(f"peer unreachable: {exc}") from exc
    if response.status_code != 200:
        raise TrackerError(f"peer GET {path} -> {response.status_code}")
    body = response.json()
    assert isinstance(body, dict), f"peer returned non-object JSON for {path}"
    return body


def sync_once(db: TrackerDB, peer_url: str, client: httpx.Client | None = None) -> dict:
    """One full bidirectional exchange with a peer replica. Returns counters."""
    assert db.conn is not None, "sync on closed TrackerDB"
    http = _client(peer_url, client)
    remote = _get_json(http, "/api/crdt/ops")
    remote_site = remote.get("site_id", "")
    if remote_site == db.site_id:
        raise TrackerError("peer reports our own site_id, refusing to sync with self")
    pulled = crdt.merge_ops(db, remote.get("ops", []))

    local_ops = crdt.ops_after(db.conn)
    try:
        response = http.post("/api/crdt/push", json={"site_id": db.site_id, "ops": local_ops})
    except httpx.HTTPError as exc:
        raise TrackerError(f"peer push failed: {exc}") from exc
    if response.status_code != 200:
        raise TrackerError(f"peer POST /api/crdt/push -> {response.status_code}")
    pushed = response.json()

    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO crdt_peers (url, last_pulled_hlc, last_synced) VALUES (?, ?, ?) "
            "ON CONFLICT(url) DO UPDATE SET last_pulled_hlc = excluded.last_pulled_hlc, "
            "last_synced = excluded.last_synced",
            (peer_url, crdt.latest_hlc(db.conn), utc_now_iso()),
        )
    counters = {
        "peer_site": remote_site,
        "pulled_new": pulled["new"],
        "pulled_applied": pulled["applied"],
        "pulled_deferred": pulled["deferred"],
        "pushed": len(local_ops),
        "pushed_new_on_peer": int(pushed.get("new", 0)),
    }
    assert counters["pulled_applied"] <= counters["pulled_new"], "applied exceeds new"
    return counters
