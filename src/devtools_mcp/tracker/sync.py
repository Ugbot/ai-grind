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

from urllib.parse import urlencode

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


MAX_SYNC_PAGES = 10_000


def _exchange(db: TrackerDB, peer_url: str, http: httpx.Client) -> dict:
    """Scan bounded pages from the beginning to include transitive older ops."""
    remote_site = ""
    after: str | None = None
    pulled = {"new": 0, "applied": 0, "deferred": 0}
    for _ in range(MAX_SYNC_PAGES):
        path = "/api/crdt/ops" + ("?" + urlencode({"after": after}) if after else "")
        remote = _get_json(http, path)
        site = remote.get("site_id")
        if not isinstance(site, str) or not site:
            raise TrackerError("peer did not report a site_id")
        if site == db.site_id:
            raise TrackerError("peer reports our own site_id; refusing to sync with self")
        if remote_site and remote_site != site:
            raise TrackerError("peer identity changed during sync")
        remote_site = site
        ops = remote.get("ops")
        if not isinstance(ops, list) or len(ops) > crdt.MAX_OPS_PER_BATCH:
            raise TrackerError("invalid peer op page")
        if not ops:
            break
        cursor = after
        for op in ops:
            stamp = op.get("hlc") if isinstance(op, dict) else None
            if not isinstance(stamp, str) or not stamp or (cursor is not None and stamp <= cursor):
                raise TrackerError("peer op cursor did not advance")
            cursor = stamp
        result = crdt.merge_ops(db, ops)
        for key in pulled:
            pulled[key] += result[key]
        after = cursor
    else:
        raise TrackerError("peer sync page limit reached; exchange incomplete")

    pushed_count = pushed_new = 0
    push_after: str | None = None
    for _ in range(MAX_SYNC_PAGES):
        local_ops = crdt.ops_after(db.conn, push_after)
        if not local_ops:
            break
        try:
            response = http.post("/api/crdt/push", json={"site_id": db.site_id, "ops": local_ops})
        except httpx.HTTPError as exc:
            raise TrackerError(f"peer push failed: {exc}") from exc
        if response.status_code != 200:
            raise TrackerError(f"peer POST /api/crdt/push -> {response.status_code}")
        pushed = response.json()
        pushed_count += len(local_ops)
        pushed_new += int(pushed.get("new", 0))
        push_after = local_ops[-1]["hlc"]
    else:
        raise TrackerError("peer sync page limit reached; exchange incomplete")

    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO crdt_peers (url, last_pulled_hlc, last_synced) VALUES (?, ?, ?) "
            "ON CONFLICT(url) DO UPDATE SET last_pulled_hlc = excluded.last_pulled_hlc, "
            "last_synced = excluded.last_synced",
            (peer_url, after, utc_now_iso()),
        )
    return {
        "peer_site": remote_site,
        "pulled_new": pulled["new"],
        "pulled_applied": pulled["applied"],
        "pulled_deferred": pulled["deferred"],
        "pushed": pushed_count,
        "pushed_new_on_peer": pushed_new,
    }


def sync_once(db: TrackerDB, peer_url: str, client: httpx.Client | None = None) -> dict:
    """Exchange all pages; retry interrupted exchanges safely via idempotent merge.

    Only a completed exchange updates the peer success watermark. A new exchange
    scans from the beginning because a peer can acquire older ops transitively.
    """
    assert db.conn is not None, "sync on closed TrackerDB"
    http = _client(peer_url, client)
    try:
        return _exchange(db, peer_url, http)
    finally:
        if client is None:
            http.close()
