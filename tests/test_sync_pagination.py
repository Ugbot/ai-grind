"""Multi-page replication through the HTTP transport contract."""

import json

import httpx
import pytest

from devtools_mcp.tracker import crdt, tasks
from devtools_mcp.tracker.db import TrackerError, open_tracker
from devtools_mcp.tracker.sync import sync_once


@pytest.mark.parametrize("interrupt", [False, True])
def test_multiple_pages_converge_and_retry(tmp_path, monkeypatch, interrupt):
    monkeypatch.setattr(crdt, "MAX_OPS_PER_BATCH", 3)
    a = open_tracker(tmp_path / "a.db")
    b = open_tracker(tmp_path / "b.db")
    tasks.create_project(a, "AA", "A")
    tasks.create_project(b, "BB", "B")
    for i in range(8):
        tasks.create_task(a, "AA", f"a-{i}")
        tasks.create_task(b, "BB", f"b-{i}")
    fail_once = interrupt

    def handler(request):
        nonlocal fail_once
        if request.method == "GET":
            after = request.url.params.get("after")
            return httpx.Response(200, json={"site_id": b.site_id, "ops": crdt.ops_after(b.conn, after)})
        if fail_once:
            fail_once = False
            return httpx.Response(503)
        return httpx.Response(200, json=crdt.merge_ops(b, json.loads(request.content)["ops"]))

    try:
        with httpx.Client(base_url="http://peer", transport=httpx.MockTransport(handler)) as client:
            if interrupt:
                with pytest.raises(TrackerError, match="503"):
                    sync_once(a, "http://peer", client)
                assert a.conn.execute("SELECT COUNT(*) FROM crdt_peers").fetchone()[0] == 0
            counters = sync_once(a, "http://peer", client)
            assert counters["pushed"] > 3
            assert crdt.canonical_state(a.conn) == crdt.canonical_state(b.conn)
            assert sync_once(a, "http://peer", client)["pulled_new"] == 0
    finally:
        a.close()
        b.close()


def test_nonadvancing_peer_is_rejected(tmp_path):
    a = open_tracker(tmp_path / "a.db")
    b = open_tracker(tmp_path / "b.db")
    tasks.create_project(b, "BB", "B")
    page = {"site_id": b.site_id, "ops": crdt.ops_after(b.conn)}
    try:
        with (
            httpx.Client(
                base_url="http://peer", transport=httpx.MockTransport(lambda request: httpx.Response(200, json=page))
            ) as client,
            pytest.raises(TrackerError, match="cursor"),
        ):
            sync_once(a, "http://peer", client)
    finally:
        a.close()
        b.close()


def test_next_exchange_finds_transitive_older_ops(tmp_path):
    a = open_tracker(tmp_path / "a.db")
    b = open_tracker(tmp_path / "b.db")
    c = open_tracker(tmp_path / "c.db")
    tasks.create_project(c, "CC", "Older independent project")
    old = crdt.ops_after(c.conn)
    tasks.create_project(b, "BB", "Newer")

    def handler(request):
        if request.method == "GET":
            return httpx.Response(
                200, json={"site_id": b.site_id, "ops": crdt.ops_after(b.conn, request.url.params.get("after"))}
            )
        return httpx.Response(200, json=crdt.merge_ops(b, json.loads(request.content)["ops"]))

    try:
        with httpx.Client(base_url="http://peer", transport=httpx.MockTransport(handler)) as client:
            sync_once(a, "http://peer", client)
            crdt.merge_ops(b, old)
            assert sync_once(a, "http://peer", client)["pulled_new"] == 1
            assert crdt.canonical_state(a.conn) == crdt.canonical_state(b.conn)
    finally:
        a.close()
        b.close()
        c.close()
