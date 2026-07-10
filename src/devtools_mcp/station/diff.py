"""Local change detection for station sync.

Tracker domains reuse the existing CRDT op-log as a durable change feed:
crdt_ops already captures every mutation (schema v3 triggers), so "what
changed since the last push" is ops_after(last_push_hlc) grouped to the
latest op per row. Non-op-logged tables (claims, skills, perf runs) use
bounded table scans diffed against station_links by the domain modules.

Each rule keeps its own watermark, so filtering another project's ops out
of a batch never loses them — that project's rule scans with its own HLC.
"""

from __future__ import annotations

import json
import sqlite3

STATION_MAX_OPS_PER_RUN: int = 2_000


def task_changes(conn: sqlite3.Connection, after_hlc: str | None, project_key: str) -> tuple[list[dict], str | None]:
    """Latest task op per uid for one project, plus the batch watermark.

    Returns (rows, max_hlc_consumed). Delete ops have no payload (the
    trigger can't see the vanished row) so they carry only the uid; the
    caller decides relevance by link existence. max_hlc_consumed covers the
    whole fetched batch — including other projects' ops — which is safe
    because watermarks are per (project, domain) rule.
    """
    assert project_key, "project_key must be non-empty"
    if after_hlc is None:
        rows = conn.execute(
            "SELECT hlc, pk, op, payload FROM crdt_ops WHERE tbl = 'tasks' ORDER BY hlc LIMIT ?",
            (STATION_MAX_OPS_PER_RUN,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT hlc, pk, op, payload FROM crdt_ops WHERE tbl = 'tasks' AND hlc > ? " "ORDER BY hlc LIMIT ?",
            (after_hlc, STATION_MAX_OPS_PER_RUN),
        ).fetchall()
    assert len(rows) <= STATION_MAX_OPS_PER_RUN, "ops fetch over bound"
    if not rows:
        return [], after_hlc
    latest: dict[str, dict] = {}
    for row in rows:  # bounded by STATION_MAX_OPS_PER_RUN; hlc-ascending, last wins
        payload = json.loads(row["payload"]) if row["payload"] else None
        if payload is not None and payload.get("project") != project_key:
            continue
        latest[row["pk"]] = {"uid": row["pk"], "hlc": row["hlc"], "op": row["op"], "payload": payload}
    max_hlc = rows[-1]["hlc"]
    assert after_hlc is None or max_hlc > after_hlc, "watermark must advance past fetched ops"
    return list(latest.values()), max_hlc


def dirty_task_uids(conn: sqlite3.Connection, after_hlc: str | None, project_key: str) -> set[str]:
    """Uids with local edits after the watermark — the pull conflict set."""
    changed, _ = task_changes(conn, after_hlc, project_key)
    uids = {row["uid"] for row in changed}
    assert len(uids) <= STATION_MAX_OPS_PER_RUN, "dirty set over bound"
    return uids
