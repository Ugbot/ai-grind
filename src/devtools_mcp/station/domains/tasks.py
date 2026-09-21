"""Tasks domain: bidirectional sync between local tracker tasks and the
platform's /orgs/{o}/projects/{p}/tasks.

Local change feed: the CRDT op-log (diff.task_changes). Remote change feed:
bounded page scan + canonical-hash diff (the platform has no updated_since).
Conflict policy: row-level local-wins by default (config: remote_wins).
Run order is pull -> push (the engine calls sync() once; we pull first),
so a conflicted row skipped by pull is overwritten by push in the same run
one run reaches fixpoint.

Platform constraints honored here:
- keys are allocated server-side; pulled tasks get fresh LOCAL keys too
- kind/parent_id are create-only on the platform: changes are counted as
  skipped_immutable, never silently diverged
- there is no DELETE endpoint: local deletes push status=cancelled
"""

from __future__ import annotations

import sqlite3

from devtools_mcp.station import diff, links
from devtools_mcp.station.client import TASKS_PAGE_MAX, StationClient, StationError
from devtools_mcp.station.config import DomainRule, StationConfig
from devtools_mcp.tracker import tasks as tasks_mod
from devtools_mcp.tracker.db import TrackerDB

STATION_MAX_PUSH_PER_RUN: int = 500
_TRANSIENT_STATUS_MIN: int = 500  # >=500 or 0 (transport) aborts the run


def _hash_fields(title: str, description: str | None, status: str, priority: int | None) -> dict:
    """The synced field set, normalized so both sides hash identically."""
    assert title, "task title must be non-empty"
    assert status, "task status must be non-empty"
    return {
        "title": title,
        "description": description or "",
        "status": status,
        "priority": int(priority) if priority is not None else 3,
    }


def _local_hash(row: sqlite3.Row) -> str:
    return links.canonical_hash(_hash_fields(row["title"], row["description"], row["status"], row["priority"]))


def _remote_hash(remote: dict) -> str:
    return links.canonical_hash(
        _hash_fields(remote["title"], remote.get("description"), remote["status"], remote.get("priority"))
    )


def _is_transient(exc: StationError) -> bool:
    return exc.status_code == 0 or exc.status_code >= _TRANSIENT_STATUS_MIN


def sync(
    db: TrackerDB,
    client: StationClient,
    cfg: StationConfig,
    project_row: sqlite3.Row,
    state_row: sqlite3.Row,
    dry_run: bool,
) -> dict:
    """One tasks sync run for one project rule. Returns counters."""
    assert project_row["remote_project_id"], "unlinked project reached tasks sync"
    rule = cfg.rule("tasks")
    report = {
        "domain": "tasks",
        "pushed": 0,
        "pulled": 0,
        "conflicts": 0,
        "skipped": 0,
        "errors": 0,
        "deferred": 0,
        "new_watermark": state_row["last_push_hlc"],
        "notes": [],
    }
    if rule.direction in ("pull", "both"):
        _pull(db, client, rule, project_row, state_row, dry_run, report)
    if rule.direction in ("push", "both"):
        _push(db, client, rule, project_row, state_row, dry_run, report)
    assert report["pushed"] <= STATION_MAX_PUSH_PER_RUN, "push count over bound"
    assert report["pulled"] <= TASKS_PAGE_MAX, "pull count over bound"
    return report


# -- pull ---------------------------------------------------------------------


def _pull(
    db: TrackerDB,
    client: StationClient,
    rule: DomainRule,
    project_row: sqlite3.Row,
    state_row: sqlite3.Row,
    dry_run: bool,
    report: dict,
) -> None:
    org_id = project_row["org_id"]
    project_key = project_row["project_key"]
    remote_rows = client.tasks_list(project_row["remote_project_id"])
    truncated = len(remote_rows) >= TASKS_PAGE_MAX
    dirty = diff.dirty_task_uids(db.conn, state_row["last_push_hlc"], project_key)
    seen_remote_ids: set[str] = set()
    for remote in sorted(remote_rows, key=lambda r: (r.get("depth", 0), r.get("key", ""))):  # parents first
        seen_remote_ids.add(str(remote["id"]))
        remote_hash = _remote_hash(remote)
        link = links.link_by_remote(db.conn, "task", org_id, str(remote["id"]))
        if link is None:
            if not dry_run:
                _pull_create(db, org_id, project_key, remote, remote_hash, report)
            report["pulled"] += 1
            continue
        if link["synced_hash"] == remote_hash or link["state"] == "deleted":
            continue
        local = db.conn.execute("SELECT * FROM tasks WHERE uid = ?", (link["local_id"],)).fetchone()
        if local is None:
            report["conflicts"] += 1  # locally deleted, remotely edited: local delete wins
            continue
        if link["local_id"] in dirty and rule.on_conflict == "local_wins":
            report["conflicts"] += 1  # push below overwrites remote this same run
            continue
        if not dry_run:
            _pull_update(db, local, remote, remote_hash)
        report["pulled"] += 1
    if not truncated and not dry_run:
        _mark_disappeared(db, org_id, seen_remote_ids, report)
    elif truncated:
        report["notes"].append(f"remote page at cap ({TASKS_PAGE_MAX}), disappearance check skipped")


def _pull_create(db: TrackerDB, org_id: str, project_key: str, remote: dict, remote_hash: str, report: dict) -> None:
    """Create a local task for a new remote row (fresh local key + uid)."""
    parent_key: str | None = None
    if remote.get("parent_id"):
        parent_link = links.link_by_remote(db.conn, "task", org_id, str(remote["parent_id"]))
        if parent_link is not None:
            row = db.conn.execute("SELECT key FROM tasks WHERE uid = ?", (parent_link["local_id"],)).fetchone()
            parent_key = row["key"] if row else None
        if parent_key is None:
            report["notes"].append(f"remote {remote.get('key', '?')}: parent unlinked, created at root")
    task, _tags = tasks_mod.create_task(
        db,
        project_key,
        remote["title"],
        description=remote.get("description") or "",
        kind=remote.get("kind", "task"),
        parent_key=parent_key,
        priority=int(remote["priority"]) if remote.get("priority") is not None else 3,
    )
    if remote["status"] != "open":
        tasks_mod.set_status(db, task.key, remote["status"], override=True)
    links.insert_link(db, "task", task.uid, str(remote["id"]), org_id, str(remote.get("key", "")), remote_hash)


def _pull_update(db: TrackerDB, local: sqlite3.Row, remote: dict, remote_hash: str) -> None:
    """Apply remote field values to an unlinked-clean local task (capture ON)."""
    tasks_mod.update_task(
        db,
        local["key"],
        title=remote["title"],
        description=remote.get("description") or "",
        priority=int(remote["priority"]) if remote.get("priority") is not None else 3,
    )
    if remote["status"] != local["status"]:
        tasks_mod.set_status(db, local["key"], remote["status"], override=True)
    links.update_hash(db, "task", local["uid"], remote_hash)


def _mark_disappeared(db: TrackerDB, org_id: str, seen_remote_ids: set[str], report: dict) -> None:
    """Remote rows gone from a full page: mark links deleted, local untouched."""
    gone = 0
    for link in links.links_for_domain(db.conn, "task", org_id):  # bounded by LINKS_MAX
        if link["state"] != "ok" or link["remote_id"].startswith(links.PENDING_PREFIX):
            continue
        if link["remote_id"] not in seen_remote_ids:
            links.mark_deleted(db, "task", link["local_id"])
            gone += 1
    if gone:
        report["notes"].append(f"{gone} remote task(s) disappeared, links marked deleted, local kept")


# -- push ---------------------------------------------------------------------


def _push(
    db: TrackerDB,
    client: StationClient,
    rule: DomainRule,
    project_row: sqlite3.Row,
    state_row: sqlite3.Row,
    dry_run: bool,
    report: dict,
) -> None:
    project_key = project_row["project_key"]
    changes, batch_watermark = diff.task_changes(db.conn, state_row["last_push_hlc"], project_key)
    ordered = sorted(changes, key=lambda c: (_local_depth(db.conn, c["uid"]), c["hlc"]))
    for change in ordered[:STATION_MAX_PUSH_PER_RUN]:  # bounded
        try:
            _push_one(db, client, rule, project_row, change, dry_run, report)
        except StationError as exc:
            if _is_transient(exc):
                report["notes"].append(f"transport failure, push halted: {exc.detail[:120]}")
                report["errors"] += 1
                return  # watermark stays, re-diff next run
            local = db.conn.execute("SELECT * FROM tasks WHERE uid = ?", (change["uid"],)).fetchone()
            attempted = _local_hash(local) if local is not None else None
            links.mark_error(db, "task", change["uid"], project_row["org_id"], str(exc), attempted)
            report["errors"] += 1
    if report["deferred"] == 0 and not dry_run and batch_watermark is not None:
        report["new_watermark"] = batch_watermark


def _local_depth(conn: sqlite3.Connection, uid: str) -> int:
    row = conn.execute("SELECT depth FROM tasks WHERE uid = ?", (uid,)).fetchone()
    depth = row["depth"] if row is not None else 99  # deletes/missing rows go last
    assert 0 <= depth <= 99, f"depth out of bounds: {depth}"
    return depth


def _push_one(
    db: TrackerDB,
    client: StationClient,
    rule: DomainRule,
    project_row: sqlite3.Row,
    change: dict,
    dry_run: bool,
    report: dict,
) -> None:
    org_id = project_row["org_id"]
    remote_project_id = project_row["remote_project_id"]
    uid = change["uid"]
    link = links.get_link(db.conn, "task", uid)
    local = db.conn.execute("SELECT * FROM tasks WHERE uid = ?", (uid,)).fetchone()
    if local is None:  # deleted locally (or delete op)
        if link is not None and link["state"] == "ok" and not link["remote_id"].startswith(links.PENDING_PREFIX):
            if not dry_run:
                client.task_update(remote_project_id, link["remote_id"], {"status": "cancelled"})
                links.mark_deleted(db, "task", uid)
            report["pushed"] += 1
        return
    if rule.kinds and local["kind"] not in rule.kinds:
        report["skipped"] += 1
        return
    local_hash = _local_hash(local)
    if link is not None and link["synced_hash"] == local_hash and link["state"] in ("ok", "error"):
        report["skipped"] += 1  # echo of our own pull-apply, or unchanged quarantined row
        return
    needs_create = (
        link is None
        or link["state"] == "pending"
        or link["remote_id"].startswith(links.PENDING_PREFIX)
        or link["remote_id"].startswith("error:")  # quarantined before the row ever existed remotely
    )
    if needs_create:
        _push_create(db, client, org_id, remote_project_id, local, local_hash, dry_run, report)
    else:
        assert link is not None, "update path requires an existing link"
        if not dry_run:
            client.task_update(
                remote_project_id,
                link["remote_id"],
                {
                    "title": local["title"],
                    "description": local["description"],
                    "status": local["status"],
                    "priority": int(local["priority"]),
                },
            )
            links.update_hash(db, "task", uid, local_hash)
        report["pushed"] += 1


def _push_create(
    db: TrackerDB,
    client: StationClient,
    org_id: str,
    remote_project_id: str,
    local: sqlite3.Row,
    local_hash: str,
    dry_run: bool,
    report: dict,
) -> None:
    """Create on the platform with pending-intent crash protection."""
    parent_remote_id: str | None = None
    if local["parent_id"] is not None:
        parent = db.conn.execute("SELECT uid FROM tasks WHERE id = ?", (local["parent_id"],)).fetchone()
        parent_link = links.get_link(db.conn, "task", parent["uid"]) if parent else None
        if parent_link is None or parent_link["remote_id"].startswith(links.PENDING_PREFIX):
            report["deferred"] += 1  # parent not linked yet: retry next run (watermark held)
            return
        parent_remote_id = parent_link["remote_id"]
    if dry_run:
        report["pushed"] += 1
        return
    links.put_pending(db, "task", local["uid"], org_id)
    body: dict = {
        "title": local["title"],
        "kind": local["kind"],
        "description": local["description"],
        "priority": int(local["priority"]),
    }
    if parent_remote_id is not None:
        body["parent_id"] = parent_remote_id
    remote = client.task_create(remote_project_id, body)
    open_hash = links.canonical_hash(_hash_fields(local["title"], local["description"], "open", local["priority"]))
    links.resolve_link(db, "task", local["uid"], str(remote["id"]), str(remote.get("key", "")), open_hash)
    if local["status"] != "open":
        client.task_update(remote_project_id, str(remote["id"]), {"status": local["status"]})
        links.update_hash(db, "task", local["uid"], local_hash)
    report["pushed"] += 1


# -- pending recovery -----------------------------------------------------------


def resolve_pending(db: TrackerDB, client: StationClient, project_row: sqlite3.Row, limit: int = 100) -> int:
    """Recover pending-intent links from a crash between POST and resolve.

    Exact-title match within the mapped remote project resolves the link;
    no match means the POST never landed, drop the pending row so the next
    push re-creates cleanly. Ambiguity (duplicate titles) quarantines.
    """
    assert 1 <= limit <= 1000, f"limit out of range: {limit}"
    pending = links.pending_links(db.conn, "task", limit)
    if not pending:
        return 0
    remote_rows = client.tasks_list(project_row["remote_project_id"])
    by_title: dict[str, list[dict]] = {}
    for remote in remote_rows:  # bounded by TASKS_PAGE_MAX
        by_title.setdefault(remote["title"], []).append(remote)
    resolved = 0
    for link in pending:  # bounded by limit
        local = db.conn.execute("SELECT * FROM tasks WHERE uid = ?", (link["local_id"],)).fetchone()
        if local is None:
            links.mark_deleted(db, "task", link["local_id"])
            continue
        candidates = by_title.get(local["title"], [])
        unlinked = [
            r for r in candidates if links.link_by_remote(db.conn, "task", project_row["org_id"], str(r["id"])) is None
        ]
        if len(unlinked) == 1:
            links.resolve_link(
                db, "task", link["local_id"], str(unlinked[0]["id"]), str(unlinked[0].get("key", "")), None
            )
            resolved += 1
        elif len(unlinked) > 1:
            links.mark_error(
                db,
                "task",
                link["local_id"],
                project_row["org_id"],
                f"pending resolve ambiguous: {len(unlinked)} remote tasks titled {local['title']!r}",
                None,
            )
        else:
            with db.transaction() as conn:  # POST never landed: clean retry next push
                conn.execute(
                    "DELETE FROM station_links WHERE domain = 'task' AND local_id = ?",
                    (link["local_id"],),
                )
    assert resolved <= len(pending), "resolved more than pending"
    return resolved
