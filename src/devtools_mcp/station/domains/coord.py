"""Sessions/handoffs domain: push local agent sessions, mirror pending
handoffs.

Sessions are push-only (the platform allows owner-only PATCH): a local
session seen in file_activity within the active window gets a platform
session; a linked session idle beyond the window is completed remotely.
Handoff creation/accept/decline are explicit station_session verbs, not
sync rules — the sync part only mirrors *pending* handoffs into
station_remote_handoffs for display.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

from devtools_mcp.station import links
from devtools_mcp.station.client import StationClient
from devtools_mcp.station.config import StationConfig
from devtools_mcp.tracker import activity
from devtools_mcp.tracker.db import TrackerDB, utc_now_iso

SESSION_ACTIVE_WINDOW_MINUTES: int = 30
SESSIONS_MAX_PER_RUN: int = 50
HANDOFFS_MIRROR_MAX: int = 200


def _is_active(last_seen: str) -> bool:
    assert last_seen, "last_seen must be non-empty"
    try:
        seen = datetime.fromisoformat(last_seen)
    except ValueError:
        return False
    return datetime.now(UTC) - seen <= timedelta(minutes=SESSION_ACTIVE_WINDOW_MINUTES)


def sync(
    db: TrackerDB,
    client: StationClient,
    cfg: StationConfig,
    project_row: sqlite3.Row,
    state_row: sqlite3.Row,
    dry_run: bool,
) -> dict:
    """Push active sessions, complete idle ones, mirror pending handoffs."""
    assert project_row["org_id"], "unlinked project reached coord sync"
    rule = cfg.rule("sessions")
    report = {"domain": "sessions", "pushed": 0, "pulled": 0, "conflicts": 0, "skipped": 0, "errors": 0, "notes": []}
    if rule.direction in ("push", "both"):
        _push_sessions(db, client, project_row, dry_run, report)
    if not dry_run:
        _mirror_pending_handoffs(db, client, project_row, report)
    return report


def _push_sessions(db: TrackerDB, client: StationClient, project_row: sqlite3.Row, dry_run: bool, report: dict) -> None:
    overview = activity.sessions_overview(db.conn)
    org_id = project_row["org_id"]
    linked = {row["local_id"]: row for row in links.links_for_domain(db.conn, "session", org_id)}
    for session in overview[:SESSIONS_MAX_PER_RUN]:  # bounded
        session_id = session["session_id"]
        link = linked.pop(session_id, None)
        if _is_active(session["last_seen"]):
            if link is None or link["state"] != "ok":
                if not dry_run:
                    remote = client.session_start(
                        {
                            "project_id": project_row["remote_project_id"],
                            "context": {"agent_label": session["agent_label"] or session_id[:16]},
                        }
                    )
                    links.insert_link(db, "session", session_id, str(remote["id"]), org_id, None, None)
                report["pushed"] += 1
        elif link is not None and link["state"] == "ok":
            if not dry_run:
                client.session_update(link["remote_id"], {"status": "completed"})
                links.mark_deleted(db, "session", session_id)
            report["pushed"] += 1
    # linked sessions with no activity rows at all anymore: complete them too
    for session_id, link in linked.items():  # bounded by LINKS_MAX
        if link["state"] != "ok":
            continue
        if not dry_run:
            client.session_update(link["remote_id"], {"status": "completed"})
            links.mark_deleted(db, "session", session_id)
        report["pushed"] += 1


def _mirror_pending_handoffs(db: TrackerDB, client: StationClient, project_row: sqlite3.Row, report: dict) -> None:
    """Replace the local read-only mirror of pending handoffs wholesale."""
    pending = client.handoffs_pending()
    assert len(pending) <= HANDOFFS_MIRROR_MAX, "pending handoffs over mirror bound"
    now = utc_now_iso()
    with db.transaction() as conn:
        conn.execute("DELETE FROM station_remote_handoffs WHERE org_id = ?", (project_row["org_id"],))
        for handoff in pending:  # bounded by assert above
            conn.execute(
                "INSERT INTO station_remote_handoffs (remote_id, org_id, from_member_id, task_key, "
                "status, context, next_steps, created_at, pulled_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(handoff["id"]),
                    project_row["org_id"],
                    str(handoff.get("from_member_id", "")),
                    handoff.get("task_key"),
                    str(handoff.get("status", "pending")),
                    (handoff.get("context") or "")[:2000],
                    (handoff.get("next_steps") or "")[:2000],
                    str(handoff.get("created_at", "")),
                    now,
                ),
            )
    report["pulled"] += len(pending)
    if pending:
        report["notes"].append(f"{len(pending)} pending handoff(s) mirrored — station_session action='inbox'")
