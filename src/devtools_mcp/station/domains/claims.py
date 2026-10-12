"""Collab domain: local advisory file claims -> platform checkouts.

Push: every active local claim holds a platform checkout on the linked
repo, heartbeated each run with enough slack that the platform's 60s
reaper never expires it before the local lease does. Released/expired
local claims release their checkouts.

Pull (direction 'both'): other members' active checkouts are mirrored
wholesale into station_remote_checkouts. Never into file_claims, whose
unique active-claim index would let a remote row hard-block a local
acquire and silently change advisory semantics.
"""

from __future__ import annotations

import math
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from devtools_mcp.station import links
from devtools_mcp.station.client import StationClient
from devtools_mcp.station.config import StationConfig
from devtools_mcp.tracker import activity
from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso

CLAIM_TTL_SLACK_MINUTES: int = 5
CLAIM_TTL_MIN_MINUTES: int = 2
CLAIM_TTL_MAX_MINUTES: int = 480
CHECKOUT_MIRROR_MAX: int = 500


def config_repo_root(cfg: StationConfig) -> Path | None:
    """The repo holding station.toml, None when the config is the global file."""
    assert cfg.source_path, "config must record its source"
    source = Path(cfg.source_path)
    root = source.parent.parent  # <root>/.devtools-mcp/station.toml
    if root == Path.home():
        return None
    return root


def _ttl_minutes(expires_at: str) -> int:
    """Remote lease covering the local lease plus sync-interval slack."""
    assert expires_at, "expires_at must be non-empty"
    try:
        expires = datetime.fromisoformat(expires_at)
    except ValueError:
        return CLAIM_TTL_MIN_MINUTES
    remaining_s = (expires - datetime.now(UTC)).total_seconds()
    ttl = math.ceil(max(remaining_s, 0.0) / 60.0) + CLAIM_TTL_SLACK_MINUTES
    clamped = max(CLAIM_TTL_MIN_MINUTES, min(CLAIM_TTL_MAX_MINUTES, ttl))
    assert CLAIM_TTL_MIN_MINUTES <= clamped <= CLAIM_TTL_MAX_MINUTES, "ttl clamp failed"
    return clamped


def sync(
    db: TrackerDB,
    client: StationClient,
    cfg: StationConfig,
    project_row: sqlite3.Row,
    state_row: sqlite3.Row,
    dry_run: bool,
) -> dict:
    """Push local claims as checkouts; optionally mirror others' checkouts."""
    rule = cfg.rule("collab")
    repo_id = project_row["repo_id"]
    if not repo_id:
        raise TrackerError("collab domain has no platform repo, re-run station_link action='link'")
    repo_root = config_repo_root(cfg)
    if repo_root is None:
        raise TrackerError(
            "collab needs a repo-level station.toml (.devtools-mcp/station.toml in the repo), "
            "not the global config, claims are scoped to one repo"
        )
    report = {"domain": "collab", "pushed": 0, "pulled": 0, "conflicts": 0, "skipped": 0, "errors": 0, "notes": []}
    if rule.direction in ("push", "both"):
        # activity.normalize stores roots as resolved posix paths, match that form
        _push_claims(db, client, project_row, repo_root.resolve().as_posix(), rule.ttl_minutes, dry_run, report)
    if rule.direction in ("pull", "both") and not dry_run:
        _mirror_checkouts(db, client, project_row, report)
    return report


def _push_claims(
    db: TrackerDB,
    client: StationClient,
    project_row: sqlite3.Row,
    repo_root: str,
    default_ttl: int,
    dry_run: bool,
    report: dict,
) -> None:
    repo_id = project_row["repo_id"]
    org_id = project_row["org_id"]
    assert repo_id and org_id, "claims push needs a resolved repo and org"
    active = activity.active_claims(db.conn, repo_root)
    active_ids = {str(claim.id) for claim in active}
    for claim in active:  # bounded by activity.LIST_MAX
        link = links.get_link(db.conn, "claim", str(claim.id))
        ttl = _ttl_minutes(claim.expires_at) if claim.expires_at else default_ttl
        if dry_run:
            report["pushed"] += 1
            continue
        if link is None or link["state"] != "ok":
            response = client.checkouts_acquire(
                {
                    "repo_id": repo_id,
                    "paths": [claim.file_path.replace("\\", "/")],
                    "path_type": "file",
                    "mode": "exclusive",
                    "task_key": claim.task_key,
                    "intent": f"local claim by {claim.agent_label or claim.session_id[:16]}",
                    "ttl_minutes": ttl,
                }
            )
            acquired = response.get("acquired", [])
            conflicts = response.get("conflicts", [])
            if acquired:
                links.insert_link(db, "claim", str(claim.id), str(acquired[0]["id"]), org_id, None, None)
                report["pushed"] += 1
            for conflict in conflicts[:10]:  # bounded display
                report["conflicts"] += 1
                report["notes"].append(f"checkout conflict on {conflict['path']}: held by {conflict['member_id']}")
        else:
            client.checkouts_heartbeat([link["remote_id"]], ttl)
            report["pushed"] += 1
    _release_stale(db, client, org_id, active_ids, dry_run, report)


def _release_stale(
    db: TrackerDB, client: StationClient, org_id: str, active_ids: set[str], dry_run: bool, report: dict
) -> None:
    """Local claim released/expired -> release its checkout and drop the link."""
    stale = [
        link
        for link in links.links_for_domain(db.conn, "claim", org_id)
        if link["state"] == "ok" and link["local_id"] not in active_ids
    ]
    for link in stale:  # bounded by LINKS_MAX
        if not dry_run:
            client.checkouts_release([link["remote_id"]])
            links.mark_deleted(db, "claim", link["local_id"])
        report["pushed"] += 1
    if stale:
        report["notes"].append(f"released {len(stale)} stale checkout(s)")


def _mirror_checkouts(db: TrackerDB, client: StationClient, project_row: sqlite3.Row, report: dict) -> None:
    """Replace the read-only mirror of other members' active checkouts."""
    rows = client.checkouts_list(active_only=True)
    assert len(rows) <= CHECKOUT_MIRROR_MAX, "checkouts over mirror bound"
    me = project_row["member_id"]
    others = [row for row in rows if str(row.get("member_id", "")) != me]
    now = utc_now_iso()
    with db.transaction() as conn:
        conn.execute("DELETE FROM station_remote_checkouts WHERE org_id = ?", (project_row["org_id"],))
        for row in others:  # bounded by assert above
            conn.execute(
                "INSERT INTO station_remote_checkouts (remote_id, org_id, repo_id, member_id, path, "
                "path_type, mode, task_key, expires_at, pulled_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(row["id"]),
                    project_row["org_id"],
                    str(row["repo_id"]),
                    str(row["member_id"]),
                    str(row["path"]),
                    str(row.get("path_type", "file")),
                    str(row.get("mode", "exclusive")),
                    row.get("task_key"),
                    str(row.get("expires_at") or ""),
                    now,
                ),
            )
    report["pulled"] += len(others)


def remote_conflicts_for(conn: sqlite3.Connection, path: str) -> list[dict]:
    """Other members' mirrored checkouts touching `path`, advisory display only."""
    assert path, "path must be non-empty"
    normalized = path.replace("\\", "/")
    rows = conn.execute(
        "SELECT member_id, path, mode, task_key, expires_at FROM station_remote_checkouts "
        "WHERE path = ? COLLATE NOCASE LIMIT 20",
        (normalized,),
    ).fetchall()
    assert len(rows) <= 20, "remote conflicts over bound"
    return [dict(row) for row in rows]
