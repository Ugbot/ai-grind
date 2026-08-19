"""run_sync: orchestrate per-domain station sync for one linked project.

The engine owns what domains must not: the single-flight lock, watermark
persistence, failure counting with auto-pause, and the bounded sync log.
Stateless re-diff means there is no offline queue anywhere, a failed run
advances nothing and the next run recomputes from durable state.
"""

from __future__ import annotations

import sqlite3
import threading

from devtools_mcp.station import project_link
from devtools_mcp.station.client import StationClient, StationError
from devtools_mcp.station.config import StationConfig
from devtools_mcp.station.domains import DOMAIN_SYNCERS
from devtools_mcp.station.domains import tasks as tasks_domain
from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso

STATION_MAX_CONSECUTIVE_FAILURES: int = 10
STATION_SYNC_LOG_KEEP: int = 500
_SYNC_LOCK = threading.Lock()


def _get_state(conn: sqlite3.Connection, rule_id: str) -> sqlite3.Row:
    assert rule_id, "rule_id must be non-empty"
    conn.execute("INSERT OR IGNORE INTO station_sync_state (rule_id) VALUES (?)", (rule_id,))
    row = conn.execute("SELECT * FROM station_sync_state WHERE rule_id = ?", (rule_id,)).fetchone()
    assert row is not None, f"sync state row missing after ensure: {rule_id}"
    return row


def _record_success(db: TrackerDB, rule_id: str, new_watermark: str | None) -> None:
    with db.transaction() as conn:
        conn.execute(
            "UPDATE station_sync_state SET last_push_hlc = COALESCE(?, last_push_hlc), "
            "last_pull_at = ?, last_run_at = ?, consecutive_failures = 0, last_error = NULL "
            "WHERE rule_id = ?",
            (new_watermark, utc_now_iso(), utc_now_iso(), rule_id),
        )


def _record_failure(db: TrackerDB, rule_id: str, error: str) -> bool:
    """Bump the failure counter; returns True when the rule auto-paused."""
    assert error, "failure needs an error message"
    with db.transaction() as conn:
        conn.execute(
            "UPDATE station_sync_state SET last_run_at = ?, last_error = ?, "
            "consecutive_failures = consecutive_failures + 1, "
            "paused = CASE WHEN consecutive_failures + 1 >= ? THEN 1 ELSE paused END "
            "WHERE rule_id = ?",
            (utc_now_iso(), error[:500], STATION_MAX_CONSECUTIVE_FAILURES, rule_id),
        )
        row = conn.execute("SELECT paused FROM station_sync_state WHERE rule_id = ?", (rule_id,)).fetchone()
    assert row is not None, "sync state vanished during failure record"
    return bool(row["paused"])


def _write_log(db: TrackerDB, rule_id: str, started_at: str, report: dict, error: str | None) -> None:
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO station_sync_log (rule_id, started_at, finished_at, pushed, pulled, "
            "conflicts, skipped, errors, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                rule_id,
                started_at,
                utc_now_iso(),
                int(report.get("pushed", 0)),
                int(report.get("pulled", 0)),
                int(report.get("conflicts", 0)),
                int(report.get("skipped", 0)),
                int(report.get("errors", 0)),
                error,
            ),
        )
        conn.execute(
            "DELETE FROM station_sync_log WHERE id NOT IN "
            "(SELECT id FROM station_sync_log ORDER BY id DESC LIMIT ?)",
            (STATION_SYNC_LOG_KEEP,),
        )


def resume_rule(db: TrackerDB, rule_id: str) -> None:
    """Un-pause a rule after the operator fixed whatever kept failing."""
    assert rule_id, "rule_id must be non-empty"
    with db.transaction() as conn:
        conn.execute(
            "UPDATE station_sync_state SET paused = 0, consecutive_failures = 0 WHERE rule_id = ?",
            (rule_id,),
        )


def run_sync(
    db: TrackerDB,
    cfg: StationConfig,
    domains: tuple[str, ...] | None = None,
    dry_run: bool = False,
    client: StationClient | None = None,
) -> list[dict]:
    """Sync every enabled (or requested) domain once. Returns one report per domain.

    `client` is an injection seam for tests (httpx.MockTransport); when
    provided the caller owns its lifecycle.
    """
    assert db.conn is not None, "run_sync on closed tracker db"
    if not _SYNC_LOCK.acquire(blocking=False):
        raise TrackerError("a station sync is already running, try again when it finishes")
    try:
        return _run_sync_locked(db, cfg, domains, dry_run, client)
    finally:
        _SYNC_LOCK.release()


def _run_sync_locked(
    db: TrackerDB,
    cfg: StationConfig,
    domains: tuple[str, ...] | None,
    dry_run: bool,
    injected: StationClient | None = None,
) -> list[dict]:
    project_key = cfg.project.local.strip().upper()
    row = project_link.get_project_link(db.conn, project_key)
    if row is None:
        raise TrackerError(f"Project {project_key} is not linked. Run station_link action='link' first")
    if row["config_hash"] != cfg.config_hash():
        raise TrackerError(f"station.toml changed since {project_key} was linked, re-run station_link action='link'")
    wanted = domains if domains is not None else cfg.enabled_domains()
    unknown = set(wanted) - set(DOMAIN_SYNCERS)
    if unknown:
        raise TrackerError(f"Unknown domains: {', '.join(sorted(unknown))}")
    reports: list[dict] = []
    client = injected if injected is not None else StationClient(row["base_url"], cfg.api_key(), row["org_id"])
    try:
        _recover_pending(db, client, row, reports)
        for domain in wanted:  # bounded: <= 5 domains
            if not cfg.rule(domain).enabled:
                reports.append({"domain": domain, "notes": ["disabled by config, skipped"]})
                continue
            reports.append(_run_domain(db, client, cfg, domain, row, dry_run))
    finally:
        if injected is None:
            client.close()
    assert len(reports) <= len(DOMAIN_SYNCERS) + 1, "report count over bound"
    return reports


def _recover_pending(db: TrackerDB, client: StationClient, project_row: sqlite3.Row, reports: list[dict]) -> None:
    """Resolve pending-intent task links left by a crash before any domain runs."""
    try:
        resolved = tasks_domain.resolve_pending(db, client, project_row)
    except StationError as exc:
        reports.append({"domain": "recovery", "errors": 1, "notes": [f"pending resolve failed: {exc}"]})
        return
    if resolved:
        reports.append({"domain": "recovery", "notes": [f"resolved {resolved} pending task link(s)"]})


def _run_domain(
    db: TrackerDB,
    client: StationClient,
    cfg: StationConfig,
    domain: str,
    project_row: sqlite3.Row,
    dry_run: bool,
) -> dict:
    rule_id = f"{project_row['project_key']}:{domain}"
    state = _get_state(db.conn, rule_id)
    if state["paused"]:
        return {
            "domain": domain,
            "notes": [f"paused after repeated failures ({state['last_error']}), station_link action='resume'"],
        }
    started_at = utc_now_iso()
    try:
        report = DOMAIN_SYNCERS[domain](db, client, cfg, project_row, state, dry_run)
    except (StationError, TrackerError) as exc:
        paused = _record_failure(db, rule_id, str(exc))
        _write_log(db, rule_id, started_at, {"errors": 1}, str(exc)[:500])
        note = ", rule auto-paused" if paused else ""
        return {"domain": domain, "errors": 1, "notes": [f"{exc}{note}"]}
    if not dry_run:
        old = state["last_push_hlc"]
        new = report.get("new_watermark")
        assert new is None or old is None or new >= old, "watermark went backwards"
        _record_success(db, rule_id, new)
        _write_log(db, rule_id, started_at, report, None)
    return report
