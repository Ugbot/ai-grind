"""Perf domain: push profiling runs from the local run store to the
platform's perf-runs API. Push-only and immutable.

Idempotency: every upload is tagged `local-run:{run_id}` (run_ids are
client-generated), so a crash between upload and link-commit is recovered
by re-listing remote runs and rebuilding links from tags — no duplicate
uploads.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from devtools_mcp.station import links
from devtools_mcp.station.client import StationClient
from devtools_mcp.station.config import StationConfig
from devtools_mcp.store.run_store import RunStore
from devtools_mcp.tracker.db import TrackerDB

LOCAL_RUN_TAG_PREFIX: str = "local-run:"
PERF_DATA_MAX_BYTES: int = 1_048_576  # 1 MiB
PERF_PUSH_MAX_PER_RUN: int = 50
RUNS_SCAN_MAX: int = 2_000


def _run_meta(store: RunStore, run_id: str) -> dict | None:
    meta_path = store.runs_path / run_id / "meta.json"
    if not meta_path.is_file():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    assert isinstance(meta, dict), "run meta must be an object"
    return meta


def _recover_links(db: TrackerDB, client: StationClient, org_id: str, unlinked: set[str]) -> int:
    """Rebuild links for runs already uploaded (crash between POST and link)."""
    assert unlinked, "recover called with nothing to recover"
    remote_runs = client.perf_list()
    recovered = 0
    for run in remote_runs:  # bounded by PERF_PAGE_MAX
        for tag in run.get("tags", [])[:20]:  # bounded tag scan
            if tag.startswith(LOCAL_RUN_TAG_PREFIX) and tag[len(LOCAL_RUN_TAG_PREFIX) :] in unlinked:
                links.insert_link(db, "perf_run", tag[len(LOCAL_RUN_TAG_PREFIX) :], str(run["id"]), org_id, None, None)
                recovered += 1
    assert recovered <= len(unlinked), "recovered more than unlinked"
    return recovered


def sync(
    db: TrackerDB,
    client: StationClient,
    cfg: StationConfig,
    project_row: sqlite3.Row,
    state_row: sqlite3.Row,
    dry_run: bool,
) -> dict:
    """Upload unlinked local runs (suite-filtered), newest last, bounded."""
    assert project_row["org_id"], "unlinked project reached perf sync"
    rule = cfg.rule("perf")
    org_id = project_row["org_id"]
    report: dict[str, Any] = {
        "domain": "perf",
        "pushed": 0,
        "pulled": 0,
        "conflicts": 0,
        "skipped": 0,
        "errors": 0,
        "notes": [],
    }
    store = RunStore()
    run_ids = store.list_run_ids()[:RUNS_SCAN_MAX]
    linked = {row["local_id"] for row in links.links_for_domain(db.conn, "perf_run", org_id)}
    unlinked = [run_id for run_id in run_ids if run_id not in linked]
    if not unlinked:
        report["skipped"] = len(run_ids)
        return report
    if not dry_run:
        recovered = _recover_links(db, client, org_id, set(unlinked))
        if recovered:
            report["notes"].append(f"recovered {recovered} already-uploaded run link(s) from tags")
            linked = {row["local_id"] for row in links.links_for_domain(db.conn, "perf_run", org_id)}
            unlinked = [run_id for run_id in unlinked if run_id not in linked]
    for run_id in unlinked[:PERF_PUSH_MAX_PER_RUN]:  # bounded
        meta = _run_meta(store, run_id)
        if meta is None:
            report["errors"] += 1
            continue
        suite = str(meta.get("suite", ""))
        if rule.suites and suite not in rule.suites:
            report["skipped"] += 1
            continue
        data = json.dumps(meta, separators=(",", ":"))
        if len(data.encode("utf-8")) > PERF_DATA_MAX_BYTES:
            report["errors"] += 1
            report["notes"].append(f"{run_id}: meta over {PERF_DATA_MAX_BYTES} bytes — skipped")
            continue
        if not dry_run:
            remote = client.perf_upload(
                {
                    "suite": suite or "unknown",
                    "tool": str(meta.get("tool", "unknown")),
                    "target": str(meta.get("binary", "") or meta.get("target", "")) or None,
                    "summary": store.load_summary(run_id)[:2000] or None,
                    "tags": [LOCAL_RUN_TAG_PREFIX + run_id],
                    "data": data,
                }
            )
            links.insert_link(db, "perf_run", run_id, str(remote["id"]), org_id, None, None)
        report["pushed"] += 1
    assert report["pushed"] <= PERF_PUSH_MAX_PER_RUN, "perf push over bound"
    return report
