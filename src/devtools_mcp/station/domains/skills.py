"""Skills domain: push the local skills library to the platform catalog.

The local MANIFEST.json (harvest provenance) is the authority; its sha256
per item is a ready-made content hash, so the diff is manifest-hash vs
link-hash. Upserts are keyed by name server-side (POST is idempotent) and
require an org-admin key. Bulk seeding stays the platform's own
seed_skills script; this keeps the catalog fresh incrementally.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from devtools_mcp.station import links
from devtools_mcp.station.client import StationClient
from devtools_mcp.station.config import StationConfig
from devtools_mcp.tracker.db import TrackerDB, TrackerError

ENV_SKILLS_ROOT: str = "DEVTOOLS_MCP_SKILLS_ROOT"
MANIFEST_ITEMS_MAX: int = 500
SKILL_BODY_MAX_BYTES: int = 262_144  # 256 KiB
SKILLS_PUSH_MAX_PER_RUN: int = 100


def _skills_root() -> Path:
    """The skills library root: env override, else this repo's skills/ tree."""
    override = os.environ.get(ENV_SKILLS_ROOT, "").strip()
    default = Path(__file__).resolve().parents[4] / "skills"  # <repo>/skills
    root = Path(override) if override else default
    assert root.name, "skills root has no name"
    return root


def _load_manifest(root: Path) -> list[dict]:
    manifest_path = root / "MANIFEST.json"
    if not manifest_path.is_file():
        raise TrackerError(f"Skills manifest not found at {manifest_path} (set {ENV_SKILLS_ROOT})")
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    items = data.get("items", data) if isinstance(data, dict) else data
    if not isinstance(items, list):
        raise TrackerError(f"Unexpected manifest shape in {manifest_path}")
    assert len(items) <= MANIFEST_ITEMS_MAX, f"manifest over bound: {len(items)} items"
    return items


def _skill_body(root: Path, item: dict) -> str:
    """The skill's content, bounded; '' when the dest file is missing."""
    dest = str(item.get("dest", "")).strip()
    if not dest:
        return ""
    path = root / dest
    if path.is_dir():
        path = path / "SKILL.md"
    if not path.is_file():
        return ""
    body = path.read_text(encoding="utf-8", errors="replace")
    assert isinstance(body, str), "skill body must be text"
    return body[:SKILL_BODY_MAX_BYTES]


def sync(
    db: TrackerDB,
    client: StationClient,
    cfg: StationConfig,
    project_row: sqlite3.Row,
    state_row: sqlite3.Row,
    dry_run: bool,
) -> dict:
    """Push manifest items whose sha256 differs from the last-synced hash."""
    assert project_row["org_id"], "unlinked project reached skills sync"
    report: dict[str, Any] = {
        "domain": "skills",
        "pushed": 0,
        "pulled": 0,
        "conflicts": 0,
        "skipped": 0,
        "errors": 0,
        "notes": [],
    }
    root = _skills_root()
    items = _load_manifest(root)
    org_id = project_row["org_id"]
    pushed = 0
    for item in items:  # bounded by MANIFEST_ITEMS_MAX
        name = str(item.get("name", "")).strip()
        sha = str(item.get("sha256", "")).strip()
        if not name or not sha:
            report["errors"] += 1
            continue
        link = links.get_link(db.conn, "skill", name)
        if link is not None and link["synced_hash"] == sha:
            report["skipped"] += 1
            continue
        if pushed >= SKILLS_PUSH_MAX_PER_RUN:
            report["notes"].append(f"push cap {SKILLS_PUSH_MAX_PER_RUN} hit — remainder next run")
            break
        if not dry_run:
            remote = client.skill_upsert(
                {
                    "name": name,
                    "type": str(item.get("type", "skill")),
                    "category": str(item.get("category", "")),
                    "body": _skill_body(root, item),
                }
            )
            links.insert_link(db, "skill", name, str(remote["id"]), org_id, None, sha)
        pushed += 1
        report["pushed"] += 1
    assert report["pushed"] <= SKILLS_PUSH_MAX_PER_RUN, "skills push over bound"
    return report
