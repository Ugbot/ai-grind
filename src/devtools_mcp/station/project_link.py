"""Project link lifecycle: validate a config against the live platform and
cache the resolved org/project/repo ids in station_projects.

Sync never runs against an unvalidated config: run_sync requires a link row
whose config_hash matches the loaded config.
"""

from __future__ import annotations

import sqlite3
import subprocess
from pathlib import Path

from devtools_mcp.station.client import StationClient
from devtools_mcp.station.config import StationConfig
from devtools_mcp.tracker.db import TrackerDB, TrackerError, utc_now_iso

GIT_TIMEOUT_SECONDS: int = 10


def get_project_link(conn: sqlite3.Connection, project_key: str) -> sqlite3.Row | None:
    assert project_key, "project_key must be non-empty"
    return conn.execute(
        "SELECT * FROM station_projects WHERE project_key = ?", (project_key.strip().upper(),)
    ).fetchone()


def list_project_links(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    rows = conn.execute("SELECT * FROM station_projects ORDER BY project_key LIMIT 100").fetchall()
    assert len(rows) <= 100, "project links over bound"
    return rows


def unlink_project(db: TrackerDB, project_key: str) -> bool:
    assert project_key, "project_key must be non-empty"
    with db.transaction() as conn:
        cursor = conn.execute("DELETE FROM station_projects WHERE project_key = ?", (project_key.strip().upper(),))
    return cursor.rowcount == 1


def _git_origin_url(repo_root: Path) -> str:
    """The repo's origin URL, or '' when there is no git remote."""
    assert repo_root.is_dir(), f"repo root missing: {repo_root}"
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def _resolve_remote_project(client: StationClient, cfg: StationConfig) -> dict:
    """Find the platform project by id/key, or create it from the local key."""
    wanted = (cfg.project.remote or cfg.project.local).strip()
    assert wanted, "resolve needs a project identifier"
    projects = client.projects_list()
    assert len(projects) <= 10_000, "projects list over bound"
    for project in projects:  # bounded by assert above
        if project.get("id") == wanted or project.get("key", "").upper() == wanted.upper():
            return project
    if cfg.project.remote:
        raise TrackerError(
            f"Platform project {cfg.project.remote!r} not found in org {client.org_id} "
            "(clear [project].remote to create one from the local key)"
        )
    return client.project_create(cfg.project.local.upper(), cfg.project.local.upper())


def _resolve_repo(client: StationClient, cfg: StationConfig) -> str | None:
    """Resolve the platform repo id for the collab domain (None when unused)."""
    if not cfg.rule("collab").enabled:
        return None
    if cfg.project.repo and cfg.project.repo != "auto":
        return cfg.project.repo
    source = Path(cfg.source_path)
    repo_root = source.parent.parent  # <root>/.devtools-mcp/station.toml
    origin = _git_origin_url(repo_root)
    if not origin:
        raise TrackerError(
            f"[domains.collab] is enabled but repo='auto' found no git origin under {repo_root} "
            "— set [project].repo to a platform repo id"
        )
    existing = client.repo_by_url(origin)
    if existing is not None:
        return str(existing["id"])
    registered = client.repo_register(origin, repo_root.name)
    return str(registered["id"])


def link_project(db: TrackerDB, cfg: StationConfig, client: StationClient) -> dict:
    """Validate config online and persist the resolved link. Returns a summary."""
    assert cfg.project.local, "config must name a local project"
    me = client.auth_me()
    org_id = str(me.get("org_id") or "")
    if not org_id:
        raise TrackerError("API key resolved no org (auth/me returned empty org_id)")
    if cfg.station.org and cfg.station.org != org_id:
        raise TrackerError(
            f"Config org {cfg.station.org!r} does not match the API key's org {org_id!r} "
            "— keys are org-scoped, fix [station].org or use the right key"
        )
    client.org_id = org_id  # adopt the key's org before any org-scoped call
    remote_project = _resolve_remote_project(client, cfg)
    repo_id = _resolve_repo(client, cfg)
    project_key = cfg.project.local.strip().upper()
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO station_projects (project_key, base_url, org_id, remote_project_id, "
            "remote_project_key, repo_id, member_id, config_hash, linked_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(project_key) DO UPDATE SET base_url = excluded.base_url, "
            "org_id = excluded.org_id, remote_project_id = excluded.remote_project_id, "
            "remote_project_key = excluded.remote_project_key, repo_id = excluded.repo_id, "
            "member_id = excluded.member_id, config_hash = excluded.config_hash, "
            "linked_at = excluded.linked_at",
            (
                project_key,
                cfg.station.url,
                org_id,
                str(remote_project["id"]),
                str(remote_project.get("key", "")),
                repo_id,
                str(me.get("member_id", "")),
                cfg.config_hash(),
                utc_now_iso(),
            ),
        )
    summary = {
        "project_key": project_key,
        "org_id": org_id,
        "member": f"{me.get('name', '?')} ({me.get('type', '?')})",
        "remote_project": f"{remote_project.get('key', '?')} ({remote_project['id']})",
        "repo_id": repo_id or "-",
    }
    assert summary["org_id"], "link summary missing org"
    return summary
