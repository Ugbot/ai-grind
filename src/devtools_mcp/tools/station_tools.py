"""Station tools: link a tracker project to an llm-station-remote platform
and run the local-first sync (tasks, sessions, collab, skills, perf).

Action-multiplexed like tracker_tools. Config rules live in per-repo
.devtools-mcp/station.toml (see station.config); the lls_ API key is
env-only. tracker_sync stays peer-CRDT-only — the platform speaks plain
REST, which is this module's job.
"""

from __future__ import annotations

import functools
from pathlib import Path

import anyio
from mcp.server.fastmcp import Context

from devtools_mcp.server import get_app_ctx, mcp
from devtools_mcp.station import credentials, engine, project_link
from devtools_mcp.station.client import StationClient
from devtools_mcp.station.config import (
    CONFIG_DIRNAME,
    CONFIG_FILENAME,
    CONFIG_TEMPLATE,
    DOMAINS,
    StationConfig,
    load_station_config,
    validate_for_link,
)
from devtools_mcp.tracker.db import TrackerDB, TrackerError

NOTES_MAX_SHOWN: int = 20
CONTEXT_SECTION_MAX: int = 10


def _tracker(ctx: Context) -> TrackerDB:
    db = get_app_ctx(ctx).get_tracker()
    assert db is not None and db.conn is not None, "tracker db unavailable"
    return db


def _load_config(repo_root: str | None) -> StationConfig:
    start = Path(repo_root).resolve() if repo_root else None
    cfg = load_station_config(start)
    if cfg is None:
        raise TrackerError(
            f"No station config found — run station_link action='init' to create "
            f"{CONFIG_DIRNAME}/{CONFIG_FILENAME} in the repo"
        )
    return cfg


def _client_for(cfg: StationConfig, org_id: str) -> StationClient:
    assert org_id, "client needs an org id"
    return StationClient(cfg.station.url, cfg.api_key(), org_id)


def _org_for_session(db: TrackerDB, cfg: StationConfig) -> str:
    """Org for live coordination calls: linked project first, config second."""
    if cfg.project.local:
        row = project_link.get_project_link(db.conn, cfg.project.local)
        if row is not None:
            return str(row["org_id"])
    if cfg.station.org:
        return cfg.station.org
    raise TrackerError("No org resolved — set [station].org or run station_link action='link'")


def _format_report(report: dict) -> str:
    parts = [f"**{report.get('domain', '?')}**:"]
    for field in ("pushed", "pulled", "conflicts", "skipped", "errors", "deferred"):
        value = report.get(field, 0)
        if value:
            parts.append(f"{field}={value}")
    if len(parts) == 1:
        parts.append("nothing to do")
    line = " ".join(parts)
    notes = report.get("notes", [])[:NOTES_MAX_SHOWN]
    for note in notes:
        line += f"\n  - {note}"
    return line


def _auth_status_line() -> str:
    stored = credentials.load_credentials()
    if stored is None:
        return "auth: ⛔ not authenticated — see station_link action='auth'"
    return (
        f"auth: ✅ {stored.get('member') or 'signed in'} @ {stored.get('url', '?')} "
        f"(stored {str(stored.get('saved_at', ''))[:19]})"
    )


def _link_status(db: TrackerDB, cfg: StationConfig | None) -> str:
    lines = ["**Station status**", "", _auth_status_line(), ""]
    if cfg is not None:
        lines.append(f"config: `{cfg.source_path}` (hash `{cfg.config_hash()[:12]}`)")
        lines.append(f"url: {cfg.station.url or '(unset)'} | org: {cfg.station.org or '(from key)'}")
        enabled = ", ".join(cfg.enabled_domains()) or "none"
        lines.append(f"enabled domains: {enabled}")
    else:
        lines.append("config: none found")
    rows = project_link.list_project_links(db.conn)
    if not rows:
        lines.append("linked projects: none — run station_link action='link'")
        return "\n".join(lines)
    for row in rows:  # bounded at 100
        stale = ""
        if cfg is not None and row["project_key"] == cfg.project.local.strip().upper():
            stale = " ⚠ config changed, re-link" if row["config_hash"] != cfg.config_hash() else " ✓"
        lines.append(
            f"- **{row['project_key']}** -> {row['remote_project_key']} "
            f"(org {row['org_id'][:8]}…, repo {row['repo_id'] or '-'}) linked {row['linked_at'][:19]}{stale}"
        )
    states = db.conn.execute("SELECT * FROM station_sync_state ORDER BY rule_id LIMIT 50").fetchall()
    if states:
        lines.append("")
        lines.append("**Sync rules:**")
        for state in states:  # bounded at 50
            flag = " [PAUSED]" if state["paused"] else ""
            err = f" last_error: {state['last_error'][:120]}" if state["last_error"] else ""
            lines.append(f"- `{state['rule_id']}`{flag} last run {state['last_run_at'] or 'never'}{err}")
    return "\n".join(lines)


@mcp.tool()
async def station_link(
    ctx: Context,
    action: str,
    repo_root: str | None = None,
    domain: str | None = None,
) -> str:
    """Manage the link between local tracker projects and the llm-station platform.

    Actions:
        auth    — how to authenticate + current auth state. If any station
                  tool fails with "Not authenticated", run this and RELAY THE
                  INSTRUCTIONS TO THE USER: they open the local dashboard's
                  /station/auth page in a browser and sign in against the
                  platform (GitHub/Google); the key is stored locally.
        init    — write a commented .devtools-mcp/station.toml template into the
                  repo (repo_root or cwd). Edit it, then run action='link'.
        link    — validate the config against the live platform (auth, org,
                  project, repo) and persist the link. Sync refuses to run
                  without this.
        status  — auth state, config source, linked projects, per-rule sync
                  state/errors.
        resume  — un-pause a rule that auto-paused after repeated failures
                  (domain required, e.g. domain='tasks').
        unlink  — remove the link row (config file is left alone).
        logout  — delete the locally stored credential.

    Key resolution order: env LLM_STATION_API_KEY, then the browser-auth
    credential store (~/.devtools-mcp/station-auth.json).
    """
    db = _tracker(ctx)
    try:
        if action == "auth":
            cfg_maybe = load_station_config(Path(repo_root).resolve() if repo_root else None)
            platform_url = cfg_maybe.station.url if cfg_maybe is not None else ""
            stored = credentials.load_credentials()
            if stored is not None:
                return (
                    f"{_auth_status_line()}\n"
                    "To re-authenticate (e.g. different platform or org): open "
                    f"{credentials.DEFAULT_DASHBOARD}{credentials.DASHBOARD_AUTH_PATH} in a browser, "
                    "or station_link action='logout' first."
                )
            return credentials.auth_instructions(platform_url)
        if action == "logout":
            removed = credentials.clear_credentials()
            return "Credential deleted." if removed else "No stored credential to delete."
        if action == "init":
            root = Path(repo_root).resolve() if repo_root else Path.cwd()
            dest = root / CONFIG_DIRNAME / CONFIG_FILENAME
            if dest.is_file():
                return f"{dest} already exists — edit it, then run station_link action='link'"
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(CONFIG_TEMPLATE, encoding="utf-8")
            return f"Wrote {dest}. Fill in [station]/[project], enable domains, then station_link action='link'."
        if action == "link":
            cfg = _load_config(repo_root)
            validate_for_link(cfg)
            # org may be empty in config: link_project adopts the key's org
            # from /auth/me before issuing any org-scoped call.
            with _client_for(cfg, cfg.station.org or "unresolved") as client:
                summary = await anyio.to_thread.run_sync(project_link.link_project, db, cfg, client)
            return (
                f"Linked **{summary['project_key']}** -> {summary['remote_project']} "
                f"as {summary['member']} in org {summary['org_id']} (repo {summary['repo_id']}).\n"
                "Run station_sync to synchronize."
            )
        if action == "status":
            maybe_cfg = load_station_config(Path(repo_root).resolve() if repo_root else None)
            return _link_status(db, maybe_cfg)
        if action == "resume":
            if not domain:
                return "resume needs domain (e.g. domain='tasks')"
            cfg = _load_config(repo_root)
            rule_id = f"{cfg.project.local.strip().upper()}:{domain}"
            engine.resume_rule(db, rule_id)
            return f"Rule `{rule_id}` resumed — next station_sync will retry."
        if action == "unlink":
            cfg = _load_config(repo_root)
            removed = project_link.unlink_project(db, cfg.project.local)
            return f"Unlinked {cfg.project.local}" if removed else f"{cfg.project.local} was not linked"
        return f"Unknown action {action!r}. One of: auth, init, link, status, resume, unlink, logout."
    except TrackerError as exc:
        return f"station_link failed: {exc}"


@mcp.tool()
async def station_sync(
    ctx: Context,
    domain: str = "all",
    dry_run: bool = False,
    repo_root: str | None = None,
) -> str:
    """Run the local-first sync with the llm-station platform.

    domain: 'all' (every domain enabled in station.toml) or one of
    tasks/sessions/collab/skills/perf. dry_run=True prints the plan without
    writing anywhere — use it after changing rules.

    Local SQLite stays the source of authority: tasks push/pull with
    local-wins conflicts, sessions and claims push (claims become advisory
    checkouts), skills and perf runs upload. Offline is a normal state —
    the run fails fast and the next run re-diffs; nothing queues.

    If this fails with "Not authenticated": relay the message to the user —
    they open the dashboard's /station/auth page in a browser and sign in
    (station_link action='auth' reprints the instructions).
    """
    db = _tracker(ctx)
    try:
        cfg = _load_config(repo_root)
        domains = None if domain == "all" else (domain,)
        if domains is not None and domain not in DOMAINS:
            return f"Unknown domain {domain!r}. One of: {', '.join(DOMAINS)} (or 'all')."
        run = functools.partial(engine.run_sync, db, cfg, domains, dry_run)
        reports = await anyio.to_thread.run_sync(run)
    except TrackerError as exc:
        return f"station_sync failed: {exc}"
    header = "**Station sync (dry run — nothing written)**" if dry_run else "**Station sync**"
    body = "\n".join(_format_report(report) for report in reports)
    return f"{header}\n{body}" if body else f"{header}\nNo enabled domains — check station.toml."


def _format_context(context: dict) -> str:
    """Bounded render of the platform's devtools_context onboarding packet."""
    lines = ["**Platform context**"]
    for section, value in list(context.items())[:CONTEXT_SECTION_MAX]:  # bounded sections
        if isinstance(value, list):
            lines.append(f"\n**{section}** ({len(value)}):")
            for item in value[:5]:
                snippet = str(item)
                lines.append(f"- {snippet[:200]}")
            if len(value) > 5:
                lines.append(f"- ... {len(value) - 5} more")
        else:
            lines.append(f"- {section}: {str(value)[:200]}")
    return "\n".join(lines)


@mcp.tool()
async def station_session(
    ctx: Context,
    action: str,
    session_id: str | None = None,
    task_key: str | None = None,
    summary: str | None = None,
    context_text: str | None = None,
    next_steps: str | None = None,
    handoff_id: str | None = None,
    repo_root: str | None = None,
) -> str:
    """Live coordination with the llm-station platform (online-only verbs).

    Actions:
        start    — start a platform work session (optional task_key)
        update   — session_id + summary and/or context_text
        handoff  — context_text + next_steps: offer work to other members
        inbox    — list pending handoffs addressed to this member
        accept   — handoff_id: accept a pending handoff
        decline  — handoff_id: decline a pending handoff
        context  — the platform's devtools_context onboarding packet

    If this fails with "Not authenticated": relay the message to the user —
    they sign in via the dashboard's /station/auth page (see station_link
    action='auth').
    """
    db = _tracker(ctx)
    try:
        cfg = _load_config(repo_root)
        org_id = _org_for_session(db, cfg)
        client = _client_for(cfg, org_id)
    except TrackerError as exc:
        return f"station_session failed: {exc}"
    try:
        with client:
            if action == "start":
                body: dict = {}
                if task_key:
                    body["task_key"] = task_key
                session = await anyio.to_thread.run_sync(client.session_start, body)
                return f"Session started: `{session['id']}` (status {session.get('status', '?')})"
            if action == "update":
                if not session_id or not (summary or context_text):
                    return "update needs session_id and summary or context_text"
                body = {}
                if summary:
                    body["summary"] = summary
                if context_text:
                    body["context"] = {"note": context_text}
                await anyio.to_thread.run_sync(client.session_update, session_id, body)
                return f"Session `{session_id}` updated."
            if action == "handoff":
                if not context_text or not next_steps:
                    return "handoff needs context_text and next_steps"
                body = {"context": context_text, "next_steps": next_steps}
                if task_key:
                    body["task_key"] = task_key
                handoff = await anyio.to_thread.run_sync(client.handoff_create, body)
                return f"Handoff created: `{handoff['id']}` (status {handoff.get('status', 'pending')})"
            if action == "inbox":
                pending = await anyio.to_thread.run_sync(client.handoffs_pending)
                if not pending:
                    return "No pending handoffs."
                lines = [f"**Pending handoffs ({len(pending)}):**"]
                for handoff in pending[:20]:  # bounded display
                    lines.append(
                        f"- `{handoff['id']}` from {handoff.get('from_member_id', '?')[:8]}… "
                        f"task={handoff.get('task_key') or '-'}: {(handoff.get('context') or '')[:120]}"
                    )
                return "\n".join(lines)
            if action == "accept":
                if not handoff_id:
                    return "accept needs handoff_id"
                handoff = await anyio.to_thread.run_sync(client.handoff_accept, handoff_id)
                return f"Handoff `{handoff_id}` accepted: {(handoff.get('next_steps') or '')[:300]}"
            if action == "decline":
                if not handoff_id:
                    return "decline needs handoff_id"
                await anyio.to_thread.run_sync(client.handoff_decline, handoff_id)
                return f"Handoff `{handoff_id}` declined."
            if action == "context":
                packet = await anyio.to_thread.run_sync(client.devtools_context)
                return _format_context(packet)
            return f"Unknown action {action!r}. One of: start, update, handoff, inbox, accept, decline, context."
    except TrackerError as exc:
        return f"station_session failed: {exc}"
