"""Tracker tools: persistent SQLite-backed mini-JIRA (projects, tasks, criteria,
tags, commits, external issues, bounded queries).

Action-multiplexed like debug(): one tool per noun, an `action` parameter per
verb. Every response is bounded markdown — full tables live in the DB and are
paged via tracker_query.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import anyio
from mcp.server.fastmcp import Context

from devtools_mcp.formatters import format_dataframe
from devtools_mcp.server import get_app_ctx, mcp
from devtools_mcp.tracker import activity as activity_mod
from devtools_mcp.tracker import commits as commits_mod
from devtools_mcp.tracker import criteria as criteria_mod
from devtools_mcp.tracker import deps as deps_mod
from devtools_mcp.tracker import frames
from devtools_mcp.tracker import tags as tags_mod
from devtools_mcp.tracker import tasks as tasks_mod
from devtools_mcp.tracker.db import TrackerDB, TrackerError

QUERY_MAX_LIMIT: int = 200


def _tracker(ctx: Context) -> TrackerDB:
    """The lazily-opened tracker database from the app context."""
    db = get_app_ctx(ctx).get_tracker()
    assert db is not None and db.conn is not None, "tracker db unavailable"
    return db


async def _offload_tracker(ctx: Context, fn: Callable[[TrackerDB], Any]) -> Any:
    """Run blocking tracker work (network + SQLite) off the event loop.

    SQLite connections are thread-affine, so the event loop's TrackerDB cannot be
    handed to a worker thread; open a fresh connection to the same DB inside the
    thread instead. WAL makes the concurrent open safe while the main connection
    sits idle during the await.
    """
    path = _tracker(ctx).path

    def work() -> Any:
        db = TrackerDB(path)
        try:
            return fn(db)
        finally:
            db.close()

    return await anyio.to_thread.run_sync(work)


def _task_line(task) -> str:
    return f"`{task.key}` [{task.status}] ({task.kind}, p{task.priority}) {task.title}"


def _task_detail(db: TrackerDB, key: str) -> str:
    """Rich single-task view: fields + children + criteria + tags + commits + refs."""
    task = tasks_mod.get_task(db.conn, key)
    parts = [f"**{task.key}** — {task.title}", ""]
    parent = ""
    if task.parent_id is not None:
        row = db.conn.execute("SELECT key FROM tasks WHERE id = ?", (task.parent_id,)).fetchone()
        parent = f" | parent: `{row[0]}`" if row else ""
    parts.append(
        f"status: **{task.status}** | kind: {task.kind} | priority: {task.priority}" f" | depth: {task.depth}{parent}"
    )
    if task.description:
        parts += ["", task.description]
    tag_names = tags_mod.tags_for_task(db.conn, task.id)
    if tag_names:
        parts.append("tags: " + ", ".join(f"`{name}`" for name in tag_names))
    children = tasks_mod.children_of(db.conn, task.id)
    if children:
        parts += ["", f"**Children ({len(children)}):**"]
        parts += [f"- {_task_line(child)}" for child in children[:30]]
        if len(children) > 30:
            parts.append(f"- ... {len(children) - 30} more (tracker_query view='tasks')")
    criteria = criteria_mod.list_criteria(db.conn, task.id)
    if criteria:
        parts += ["", f"**Acceptance criteria ({len(criteria)}):**"]
        for criterion in criteria[:30]:
            mark = {"pass": "[x]", "fail": "[!]"}.get(criterion.last_result or "", "[ ]")
            ref = f" — `{criterion.test_ref}`" if criterion.test_ref else " — *no test linked*"
            parts.append(f"- {mark} #{criterion.id} {criterion.text}{ref}")
    commit_rows = db.conn.execute(
        "SELECT commit_hash, message_snippet FROM task_commits WHERE task_id = ? " "ORDER BY id DESC LIMIT 10",
        (task.id,),
    ).fetchall()
    if commit_rows:
        parts += ["", f"**Commits ({len(commit_rows)} most recent):**"]
        parts += [f"- `{row[0][:12]}` {row[1]}" for row in commit_rows]
    ref_rows = db.conn.execute(
        "SELECT provider, ref_id, url, state FROM external_refs WHERE task_id = ?",
        (task.id,),
    ).fetchall()
    for row in ref_rows:
        parts.append(f"- external: {row[0]} #{row[1]} ({row[3] or 'unsynced'}) {row[2]}")
    parts += ["", f"created {task.created_at} | updated {task.updated_at}"]
    return "\n".join(parts)


@mcp.tool()
async def tracker_project(
    ctx: Context,
    action: str,
    key: str | None = None,
    name: str | None = None,
    description: str = "",
    close_policy: str | None = None,
) -> str:
    """Manage tracker projects (the namespaces for PROJ-123 task keys).

    Actions:
        create     — key (2-10 chars, [A-Z][A-Z0-9]*) + name; optional close_policy
        list       — all projects
        get        — one project by key
        set_policy — change close_policy ('advisory' warns on unmet acceptance
                     criteria at close; 'strict' rejects unless override)
    """
    db = _tracker(ctx)
    try:
        if action == "create":
            if not key or not name:
                return "create needs key and name"
            project = tasks_mod.create_project(db, key, name, description, close_policy or "advisory")
            return f"Created project **{project.key}** ({project.name}), policy={project.close_policy}"
        if action == "list":
            projects = tasks_mod.list_projects(db.conn)
            if not projects:
                return "No projects yet. Create one with action='create'."
            lines = [
                f"- **{p.key}** {p.name} (policy={p.close_policy}, next={p.key}-{p.next_seq})" for p in projects[:100]
            ]
            return f"**Projects ({len(projects)}):**\n" + "\n".join(lines)
        if action == "get":
            if not key:
                return "get needs key"
            project = tasks_mod.get_project(db.conn, key)
            rollup = format_dataframe(frames.rollup_frame(db.conn, project.key), max_rows=20)
            return (
                f"**{project.key}** {project.name} — {project.description or '(no description)'}\n"
                f"policy={project.close_policy} | created {project.created_at}\n\n{rollup}"
            )
        if action == "set_policy":
            if not key or not close_policy:
                return "set_policy needs key and close_policy"
            project = tasks_mod.set_policy(db, key, close_policy)
            return f"Project **{project.key}** close_policy={project.close_policy}"
        return f"Unknown action {action!r}. One of: create, list, get, set_policy"
    except TrackerError as exc:
        return f"Error: {exc}"


@mcp.tool()
async def tracker_task(
    ctx: Context,
    action: str,
    key: str | None = None,
    project: str | None = None,
    title: str | None = None,
    description: str | None = None,
    kind: str = "task",
    parent: str | None = None,
    priority: int | None = None,
    to_root: bool = False,
    before: str | None = None,
    subtasks: list[str] | None = None,
) -> str:
    """Create, inspect, update, move, or break down tracker tasks.

    Actions:
        create    — project + title; optional kind (epic/story/task/subtask/spike/test),
                    parent (task key), description, priority 1-5. Auto-applies tag rules.
                    Always supply a short description — it appears on dashboard cards at
                    http://127.0.0.1:8765/tracker (what/why, acceptance hints, links).
        get       — key: rich detail (children, criteria, tags, commits, external refs)
        update    — key + any of title/description/kind/priority (kind='task' is
                    the parameter default and therefore ignored on update). Use
                    description for context the dashboard cards should show.
        move      — key + parent (reparent) or to_root=True, and/or before (sibling
                    key to reorder in front of). Hierarchy is bounded at 6 levels.
        breakdown — key + subtasks (list of titles, max 20): punch-card decomposition;
                    child kind defaults by parent kind (epic→story, story→task, task→subtask)
    """
    db = _tracker(ctx)
    try:
        if action == "create":
            if not project or not title:
                return "create needs project and title"
            task, applied = tasks_mod.create_task(db, project, title, description or "", kind, parent, priority or 3)
            tag_note = f" | auto-tags: {', '.join(applied)}" if applied else ""
            return f"Created {_task_line(task)}{tag_note}"
        if action == "get":
            if not key:
                return "get needs key"
            return _task_detail(db, key)
        if action == "update":
            if not key:
                return "update needs key"
            task = tasks_mod.update_task(
                db,
                key,
                title,
                description,
                kind if kind != "task" else None,
                priority,
            )
            return f"Updated {_task_line(task)}"
        if action == "move":
            if not key:
                return "move needs key"
            task = tasks_mod.move_task(db, key, parent, to_root, before)
            return f"Moved {_task_line(task)} (depth {task.depth})"
        if action == "breakdown":
            if not key or not subtasks:
                return "breakdown needs key and subtasks (list of titles)"
            created = tasks_mod.breakdown(db, key, subtasks)
            lines = [f"- {_task_line(task)}" for task, _ in created]
            return f"Broke down `{key}` into {len(created)} subtasks:\n" + "\n".join(lines)
        return f"Unknown action {action!r}. One of: create, get, update, move, breakdown"
    except TrackerError as exc:
        return f"Error: {exc}"


@mcp.tool()
async def tracker_status(
    ctx: Context,
    key: str,
    status: str,
    override: bool = False,
) -> str:
    """Transition a task's status (open/in_progress/blocked/done/cancelled).

    Closing to 'done' evaluates the acceptance-criteria gate: under a 'strict'
    project policy the close is rejected while criteria are unmet or unlinked
    (pass override=True to force); under 'advisory' it closes with warnings.
    Closing also reports which dependent tasks just became unblocked.
    """
    db = _tracker(ctx)
    try:
        task, warnings = tasks_mod.set_status(db, key, status, override)
        unblocked: list[str] = []
        if task.status in ("done", "cancelled"):
            unblocked = deps_mod.unblocked_by_closing(db.conn, task.id)
    except TrackerError as exc:
        return f"Error: {exc}"
    result = f"{_task_line(task)}"
    if warnings:
        result += "\n\n**Warnings:**\n- " + "\n- ".join(warnings)
        if override:
            result += "\n(closed with override)"
    if unblocked:
        result += "\n\n**Now unblocked:** " + ", ".join(f"`{k}`" for k in unblocked)
    return result


@mcp.tool()
async def tracker_criteria(
    ctx: Context,
    action: str,
    key: str | None = None,
    criterion_id: int | None = None,
    text: str | None = None,
    test_ref: str | None = None,
    result: str | None = None,
) -> str:
    """Manage acceptance criteria on a task — outcomes that tests must verify.

    Actions:
        add    — key + text; optional test_ref ('tests/test_x.py::test_name')
        update — criterion_id + text and/or test_ref
        record — criterion_id + result ('pass'|'fail'); stamps last_run_at
        remove — criterion_id
        list   — key: all criteria on the task
    """
    db = _tracker(ctx)
    try:
        if action == "add":
            if not key or not text:
                return "add needs key and text"
            task = tasks_mod.get_task(db.conn, key)
            criterion = criteria_mod.add_criterion(db, task.id, text, test_ref)
            return f"Added criterion #{criterion.id} to `{task.key}`: {criterion.text}"
        if action == "update":
            if not criterion_id:
                return "update needs criterion_id"
            criterion = criteria_mod.update_criterion(db, criterion_id, text, test_ref)
            return f"Criterion #{criterion.id}: {criterion.text} (test: {criterion.test_ref or 'none'})"
        if action == "record":
            if not criterion_id or not result:
                return "record needs criterion_id and result ('pass'|'fail')"
            criterion = criteria_mod.record_result(db, criterion_id, result)
            return f"Criterion #{criterion.id} recorded **{criterion.last_result}** at {criterion.last_run_at}"
        if action == "remove":
            if not criterion_id:
                return "remove needs criterion_id"
            removed = criteria_mod.remove_criterion(db, criterion_id)
            return f"Criterion #{criterion_id} {'removed' if removed else 'not found'}"
        if action == "list":
            if not key:
                return "list needs key"
            task = tasks_mod.get_task(db.conn, key)
            items = criteria_mod.list_criteria(db.conn, task.id)
            if not items:
                return f"`{task.key}` has no acceptance criteria"
            lines = []
            for criterion in items[:50]:
                mark = {"pass": "[x]", "fail": "[!]"}.get(criterion.last_result or "", "[ ]")
                ref = f" — `{criterion.test_ref}`" if criterion.test_ref else ""
                lines.append(f"- {mark} #{criterion.id} {criterion.text}{ref}")
            return f"**Criteria on `{task.key}` ({len(items)}):**\n" + "\n".join(lines)
        return f"Unknown action {action!r}. One of: add, update, record, remove, list"
    except TrackerError as exc:
        return f"Error: {exc}"


@mcp.tool()
async def tracker_tag(
    ctx: Context,
    action: str,
    key: str | None = None,
    tag: str | None = None,
    project: str | None = None,
    match_kind: str | None = None,
    match_regex: str | None = None,
    match_parent_kind: str | None = None,
    rule_id: int | None = None,
) -> str:
    """Tag tasks and manage auto-tagging rules applied at task creation.

    Actions:
        add         — key + tag: attach a tag to a task
        remove      — key + tag: detach
        rule_add    — tag + at least one of match_kind / match_regex (over
                      title+description) / match_parent_kind; optional project
                      to scope the rule (else global)
        rule_list   — rules (optionally scoped to project)
        rule_remove — rule_id
    """
    db = _tracker(ctx)
    try:
        if action == "add":
            if not key or not tag:
                return "add needs key and tag"
            task = tasks_mod.get_task(db.conn, key)
            name = tags_mod.add_tag(db, task.id, tag)
            return f"Tagged `{task.key}` with `{name}`"
        if action == "remove":
            if not key or not tag:
                return "remove needs key and tag"
            task = tasks_mod.get_task(db.conn, key)
            removed = tags_mod.remove_tag(db, task.id, tag)
            return f"Tag `{tag}` {'removed from' if removed else 'was not on'} `{task.key}`"
        if action == "rule_add":
            if not tag:
                return "rule_add needs tag"
            project_id = tasks_mod.get_project(db.conn, project).id if project else None
            rule_id_new = tags_mod.add_rule(db, tag, project_id, match_kind, match_regex, match_parent_kind)
            scope = project.upper() if project else "global"
            return (
                f"Rule #{rule_id_new} ({scope}): tag `{tag}` on kind={match_kind} "
                f"regex={match_regex} parent_kind={match_parent_kind}"
            )
        if action == "rule_list":
            project_id = tasks_mod.get_project(db.conn, project).id if project else None
            rules = tags_mod.list_rules(db.conn, project_id)
            if not rules:
                return "No tag rules defined"
            lines = [
                f"- #{rule['id']} tag=`{rule['tag_name']}` kind={rule['match_kind']} "
                f"regex={rule['match_regex']} parent_kind={rule['match_parent_kind']} "
                f"{'(global)' if rule['project_id'] is None else ''}"
                for rule in rules[:100]
            ]
            return f"**Tag rules ({len(rules)}):**\n" + "\n".join(lines)
        if action == "rule_remove":
            if not rule_id:
                return "rule_remove needs rule_id"
            removed = tags_mod.remove_rule(db, rule_id)
            return f"Rule #{rule_id} {'removed' if removed else 'not found'}"
        return f"Unknown action {action!r}. One of: add, remove, rule_add, rule_list, rule_remove"
    except TrackerError as exc:
        return f"Error: {exc}"


@mcp.tool()
async def tracker_commits(
    ctx: Context,
    action: str,
    key: str | None = None,
    repo: str | None = None,
    commit: str | None = None,
    message: str = "",
    max_commits: int = 500,
) -> str:
    """Link git commits to tasks.

    Actions:
        link — key + repo (path) + commit (hash): manual link
        scan — repo: scan `git log` for task keys (PROJ-123) in commit messages
               and auto-link them to existing tasks; idempotent on re-scan
    """
    db = _tracker(ctx)
    try:
        if action == "link":
            if not key or not repo or not commit:
                return "link needs key, repo, and commit"
            created = commits_mod.link_commit(db, key, repo, commit, message)
            return (
                f"Linked `{commit[:12]}` to `{key.upper()}`"
                if created
                else f"`{commit[:12]}` was already linked to `{key.upper()}`"
            )
        if action == "scan":
            if not repo:
                return "scan needs repo (path to a git repository)"
            if not (1 <= max_commits <= commits_mod.SCAN_MAX_COMMITS):
                return f"max_commits must be 1..{commits_mod.SCAN_MAX_COMMITS}, got {max_commits}"
            # `git log` is a blocking subprocess (up to GIT_TIMEOUT_SECONDS); run
            # it off the event loop so the server stays responsive during a scan.
            # DB linking stays on the loop thread — the sqlite connection has
            # thread affinity and link_entries is a single fast transaction.
            entries = await anyio.to_thread.run_sync(commits_mod._git_log, repo, max_commits)
            counters = commits_mod.link_entries(db, repo, entries)
            return (
                f"Scanned {counters['scanned']} commits in `{repo}`: "
                f"{counters['matched']} key mentions, {counters['linked']} new links, "
                f"{counters['skipped_unknown_key']} unknown keys skipped"
            )
        return f"Unknown action {action!r}. One of: link, scan"
    except TrackerError as exc:
        return f"Error: {exc}"


def _format_plan(plan) -> str:
    """Bounded rendering of a resolver Plan."""
    assert plan.project_key, "plan missing project key"
    scope = f" (goal `{plan.goal_key.upper()}`)" if plan.goal_key else ""
    if plan.open_count == 0:
        return f"**Execution plan — {plan.project_key}**{scope}: nothing open. All done."
    parts = [f"**Execution plan — {plan.project_key}**{scope} — {plan.open_count} open tasks", ""]
    if plan.ready:
        parts.append(f"**Ready now ({len(plan.ready)}):**")
        parts += [f"- {_task_line(task)}" for task in plan.ready[:30]]
        if len(plan.ready) > 30:
            parts.append(f"- ... {len(plan.ready) - 30} more")
        parts.append("")
    if plan.blocked:
        parts.append(f"**Blocked ({len(plan.blocked)}):**")
        parts += [
            f"- `{task.key}` ← waiting on {', '.join(f'`{b}`' for b in blockers)}"
            for task, blockers in plan.blocked[:30]
        ]
        if len(plan.blocked) > 30:
            parts.append(f"- ... {len(plan.blocked) - 30} more")
        parts.append("")
    if plan.waiting_on_children:
        keys = ", ".join(f"`{task.key}`" for task in plan.waiting_on_children[:30])
        parts.append(f"**Waiting on subtasks ({len(plan.waiting_on_children)}):** {keys}")
        parts.append("")
    if len(plan.layers) > 1:
        parts.append("**Order (each layer is parallelizable):**")
        for index, layer in enumerate(plan.layers[:20], start=1):
            parts.append(f"{index}. " + ", ".join(f"`{k}`" for k in layer[:30]))
        if len(plan.layers) > 20:
            parts.append(f"... {len(plan.layers) - 20} more layers")
    return "\n".join(parts).rstrip()


@mcp.tool()
async def tracker_deps(
    ctx: Context,
    action: str,
    key: str | None = None,
    depends_on: str | None = None,
    project: str | None = None,
) -> str:
    """Task dependencies and the execution-plan resolver.

    A dependency means `depends_on` must be done/cancelled before `key` can
    start. Edges are cycle-checked and stay within one project.

    Actions:
        add     — key + depends_on: add an edge
        remove  — key + depends_on: remove it
        list    — key: what this task waits on, and what waits on it
        resolve — project (and/or key as the goal): what needs to happen —
                  ready-now tasks, blocked tasks with their blockers, and the
                  parallelizable execution order. With key, the plan is limited
                  to that task's subtree + transitive dependencies.
    """
    db = _tracker(ctx)
    try:
        if action == "add":
            if not key or not depends_on:
                return "add needs key and depends_on"
            created = deps_mod.add_dep(db, key, depends_on)
            note = "now depends on" if created else "already depended on"
            return f"`{key.upper()}` {note} `{depends_on.upper()}`"
        if action == "remove":
            if not key or not depends_on:
                return "remove needs key and depends_on"
            removed = deps_mod.remove_dep(db, key, depends_on)
            return f"Dependency {'removed' if removed else 'did not exist'}"
        if action == "list":
            if not key:
                return "list needs key"
            task = tasks_mod.get_task(db.conn, key)
            blockers = deps_mod.deps_of(db.conn, task.id)
            dependents = deps_mod.dependents_of(db.conn, task.id)
            parts = [f"**Dependencies of `{task.key}`:**"]
            if blockers:
                parts += [f"- waits on {_task_line(dep)}" for dep in blockers[:50]]
            else:
                parts.append("- waits on nothing")
            if dependents:
                parts += [f"- blocks {_task_line(dep)}" for dep in dependents[:50]]
            return "\n".join(parts)
        if action == "resolve":
            goal: str | None = key
            project_key = project
            if project_key is None and goal is not None:
                project_key = tasks_mod.get_task(db.conn, goal).key.rsplit("-", 1)[0]
            if project_key is None:
                return "resolve needs project (or key to derive it from)"
            plan = deps_mod.resolve_plan(db.conn, project_key, goal)
            return _format_plan(plan)
        return f"Unknown action {action!r}. One of: add, remove, list, resolve"
    except TrackerError as exc:
        return f"Error: {exc}"


@mcp.tool()
async def tracker_issue(
    ctx: Context,
    action: str,
    key: str,
    provider: str = "github",
    repo: str | None = None,
    ref_id: str | None = None,
) -> str:
    """Bridge tracker tasks to external issue trackers (GitHub; GitLab planned).

    Auth: GITHUB_TOKEN or GH_TOKEN env var (classic or fine-grained with issue
    write access). Note this is the *server's* environment — `gh auth` alone is
    not picked up.

    Every issue body carries a machine-readable marker,
    `<!-- devtools-mcp:task=PROJ-123 -->`, so the link survives a rebuilt
    tracker DB, a fresh CRDT replica, or an issue opened by hand.

    Actions:
        create — key + repo ('owner/name'): create a remote issue from the task
                 (description + acceptance-criteria checklist + back-link), label
                 it with the task's tags, and store the external ref
        adopt  — key + repo + ref_id: link an issue that already exists (opened
                 by hand or by another replica), stamping the marker into its
                 body if absent. Refuses an issue marked for a different task.
        sync   — key: pull remote state, stamp last_synced, report drift between
                 local task status and remote open/closed state
        close  — key: close the linked remote issue
    """
    from devtools_mcp.tracker import issues as issues_mod

    try:
        # GitHub calls are blocking HTTP + SQLite writes — run off the event loop.
        if action == "create":
            if not repo:
                return "create needs repo ('owner/name')"
            issue = await _offload_tracker(ctx, lambda db: issues_mod.create_issue_for_task(db, key, provider, repo))
            return f"Created {provider} issue #{issue.ref_id} for `{key.upper()}`: {issue.url}"
        if action == "adopt":
            if not repo or not ref_id:
                return "adopt needs repo ('owner/name') and ref_id (the issue number)"
            issue, stamped = await _offload_tracker(
                ctx, lambda db: issues_mod.adopt_issue(db, key, provider, repo, ref_id)
            )
            note = "marker stamped into the issue body" if stamped else "already marked"
            return f"Adopted {provider} issue #{issue.ref_id} for `{key.upper()}` ({note}): {issue.url}"
        if action == "sync":
            issue, drift = await _offload_tracker(ctx, lambda db: issues_mod.sync_issue(db, key, provider))
            result = f"`{key.upper()}` ↔ {provider} #{issue.ref_id} ({issue.state}) {issue.url}"
            if drift:
                result += "\n\n**Drift:**\n- " + "\n- ".join(drift)
            return result
        if action == "close":
            issue = await _offload_tracker(ctx, lambda db: issues_mod.close_external_issue(db, key, provider))
            return f"Closed {provider} issue #{issue.ref_id} for `{key.upper()}`"
        return f"Unknown action {action!r}. One of: create, adopt, sync, close"
    except TrackerError as exc:
        return f"Error: {exc}"
    except NotImplementedError as exc:
        return f"Error: {exc}"


@mcp.tool()
async def tracker_sync(
    ctx: Context,
    action: str,
    url: str | None = None,
) -> str:
    """Local-first collaboration: sync this tracker replica with a peer.

    Every replica keeps a CRDT op-log (hybrid logical clocks, last-writer-wins
    rows); merge is idempotent and converges regardless of sync order. A peer
    is any machine running `devtools_dashboard` — its viz server exposes the
    sync API at /api/crdt/.

    Actions:
        status — this replica's site id, op count, watermark, known peers
        sync   — url (e.g. 'http://other-box:8765'): full bidirectional
                 exchange — pull the peer's ops, merge, push ours
    """
    from devtools_mcp.tracker import crdt
    from devtools_mcp.tracker import sync as sync_mod

    db = _tracker(ctx)
    try:
        if action == "status":
            info = crdt.status(db)
            peers = "\n".join(f"- `{p['url']}` last synced {p['last_synced']}" for p in info["peers"]) or "- none yet"
            station_rows = db.conn.execute(
                "SELECT project_key, remote_project_key, linked_at FROM station_projects LIMIT 10"
            ).fetchall()
            station = (
                "\n**Station:** "
                + "; ".join(f"`{r['project_key']}` -> {r['remote_project_key']}" for r in station_rows)
                + " (rule-driven platform sync — see station_sync)"
                if station_rows
                else ""
            )
            return (
                f"**Tracker replica** site `{info['site_id'][:12]}…`\n"
                f"ops in log: {info['ops']} | watermark: `{info['latest_hlc'] or '—'}`\n"
                f"**Peers:**\n{peers}{station}"
            )
        if action == "sync":
            if not url:
                return "sync needs url (a peer's dashboard, e.g. http://host:8765)"
            from devtools_mcp.net_guard import SsrfError, check_sync_url

            try:
                check_sync_url(url)
            except SsrfError as exc:
                return f"Refused to sync: {exc}"
            counters = await _offload_tracker(ctx, lambda db: sync_mod.sync_once(db, url))
            return (
                f"Synced with `{url}` (site `{counters['peer_site'][:12]}…`): "
                f"pulled {counters['pulled_new']} new ops ({counters['pulled_applied']} applied, "
                f"{counters['pulled_deferred']} deferred), pushed {counters['pushed']} "
                f"({counters['pushed_new_on_peer']} new on peer)"
            )
        return f"Unknown action {action!r}. One of: status, sync"
    except TrackerError as exc:
        return f"Error: {exc}"


def _collab_identity(agent: str | None) -> str:
    """Resolve this caller's collab identity: explicit param, env label, or pid.

    Hooks report the real Claude Code session_id; tool calls fall back to this
    (documented limitation on a shared HTTP server — the team collab server
    will own identity properly)."""
    resolved = (agent or "").strip() or os.environ.get("DEVTOOLS_MCP_AGENT", "").strip() or f"pid-{os.getpid()}"
    assert resolved, "collab identity resolution produced empty id"
    assert len(resolved) <= activity_mod.SESSION_ID_MAX, "identity too long"
    return resolved


def _remote_checkout_lines(db: TrackerDB, rel_path: str) -> list[str]:
    """Other members' platform checkouts on this path (station collab mirror)."""
    from devtools_mcp.station.domains.claims import remote_conflicts_for

    assert rel_path, "rel_path must be non-empty"
    rows = remote_conflicts_for(db.conn, rel_path)
    return [
        f"- CHECKED OUT on the platform by member `{r['member_id'][:12]}…`"
        + (f" (task {r['task_key']})" if r.get("task_key") else "")
        + (f" until {r['expires_at']}" if r.get("expires_at") else "")
        for r in rows[:10]  # bounded
    ]


def _conflict_lines(found: list[dict]) -> list[str]:
    """Render conflict dicts (activity.conflicts_for) as markdown lines."""
    assert isinstance(found, list), "conflicts must be a list"
    lines = []
    for c in found[:20]:  # bounded
        who = c.get("agent") or c.get("session_id")
        task = f" on task `{c['task_key']}`" if c.get("task_key") else ""
        if c["kind"] == "claim":
            lines.append(f"- CLAIMED by **{who}**{task} until {c['expires_at']}: `{c['file']}`")
        else:
            lines.append(f"- touched by **{who}**{task} at {c['ts']}: `{c['file']}`")
    return lines


@mcp.tool()
async def tracker_files(
    ctx: Context,
    action: str,
    repo: str | None = None,
    files: list[str] | None = None,
    file: str | None = None,
    task_key: str | None = None,
    op: str = "edit",
    ttl_minutes: int = 15,
    agent: str | None = None,
) -> str:
    """Local agent collaboration: report file touches, take advisory claims,
    and see who else is working where. Machine-local (a multi-user team collab
    server is coming soon; this is its single-machine precursor).

    Actions:
        touch     — repo + files (≤50): record activity; returns any conflicts
        claim     — repo + file: advisory lease (ttl_minutes, renewable; touching
                    the file heartbeats it). Fails if another session holds it.
        release   — release own claims: repo + file for one, repo for all there,
                    neither for everything
        status    — sessions, active claims and recent touches (repo optional)
        conflicts — repo + file: who else claimed/recently touched it
    Set agent (or env DEVTOOLS_MCP_AGENT) to a stable label so humans can tell
    agents apart; task_key links activity to a tracker task.
    """
    db = _tracker(ctx)
    who = _collab_identity(agent)
    cwd = (repo or "").strip() or os.getcwd()
    try:
        if action == "touch":
            if not files:
                return "touch needs files (list of paths)"
            written = activity_mod.record_touches(
                db, who, cwd, files, agent_label=who, task_key=task_key, tool_name="tracker_files", op=op
            )
            found: list[dict] = []
            for path in files[:10]:  # conflict check bounded to first 10
                root, rel = activity_mod.normalize(cwd, path)
                found += activity_mod.conflicts_for(db.conn, who, root, rel)
            head = f"Recorded {written} touch(es) as **{who}**" + (f" on `{task_key}`" if task_key else "")
            if not found:
                return head
            return head + "\n\n**Heads up — others are here:**\n" + "\n".join(_conflict_lines(found))
        if action == "claim":
            if not file:
                return "claim needs file (and repo when the path is relative)"
            ttl_s = max(1, ttl_minutes) * 60
            claim = activity_mod.acquire_claim(db, who, cwd, file, agent_label=who, task_key=task_key, ttl_s=ttl_s)
            return f"Claimed `{claim.file_path}` in `{claim.repo_root}` as **{who}** " f"until {claim.expires_at}" + (
                f" (task `{claim.task_key}`)" if claim.task_key else ""
            )
        if action == "release":
            scope_root = activity_mod.normalize(cwd, file or ".")[0] if (repo or file) else None
            scope_rel = activity_mod.normalize(cwd, file)[1] if file else None
            released = activity_mod.release_claims(db, who, repo_root=scope_root, file_path=scope_rel)
            return f"Released {released} claim(s) held by **{who}**"
        if action == "status":
            scope = activity_mod.normalize(cwd, ".")[0] if repo else None
            sessions = activity_mod.sessions_overview(db.conn)
            claims = activity_mod.active_claims(db.conn, scope)
            recent = activity_mod.recent_activity(db.conn, scope, limit=20)
            parts = [f"**Sessions ({len(sessions)}):**"]
            for s in sessions[:20]:
                label = s["agent_label"] or s["session_id"]
                parts.append(
                    f"- **{label}** last seen {s['last_seen']} | {s['touches']} touches | {s['claims']} claims"
                )
            parts.append(f"\n**Active claims ({len(claims)}):**")
            parts += [
                f"- `{c.file_path}` by **{c.agent_label or c.session_id}** until {c.expires_at}" for c in claims[:20]
            ] or ["- none"]
            parts.append(f"\n**Recent touches ({len(recent)}):**")
            parts += [f"- `{t.file_path}` {t.op} by **{t.agent_label or t.session_id}** at {t.ts}" for t in recent] or [
                "- none"
            ]
            return "\n".join(parts)
        if action == "conflicts":
            if not file:
                return "conflicts needs file (and repo when the path is relative)"
            root, rel = activity_mod.normalize(cwd, file)
            found = activity_mod.conflicts_for(db.conn, who, root, rel)
            remote_lines = _remote_checkout_lines(db, rel)
            if not found and not remote_lines:
                return f"No one else is on `{rel}` — clear to edit."
            body = _conflict_lines(found) + remote_lines
            return f"**Conflicts on `{rel}`:**\n" + "\n".join(body)
        return f"Unknown action {action!r}. One of: touch, claim, release, status, conflicts"
    except TrackerError as exc:
        return f"Error: {exc}"


@mcp.tool()
async def tracker_query(
    ctx: Context,
    view: str = "tasks",
    project: str | None = None,
    status: str | None = None,
    kind: str | None = None,
    tag: str | None = None,
    parent: str | None = None,
    title_pattern: str | None = None,
    columns: list[str] | None = None,
    sort_by: str | None = None,
    sort_descending: bool = True,
    offset: int = 0,
    limit: int = 50,
) -> str:
    """Query the tracker with bounded output (the no-token-flood contract).

    Views:
        tasks    — task table (filters: project/status/kind/tag/parent/title_pattern)
        tree     — indented hierarchy (project required; parent narrows to a subtree)
        rollup   — per project+kind status counts and criteria pass totals
        criteria — all criteria with task keys
        commits  — all commit links
        tags     — tag usage counts
        deps     — dependency edges
        issues   — external issue links
        activity — file-touch log from local agent collaboration (tracker_files)
        claims   — active advisory file claims (leases)
    Pass columns=["schema"] to list a view's columns. limit is capped at 200.
    """
    db = _tracker(ctx)
    limit = max(1, min(limit, QUERY_MAX_LIMIT))
    offset = max(0, offset)
    try:
        if view == "tree":
            if not project:
                return "tree view needs project"
            lines = frames.tree_lines(db.conn, project, parent)
            if not lines:
                return f"No tasks in {project.upper()}" + (f" under {parent}" if parent else "")
            return f"**{project.upper()} tree:**\n```\n" + "\n".join(lines) + "\n```"
        builders = {
            "tasks": lambda: frames.tasks_frame(db.conn, project, status, kind, tag, parent, title_pattern),
            "rollup": lambda: frames.rollup_frame(db.conn, project),
            "criteria": lambda: frames.criteria_frame(db.conn, project),
            "commits": lambda: frames.commits_frame(db.conn, project),
            "tags": lambda: frames.tags_frame(db.conn, project),
            "deps": lambda: frames.deps_frame(db.conn, project),
            "issues": lambda: frames.issues_frame(db.conn, project),
            "activity": lambda: frames.activity_frame(db.conn),
            "claims": lambda: frames.claims_frame(db.conn),
        }
        if view not in builders:
            return (
                f"Unknown view {view!r}. One of: tasks, tree, rollup, criteria, "
                "commits, tags, deps, issues, activity, claims"
            )
        df = builders[view]()
    except TrackerError as exc:
        return f"Error: {exc}"
    if columns == ["schema"]:
        schema_info = [f"- `{col}`: {dtype}" for col, dtype in df.schema.items()]
        return f"**Schema for view '{view}':**\n\n" + "\n".join(schema_info) + f"\n\n{len(df)} total rows"
    if columns:
        valid = [col for col in columns if col in df.columns]
        if not valid:
            return f"No matching columns. Available: {df.columns}"
        df = df.select(valid)
    if sort_by and sort_by in df.columns:
        df = df.sort(sort_by, descending=sort_descending)
    total = len(df)
    df = df.slice(offset, limit)
    title = f"Tracker {view}" + (f" — {project.upper()}" if project else "")
    suffix = f"\n\n*Rows {offset + 1}-{offset + len(df)} of {total}*" if total > len(df) else ""
    return format_dataframe(df, title=title, max_rows=limit) + suffix
