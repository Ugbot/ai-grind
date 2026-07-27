"""Live skills: SKILL.md documents backed by a CRDT (pycrdt), edited in place
and synced between machines. Action-multiplexed like the tracker tools."""

from __future__ import annotations

import anyio
from mcp.server.fastmcp import Context

from devtools_mcp.server import get_app_ctx, mcp
from devtools_mcp.skilldocs import SkillDocError, SkillDocStore
from devtools_mcp.skilldocs import store as store_mod
from devtools_mcp.skilldocs.control import SkillControl, SkillControlError

GET_PREVIEW_MAX: int = 6_000  # chars of skill content echoed back to the model


def _store(ctx: Context) -> SkillDocStore:
    """The AppContext-owned live-skill store (opened once, closed in cleanup_all).

    Reusing the shared instance avoids leaking a per-call connection and keeps a
    single migrated connection for the whole process.
    """
    store = get_app_ctx(ctx).get_skilldocs()
    assert isinstance(store, SkillDocStore), "app skilldocs store is the wrong type"
    return store


def _bounded(content: str) -> str:
    assert isinstance(content, str), "content must be str"
    if len(content) <= GET_PREVIEW_MAX:
        return content
    omitted = len(content) - GET_PREVIEW_MAX
    return content[:GET_PREVIEW_MAX] + f"\n\n… [{omitted} more chars — patch with surgical old/new strings]"


@mcp.tool()
async def skill_live(
    ctx: Context,
    action: str,
    name: str | None = None,
    content: str | None = None,
    old: str | None = None,
    new: str | None = None,
    url: str | None = None,
) -> str:
    """Live skills: SKILL.md files that are CRDT documents — edit them here,
    sync them between machines, and every peer's skill file updates in place.
    Concurrent edits from different agents/machines merge at character level.

    Actions:
        create  — name + content (full markdown; frontmatter must declare the
                  same `name:` and a `description:`). Materializes immediately.
        get     — name: current merged text (bounded preview)
        list    — all live skills on this machine
        append  — name + content: add to the end (changelog-style growth)
        patch   — name + old + new: surgical find/replace; `old` must match
                  exactly once. Prefer patch/append over rewriting — concurrent
                  whole-document rewrites both survive a merge and duplicate.
        sync    — url (a peer dashboard, e.g. http://other-box:8765): full
                  bidirectional exchange; both sides converge
        publish — re-materialize every live skill to the skills dir
        delete  — name: remove locally (log + file); peers keep their copy
        route   — (re)build the `skill-router` live skill: an auto-generated index
                  of every skill, patched under the live-editable routing rules
        mode    — content=low|high: set the active power mode (optional name for a
                  per-skill override); no content reports the current state. Flips
                  which variant of dynamic skills is materialized.
        enable/disable — name: include/exclude a skill from materialization AND
            the router index. Works for static skills too — disable a skill
            that's irrelevant on this machine (e.g. build tools for a language
            you don't use) and the router stops advertising it; enable restores.
    Files land in ~/.claude/skills/<name>/SKILL.md (DEVTOOLS_MCP_LIVE_SKILLS_DIR
    overrides), so changes are live at the next skill load.
    """
    store = _store(ctx)
    try:
        if action == "create":
            if not name or not content:
                return "create needs name and content"
            path = store.create(name, content)
            where = f" -> `{path}`" if path else " (stored; frontmatter invalid so not materialized)"
            return f"Created live skill **{name}** ({len(content)} chars){where}"
        if action == "get":
            if not name:
                return "get needs name"
            text = store.get_text(name)
            return f"**{name}** ({len(text)} chars):\n\n{_bounded(text)}"
        if action == "list":
            skills = store.list_skills()
            if not skills:
                return "No live skills yet. Create one with action='create'."
            lines = [f"- **{s['name']}** updated {s['updated_at']}" for s in skills[:100]]
            return f"**Live skills ({len(skills)}):**\n" + "\n".join(lines)
        if action == "append":
            if not name or not content:
                return "append needs name and content"
            path = store.append(name, content)
            return f"Appended {len(content)} chars to **{name}**" + (f" -> `{path}`" if path else "")
        if action == "patch":
            if not name or old is None or new is None:
                return "patch needs name, old and new"
            path = store.patch(name, old, new)
            return f"Patched **{name}**" + (f" -> `{path}`" if path else " (not materialized)")
        if action == "sync":
            if not url:
                return "sync needs url (a peer's dashboard, e.g. http://host:8765)"
            from devtools_mcp.net_guard import SsrfError, check_sync_url
            from devtools_mcp.skilldocs.sync import sync_once

            try:
                check_sync_url(url)
            except SsrfError as exc:
                return f"Refused to sync: {exc}"

            def _do_sync() -> dict:
                # Fresh independent store in the worker thread — sqlite is
                # thread-affine, so the shared AppContext connection must not be
                # touched off the event loop; the blocking urllib per-doc runs here.
                thread_store = SkillDocStore()
                try:
                    return sync_once(thread_store, url)
                finally:
                    thread_store.close()

            counters = await anyio.to_thread.run_sync(_do_sync)
            return (
                f"Synced live skills with `{url}`: {counters['skills']} skill(s), "
                f"pulled {counters['pulled']}, pushed {counters['pushed']}, "
                f"materialized {counters['materialized']}"
            )
        if action == "publish":
            written = store.materialize_all()
            return f"Materialized {written} live skill(s) under `{store_mod.publish_root()}`"
        if action == "delete":
            if not name:
                return "delete needs name"
            removed = store.delete(name)
            return f"Deleted live skill **{name}** locally" if removed else f"No live skill {name!r}"
        if action == "route":
            from devtools_mcp.skilldocs import router

            path = router.rebuild(store)
            count = len(router.collect_skills(store))
            where = f" -> `{path}`" if path else ""
            return f"Rebuilt **{router.ROUTER_NAME}** indexing {count} skill(s){where}"
        if action == "mode":
            return _mode_action(store, name, content)
        if action in ("enable", "disable"):
            if not name:
                return f"{action} needs name"
            from devtools_mcp.skilldocs import router

            control = SkillControl(conn=store.conn)
            control.set_disabled(name, action == "disable")
            path = store.materialize(name) if store.exists(name) else None
            router.rebuild(store)  # prune from / restore to the router index immediately
            state = "disabled" if action == "disable" else "enabled"
            return f"Skill **{name}** {state} and the router index rebuilt" + (f" -> `{path}`" if path else "")
        return (
            f"Unknown action {action!r}. One of: create, get, list, append, patch, "
            "sync, publish, delete, route, mode, enable, disable"
        )
    except (SkillDocError, SkillControlError) as exc:
        return f"Error: {exc}"
    # store is owned by the AppContext (closed in cleanup_all) — do not close here.


def _mode_action(store: SkillDocStore, name: str | None, content: str | None) -> str:
    """get/set the power mode. content=low|high sets it (global, or per-skill when
    name is given); empty content reports current state."""
    control = SkillControl(conn=store.conn)
    if not content:
        overrides = control.overrides()
        disabled = sorted(control.disabled())
        lines = [f"**Active power mode:** {control.global_mode()}"]
        if overrides:
            lines.append("Overrides: " + ", ".join(f"{k}={v}" for k, v in sorted(overrides.items())))
        if disabled:
            lines.append("Disabled: " + ", ".join(disabled))
        return "\n".join(lines)
    if name:
        control.set_override(name, content)
        scope = f"override for **{name}**"
    else:
        control.set_mode(content)
        scope = "global mode"
    written = store.materialize_all()
    from devtools_mcp.skilldocs import router

    router.rebuild(store)
    return f"Set {scope} = **{content}**; re-materialized {written} skill(s)."
