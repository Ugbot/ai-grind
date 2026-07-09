"""skills_sync: drive the static skills library (harvest.py / sync.py) from MCP.

Distinct from skill_live (CRDT live skills): this manages the *static* library —
harvested catalog/ + hand-written authored/ flattened into loadable mirrors
(plugin bundle, loadable/, .agents/, project/global .claude dirs).
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys

from mcp.server.fastmcp import Context

from devtools_mcp import skills_discovery as discovery
from devtools_mcp.server import mcp

SYNC_TARGETS = ("local", "plugin", "agents", "project", "global")
# "all" fans out to the wholly-owned derived mirrors only; project/global write
# into .claude dirs shared with other tooling, so they stay explicit-only.
OWNED_TARGETS = ("local", "plugin", "agents")

_SCRIPT_TIMEOUT = 120  # seconds per script run
_OUTPUT_TAIL_LINES = 30
_MAX_AUTHORED_SCAN = 200


def find_skills_root() -> pathlib.Path | None:
    """Locate the skills library: $DEVTOOLS_MCP_SKILLS_ROOT -> repo checkout.

    The library lives in the ai-grind checkout (skills/ beside src/), not in the
    installed package, so an installed server needs the env override.
    """
    env = os.environ.get("DEVTOOLS_MCP_SKILLS_ROOT")
    if env:
        root = pathlib.Path(env)
        return root if (root / "sync.py").is_file() else None
    import devtools_mcp

    package_dir = pathlib.Path(devtools_mcp.__file__).resolve().parent
    for up in range(1, 4):  # bounded walk: src layout puts the repo 2 levels up
        candidate = package_dir.parents[up - 1] / "skills"
        if (candidate / "sync.py").is_file() and (candidate / "harvest.py").is_file():
            return candidate
    return None


def library_status(root: pathlib.Path) -> str:
    """Bounded status: manifest counts, authored count, mirror freshness."""
    assert root.is_dir(), f"skills root missing: {root}"
    parts = [f"**Skills library:** `{root}`", ""]
    manifest = root / "MANIFEST.json"
    if manifest.is_file():
        items = json.loads(manifest.read_text(encoding="utf-8")).get("items", [])
        by_type: dict[str, int] = {}
        for item in items[:500]:
            by_type[item.get("type", "?")] = by_type.get(item.get("type", "?"), 0) + 1
        counts = ", ".join(f"{k}={v}" for k, v in sorted(by_type.items()))
        parts.append(f"**Harvested (MANIFEST.json):** {len(items)} items ({counts})")
    else:
        parts.append("**Harvested:** no MANIFEST.json — run action='harvest' first")
    authored = sorted((root / "authored" / "skills").rglob("SKILL.md"))[:_MAX_AUTHORED_SCAN]
    parts.append(f"**Authored:** {len(authored)} skills under authored/skills/")
    parts.append("")
    parts.append("**Mirrors:**")
    mirrors = {
        "local": root / "loadable",
        "plugin": root.parent / "plugin",
        "agents": root.parent / ".agents",
        "project": root.parent / ".claude",
        "global": pathlib.Path.home() / ".claude",
    }
    for name, base in mirrors.items():
        skills_dir = base / "skills"
        count = len(list(skills_dir.iterdir())) if skills_dir.is_dir() else 0
        state = f"{count} skills" if count else "absent"
        parts.append(f"- {name}: `{base}` — {state}")
    parts.append("")
    parts.append("Sync with action='sync', target='plugin'|'local'|'agents'|'project'|'global'|'all'.")
    return "\n".join(parts)


async def run_script(root: pathlib.Path, script: str, args: list[str]) -> tuple[int, str]:
    """Run one library script with the server's interpreter; bounded output tail."""
    assert script in ("sync.py", "harvest.py"), f"unexpected script {script!r}"
    script_path = root / script
    assert script_path.is_file(), f"missing script: {script_path}"
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script_path),
        *args,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=str(root),
    )
    try:
        out_bytes, _ = await asyncio.wait_for(proc.communicate(), timeout=_SCRIPT_TIMEOUT)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        return 124, f"{script} timed out after {_SCRIPT_TIMEOUT}s"
    text = out_bytes.decode("utf-8", errors="replace").strip()
    tail = "\n".join(text.splitlines()[-_OUTPUT_TAIL_LINES:])
    return proc.returncode or 0, tail


@mcp.tool()
async def skills_sync(
    ctx: Context,
    action: str = "status",
    target: str = "",
    src: str = "",
    category: str = "",
    note: str = "",
) -> str:
    """Manage the static skills library: harvest upstream assets, discover
    unharvested ones across the machine, and sync the flat mirrors that
    Claude Code (and other clients) load.

    Actions:
        status   — library location, harvested/authored counts, mirror freshness
        discover — scan every project .claude/{skills,commands,agents} dir
                   (roots derived from sources.toml + ~/.claude +
                   $DEVTOOLS_MCP_SKILL_SCAN_ROOTS) for assets not yet in the
                   library; reports candidates and malformed entries
        adopt    — src (path to a discovered asset) + category (+ note):
                   validate, append an [[item]] to sources.toml, re-harvest
        harvest  — re-copy upstream assets from sources.toml into catalog/
                   (refreshes MANIFEST.json; run after editing sources.toml)
        sync     — flatten catalog/ + authored/ into a mirror. target one of:
                   local (skills/loadable), plugin (committed plugin bundle),
                   agents (.agents), project (<repo>/.claude), global (~/.claude),
                   or 'all' (= the derived mirrors: local+plugin+agents).

    Skill anatomy: folder-form skills are <name>/SKILL.md (frontmatter name ==
    folder name, bundled assets copied whole); single-file <name>.md skills are
    wrapped at harvest; commands/agents are flat .md files. After adding or
    editing: action='sync', target='all', then commit the plugin/ changes.
    New skills appear in sessions after a plugin reload. Live CRDT skills are
    a separate system — see skill_live.
    """
    root = find_skills_root()
    if root is None:
        return (
            "Skills library not found. Set DEVTOOLS_MCP_SKILLS_ROOT to the ai-grind "
            "skills/ directory (the one containing sync.py and harvest.py)."
        )

    if action == "status":
        return library_status(root)

    if action == "discover":
        candidates = discovery.discover(root)
        return discovery.format_candidates(candidates, root)

    if action == "adopt":
        if not src:
            return "adopt needs src (path from action='discover') and category"
        error = discovery.adopt(root, src, category, note)
        if error:
            return error
        code, tail = await run_script(root, "harvest.py", [])
        status = "ok" if code == 0 else f"FAILED (exit {code})"
        return (
            f"Adopted `{src}` into sources.toml (category={category}).\n\n"
            f"**harvest.py** — {status}\n```\n{tail}\n```\n\n"
            "_Next: action='sync', target='all', then commit sources.toml + catalog/ + plugin/._"
        )

    if action == "harvest":
        code, tail = await run_script(root, "harvest.py", [])
        status = "ok" if code == 0 else f"FAILED (exit {code})"
        return f"**harvest.py** — {status}\n```\n{tail}\n```"

    if action == "sync":
        if not target:
            return f"sync needs target: one of {', '.join(SYNC_TARGETS)} or 'all'"
        targets = list(OWNED_TARGETS) if target == "all" else [target]
        for t in targets:
            if t not in SYNC_TARGETS:
                return f"Unknown target {t!r}: one of {', '.join(SYNC_TARGETS)} or 'all'"
        assert 0 < len(targets) <= len(SYNC_TARGETS), "target fan-out out of bounds"
        parts = []
        for t in targets:
            code, tail = await run_script(root, "sync.py", ["--target", t])
            status = "ok" if code == 0 else f"FAILED (exit {code})"
            parts.append(f"**sync --target {t}** — {status}\n```\n{tail}\n```")
            if code != 0:
                break
        if target == "all" and all(p.count("FAILED") == 0 for p in parts):
            parts.append("_plugin/ is committed output — review `git diff plugin/` and commit._")
        return "\n\n".join(parts)

    return f"Unknown action {action!r}: one of status, discover, adopt, harvest, sync"
