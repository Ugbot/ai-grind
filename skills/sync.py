#!/usr/bin/env python3
"""Flatten the hierarchical catalog/ into a flat mirror Claude Code can load.

Claude discovers skills only as flat <name>/SKILL.md folders under a skills/
root, and it does not recurse into category folders. This script reads
MANIFEST.json (written by harvest.py) and copies every non-archived item into a
flat target tree.

Targets:
  local   (default)  ->  skills/loadable/{skills,commands,agents}/   [fully owned, wiped]
  plugin             ->  <repo>/plugin/{skills,commands,agents}/     [owned subfolders wiped]
  plugin-windows     ->  <repo>/plugin-windows/skills/               [owned subfolders wiped; Windows-only]
  agents             ->  <repo>/.agents/skills/                      [fully owned, wiped; skills only]
  agents-global      ->  ~/.agents/skills/                           [owned subfolder wiped; skills only]
  project            ->  <repo>/.claude/{skills,commands,agents}/    [per-item overwrite]
  global             ->  ~/.claude/{skills,commands,agents}/         [per-item overwrite; --prune]

Skills whose category (authored or catalog) has a `windows` path segment are
Windows-only: every target except `plugin-windows` excludes them, and
`plugin-windows` includes only them.

`local`, `plugin`, `plugin-windows`, `agents` and `agents-global` are wiped
(fully owned, derived output); the two plugin targets and agents-global wipe
only their owned subfolders so the committed `.claude-plugin/plugin.json`
(or a user's other ~/.agents content) survives. project/global are overwritten
per item so unrelated skills already present there are left untouched;
`global --prune` additionally removes items the library once listed but no
longer publishes (never names it has not listed). The `plugin` mirror is the
Claude Code plugin `ai-grind-skills` (plugin/.claude-plugin/plugin.json). It
is committed, not gitignored. The `agents` mirror feeds clients that read the agents.md
convention (skills/<name>/SKILL.md only; commands/agents are Claude-specific)
-- it is generated, gitignored output. Hand-written client configs (.codex/,
.cursor/rules/) are committed source, not sync targets.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path, PurePosixPath, PureWindowsPath

ROOT = Path(__file__).resolve().parent
MANIFEST = ROOT / "MANIFEST.json"
AUTHORED = ROOT / "authored"  # hand-written skills; never touched by harvest.py
MAX_ITEMS = 200  # bound mirrors harvest.py
MAX_AUTHORED = 100  # bound on hand-written skills
MAX_MISSING = 10  # tolerate a few upstream-only assets absent from a partial checkout


def sidelined(category: str) -> bool:
    """True for categories harvested for reference but NOT synced into mirrors:
    any `experimental` segment or one starting with `_` (e.g. `_archive`)."""
    return any(seg == "experimental" or seg.startswith("_") for seg in category.split("/"))


TARGETS = ("local", "plugin", "plugin-windows", "agents", "agents-global", "project", "global")
WINDOWS_TARGETS = ("plugin-windows",)
SKILLS_ONLY_TARGETS = ("agents", "agents-global", "plugin-windows")
# Owned targets whose base also holds committed/foreign files: wipe these subfolders only.
OWNED_SUBDIRS = {
    "plugin": ("skills", "commands", "agents"),
    "plugin-windows": ("skills", "commands", "agents"),
    "agents-global": ("skills",),
}
SHARED_PLUGIN_TARGETS = ("local", "plugin", "plugin-windows", "agents")  # --additive allowed
MAX_PRUNE = 400  # bound on directory entries inspected per pruned folder


def windows_only(category: str) -> bool:
    """True for categories with a `windows` segment (Windows-only skills)."""
    return any(seg == "windows" for seg in category.replace("\\", "/").split("/"))


def target_base(name: str) -> tuple[Path, bool]:
    """Resolve the target root and whether it is wholly owned (safe to wipe)."""
    assert name in TARGETS, f"bad target: {name}"
    bases = {
        "local": (ROOT / "loadable", True),
        "plugin": (ROOT.parent / "plugin", True),
        "plugin-windows": (ROOT.parent / "plugin-windows", True),
        "agents": (ROOT.parent / ".agents", True),
        "agents-global": (Path.home() / ".agents", True),
        "project": (ROOT.parent / ".claude", False),
        "global": (Path.home() / ".claude", False),
    }
    return bases[name]


def wipe_owned(target: str, base: Path) -> None:
    """Wipe an owned target: only its owned subfolders where the base is shared."""
    assert target in TARGETS and target not in ("project", "global"), f"not owned: {target}"
    if not base.exists():
        return
    subdirs = OWNED_SUBDIRS.get(target)
    if subdirs is None:
        shutil.rmtree(base)
        return
    for sub in subdirs:
        if (base / sub).exists():
            shutil.rmtree(base / sub)


def load_manifest() -> list[dict]:
    """Load the harvest manifest; fail clearly if harvest hasn't run."""
    assert MANIFEST.is_file(), "MANIFEST.json missing -- run harvest.py first"
    data = json.loads(MANIFEST.read_text(encoding="utf-8"))
    items = data.get("items", [])
    assert 0 < len(items) <= MAX_ITEMS, f"manifest size out of bounds: {len(items)}"
    return items


def dest_path(rel: str) -> Path:
    """Resolve a manifest `dest` under ROOT, tolerating Windows or POSIX separators.

    MANIFEST.json may be written on either platform, so normalize both `\\` and
    `/` into an OS-native relative path before joining.
    """
    assert rel, "empty manifest dest"
    parts = PureWindowsPath(rel).parts if "\\" in rel else PurePosixPath(rel).parts
    return ROOT.joinpath(*parts)


def read_skill_name(md_path: Path) -> str:
    """Extract the YAML frontmatter `name:` from a skill .md file."""
    assert md_path.is_file(), f"no skill markdown: {md_path}"
    lines = md_path.read_text(encoding="utf-8").splitlines()
    assert lines and lines[0].strip() == "---", f"no frontmatter: {md_path}"
    for i in range(1, min(len(lines), 60)):  # bound the header scan
        if lines[i].strip() == "---":
            break
        if lines[i].startswith("name:"):
            return lines[i].split(":", 1)[1].strip().strip("'\"")
    raise AssertionError(f"no `name:` in frontmatter: {md_path}")


def authored_category(md: Path, root: Path) -> str:
    """Category (posix, relative to `root`) of an authored skill folder or agent file."""
    rel = md.parent.parent if md.name == "SKILL.md" else md.parent
    return "" if rel == root else rel.relative_to(root).as_posix()


def sync_authored(base: Path, seen: set[str], windows: bool = False) -> int:
    """Flatten authored/skills/**/<name>/SKILL.md into base/skills/.

    Only skills whose category has a `windows` segment when `windows`, only the
    others when not."""
    skills_root = AUTHORED / "skills"
    if not skills_root.is_dir():
        return 0
    found = sorted(skills_root.rglob("SKILL.md"))
    assert len(found) <= MAX_AUTHORED, f"authored skills out of bounds: {len(found)}"
    count = 0
    for md in found:
        if windows_only(authored_category(md, skills_root)) != windows:
            continue
        name = read_skill_name(md)
        assert name == md.parent.name, f"folder/name mismatch: {md.parent.name} != {name}"
        assert name not in seen, f"duplicate skill name (authored vs harvested): {name}"
        seen.add(name)
        dest = base / "skills" / name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(md.parent, dest)
        assert (dest / "SKILL.md").is_file(), f"authored sync failed: {dest}"
        count += 1
    assert count <= len(found)
    return count


def sync_skill(item: dict, base: Path, seen: set[str]) -> bool:
    """Copy a catalog skill folder to base/skills/<name>/. Returns False if absent.

    A manifest entry whose source is missing from this checkout (partial clone,
    upstream-only asset) is skipped, not fatal. The caller records and reports it.
    """
    name = item["name"]
    assert name not in seen, f"duplicate skill name in mirror: {name}"
    src_folder = dest_path(item["dest"]).parent
    if not (src_folder / "SKILL.md").is_file():
        return False
    seen.add(name)
    dest = base / "skills" / name
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src_folder, dest)
    assert (dest / "SKILL.md").is_file(), f"skill sync failed: {dest}"
    return True


def sync_flat(item: dict, base: Path, seen: set[str] | None = None) -> bool:
    """Copy a command/agent .md to base/<type>s/<filename>. Returns False if absent."""
    src = dest_path(item["dest"])
    if not src.is_file():
        return False
    sub = "commands" if item["type"] == "command" else "agents"
    dest_dir = base / sub
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.copyfile(src, dest)
    if seen is not None:
        seen.add(f"{sub}/{src.stem}")
    assert dest.is_file(), f"flat sync failed: {dest}"
    return True


def sync_authored_agents(base: Path, seen: set[str], windows: bool = False) -> int:
    """Publish authored Markdown agents; collisions fail before overwriting."""
    agents_root = AUTHORED / "agents"
    if not agents_root.is_dir():
        return 0
    found = sorted(agents_root.rglob("*.md"))
    assert len(found) <= MAX_AUTHORED, f"authored agents out of bounds: {len(found)}"
    normalized = {name.casefold() for name in seen}
    count = 0
    for source in found:
        category = source.parent.relative_to(agents_root).as_posix()
        if sidelined(category) or windows_only(category) != windows:
            continue
        name = read_skill_name(source)
        assert name == source.stem, f"filename/name mismatch: {source.stem} != {name}"
        assert name.casefold() not in normalized, f"duplicate agent name (authored vs catalog): {name}"
        seen.add(name)
        normalized.add(name.casefold())
        destination = base / "agents" / source.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        assert destination.is_file(), f"authored agent sync failed: {destination}"
        count += 1
    assert count <= len(found)
    return count


def selected_sync(target: str, names: list[str]) -> int:
    from additive import publish, read_document, safe_destination, skill_name

    if target not in SHARED_PLUGIN_TARGETS:
        raise ValueError("selected sync only publishes owned local/plugin/plugin-windows/agents mirrors")
    wants_windows = target in WINDOWS_TARGETS
    if not names or len(names) != len(set(names)) or len(names) > MAX_ITEMS:
        raise ValueError("selected names must be explicit, unique and bounded")
    selected = set(names)
    base, _ = target_base(target)
    sources: dict[str, tuple[Path, Path]] = {}

    def add(name: str, source: Path, relative: str) -> None:
        if name.casefold() in sources:
            raise ValueError(f"ambiguous selected name: {name}")
        sources[name.casefold()] = (source, safe_destination(base, relative))

    for item in read_document(MANIFEST)["items"]:
        name, kind = item["name"], item["type"]
        if name not in selected:
            continue
        if kind not in ("skill", "agent") or sidelined(item.get("category", "")):
            raise ValueError("selected asset is not a published skill or agent")
        if windows_only(item.get("category", "")) != wants_windows:
            raise ValueError("selected asset does not belong to this target's platform")
        if target in SKILLS_ONLY_TARGETS and kind != "skill":
            raise ValueError("agents mirror accepts skills only")
        source = safe_destination(ROOT, item["dest"])
        if kind == "skill":
            if skill_name(source) != name:
                raise ValueError("selected catalog name mismatch")
            add(name, source.parent, f"skills/{name}")
        else:
            add(name, source, f"agents/{source.name}")
    for kind in ("skills", "agents"):
        authored = AUTHORED / kind
        found = sorted(authored.rglob("SKILL.md" if kind == "skills" else "*.md")) if authored.is_dir() else []
        if len(found) > MAX_AUTHORED:
            raise ValueError("authored asset count limit")
        for path in found:
            name = path.parent.name if kind == "skills" else path.stem
            if name not in selected or sidelined(path.parent.relative_to(authored).as_posix()):
                continue
            if windows_only(authored_category(path, authored)) != wants_windows:
                raise ValueError("selected asset does not belong to this target's platform")
            if skill_name(path) != name or (target in SKILLS_ONLY_TARGETS and kind != "skills"):
                raise ValueError("selected authored name/type mismatch")
            add(name, path.parent if kind == "skills" else path, f"{kind}/{name}" + (".md" if kind == "agents" else ""))
    if set(sources) != {name.casefold() for name in selected}:
        raise ValueError("selected source names missing")
    publish(list(sources.values()))
    print(f"additive sync: {len(sources)} selected assets -> {base}; unrelated destinations preserved")
    return 0


def ever_listed_names() -> set[str]:
    """Casefolded names the library has ever listed: sources.toml rows (including
    `_disabled`/archived ones) and every authored skill or agent (including
    `_disabled`). Nothing else is known to be ours, so nothing else may be pruned."""
    import tomllib

    names: set[str] = set()
    sources = ROOT / "sources.toml"
    rows = tomllib.loads(sources.read_text(encoding="utf-8")).get("item", []) if sources.is_file() else []
    assert len(rows) <= 10 * MAX_ITEMS, f"sources.toml out of bounds: {len(rows)}"
    for row in rows:
        src = str(row.get("src", "")).replace("\\", "/").rstrip("/")
        leaf = src.rsplit("/", 1)[-1]
        names.add((leaf[:-3] if leaf.endswith(".md") else leaf).casefold())
    for sub, pattern in (("skills", "SKILL.md"), ("agents", "*.md")):
        root = AUTHORED / sub
        found = sorted(root.rglob(pattern)) if root.is_dir() else []
        assert len(found) <= 10 * MAX_AUTHORED, f"authored {sub} out of bounds: {len(found)}"
        names.update((f.parent.name if sub == "skills" else f.stem).casefold() for f in found)
        if sub == "skills":  # disabled authored skills also live outside authored/skills/
            disabled = sorted((AUTHORED / "_disabled").rglob("SKILL.md")) if (AUTHORED / "_disabled").is_dir() else []
            names.update(f.parent.name.casefold() for f in disabled[: 10 * MAX_AUTHORED])
    names.discard("")
    return names


def prune_global(base: Path, published: dict[str, set[str]], ever: set[str]) -> list[str]:
    """Remove items under base/{skills,commands,agents} that the library once
    listed (`ever`) but no longer publishes. Unknown names are never touched."""
    assert base.is_dir(), f"prune base missing: {base}"
    removed: list[str] = []
    for sub, is_dir in (("skills", True), ("commands", False), ("agents", False)):
        folder = base / sub
        if not folder.is_dir():
            continue
        keep = {n.casefold() for n in published[sub]}
        entries = sorted(folder.iterdir())
        assert len(entries) <= 100 * MAX_PRUNE, f"{folder} too large to prune safely"
        for entry in entries:
            if entry.is_symlink() or entry.is_dir() != is_dir or (not is_dir and entry.suffix != ".md"):
                continue
            name = (entry.name if is_dir else entry.stem).casefold()
            if name in ever and name not in keep:
                shutil.rmtree(entry) if is_dir else entry.unlink()
                removed.append(f"{sub}/{entry.name}")
    assert all(r.split("/", 1)[1].rsplit(".md", 1)[0].casefold() in ever for r in removed)
    return removed


def sync_catalog(items: list[dict], base: Path, target: str) -> tuple[dict, list[str], dict[str, set[str]]]:
    """Copy catalog + authored assets for `target`; return counts, missing, published names."""
    windows = target in WINDOWS_TARGETS
    skills_only = target in SKILLS_ONLY_TARGETS  # commands/agents .md are Claude-specific
    seen_skills: set[str] = set()
    flat: set[str] = set()  # "commands/<stem>" and "agents/<stem>"
    counts = {"skill": 0, "command": 0, "agent": 0, "skipped": 0}
    missing: list[str] = []
    for it in items:
        kind = it["type"]
        cat = it.get("category", "")
        if kind == "archive" or sidelined(cat) or windows_only(cat) != windows or (skills_only and kind != "skill"):
            counts["skipped"] += 1
            continue
        copied = sync_skill(it, base, seen_skills) if kind == "skill" else sync_flat(it, base, flat)
        if copied:
            counts[kind] += 1
        else:
            missing.append(f"{kind}:{it.get('name', it['dest'])}")
    # A few upstream-only assets may be absent from a partial checkout; that is
    # tolerable. Wholesale absence means a broken catalog, so fail loud instead.
    assert len(missing) <= MAX_MISSING, f"too many missing sources ({len(missing)}) -- run harvest.py?"
    counts["authored"] = sync_authored(base, seen_skills, windows)
    counts["skill"] += counts["authored"]
    if not skills_only:
        agent_names = {n.split("/", 1)[1] for n in flat if n.startswith("agents/")}
        counts["agent"] += sync_authored_agents(base, agent_names, windows)
        flat |= {f"agents/{n}" for n in agent_names}
    assert counts["skill"] == len(seen_skills), "skill count/name-set mismatch"
    published = {
        "skills": seen_skills,
        "commands": {n.split("/", 1)[1] for n in flat if n.startswith("commands/")},
        "agents": {n.split("/", 1)[1] for n in flat if n.startswith("agents/")},
    }
    return counts, missing, published


def main() -> int:
    ap = argparse.ArgumentParser(description="Flatten catalog/ into a loadable mirror.")
    ap.add_argument("--target", choices=TARGETS, default="local")
    ap.add_argument("--additive", action="store_true")
    ap.add_argument("--select", action="append", default=[])
    ap.add_argument("--prune", action="store_true", help="global only: remove retired library-listed items")
    args = ap.parse_args()
    if args.additive != bool(args.select):
        ap.error("--additive and one or more explicit --select names are required together")
    if args.prune and (args.target != "global" or args.additive):
        ap.error("--prune is only valid with --target global (and not --additive)")
    if args.additive:
        return selected_sync(args.target, args.select)

    items = load_manifest()
    base, owned = target_base(args.target)
    if owned:
        wipe_owned(args.target, base)
    base.mkdir(parents=True, exist_ok=True)
    assert base.is_dir(), f"target base not created: {base}"

    counts, missing, published = sync_catalog(items, base, args.target)
    print(f"synced -> {base}  (target={args.target})")
    print(
        f"  skills={counts['skill']} (harvested={counts['skill'] - counts['authored']} "
        f"authored={counts['authored']}) commands={counts['command']} "
        f"agents={counts['agent']} skipped(archive/platform)={counts['skipped']}"
    )
    if args.prune:
        removed = prune_global(base, published, ever_listed_names())
        print(f"  pruned {len(removed)} retired item(s): {', '.join(removed) or 'none'}")
    if missing:
        print(f"  WARNING: {len(missing)} manifest source(s) absent from this checkout, skipped:")
        for m in missing:
            print(f"    - {m}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
