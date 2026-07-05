#!/usr/bin/env python3
"""Flatten the hierarchical catalog/ into a flat mirror Claude Code can load.

Claude discovers skills only as flat <name>/SKILL.md folders under a skills/
root -- it does not recurse into category folders. This script reads
MANIFEST.json (written by harvest.py) and copies every non-archived item into a
flat target tree.

Targets:
  local   (default)  ->  skills/loadable/{skills,commands,agents}/   [fully owned, wiped]
  plugin             ->  <repo>/plugin/{skills,commands,agents}/     [fully owned, wiped]
  project            ->  <repo>/.claude/{skills,commands,agents}/    [per-item overwrite]
  global             ->  ~/.claude/{skills,commands,agents}/         [per-item overwrite]

`local` and `plugin` are wiped wholesale (fully owned, derived output).
project/global are overwritten per item so unrelated skills already present
there are left untouched. The `plugin` mirror is the flat bundle the Claude Code
plugin (.claude-plugin/plugin.json) points at -- it is committed, not gitignored.
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


def target_base(name: str) -> tuple[Path, bool]:
    """Resolve the target root and whether it is wholly owned (safe to wipe)."""
    assert name in ("local", "plugin", "project", "global"), f"bad target: {name}"
    if name == "local":
        return ROOT / "loadable", True
    if name == "plugin":
        return ROOT.parent / "plugin", True
    if name == "project":
        return ROOT.parent / ".claude", False
    return Path.home() / ".claude", False


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


def sync_authored(base: Path, seen: set[str]) -> int:
    """Flatten hand-written authored/skills/**/<name>/SKILL.md into base/skills/."""
    skills_root = AUTHORED / "skills"
    if not skills_root.is_dir():
        return 0
    found = sorted(skills_root.rglob("SKILL.md"))
    assert len(found) <= MAX_AUTHORED, f"authored skills out of bounds: {len(found)}"
    for md in found:
        name = read_skill_name(md)
        assert name == md.parent.name, f"folder/name mismatch: {md.parent.name} != {name}"
        assert name not in seen, f"duplicate skill name (authored vs harvested): {name}"
        seen.add(name)
        dest = base / "skills" / name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(md.parent, dest)
        assert (dest / "SKILL.md").is_file(), f"authored sync failed: {dest}"
    return len(found)


def sync_skill(item: dict, base: Path, seen: set[str]) -> bool:
    """Copy a catalog skill folder to base/skills/<name>/. Returns False if absent.

    A manifest entry whose source is missing from this checkout (partial clone,
    upstream-only asset) is skipped, not fatal -- the caller records and reports it.
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


def sync_flat(item: dict, base: Path) -> bool:
    """Copy a command/agent .md to base/<type>s/<filename>. Returns False if absent."""
    src = dest_path(item["dest"])
    if not src.is_file():
        return False
    sub = "commands" if item["type"] == "command" else "agents"
    dest_dir = base / sub
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.copyfile(src, dest)
    assert dest.is_file(), f"flat sync failed: {dest}"
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description="Flatten catalog/ into a loadable mirror.")
    ap.add_argument("--target", choices=("local", "plugin", "project", "global"), default="local")
    args = ap.parse_args()

    items = load_manifest()
    base, owned = target_base(args.target)
    if owned and base.exists():
        shutil.rmtree(base)
    base.mkdir(parents=True, exist_ok=True)
    assert base.is_dir(), f"target base not created: {base}"

    seen_skills: set[str] = set()
    counts = {"skill": 0, "command": 0, "agent": 0, "skipped": 0}
    missing: list[str] = []
    for it in items:
        kind = it["type"]
        if kind == "archive":
            counts["skipped"] += 1
            continue
        copied = sync_skill(it, base, seen_skills) if kind == "skill" else sync_flat(it, base)
        if copied:
            counts[kind] += 1
        else:
            missing.append(f"{kind}:{it.get('name', it['dest'])}")

    # A few upstream-only assets may be absent from a partial checkout; that is
    # tolerable. Wholesale absence means a broken catalog -- fail loud instead.
    assert len(missing) <= MAX_MISSING, f"too many missing sources ({len(missing)}) -- run harvest.py?"

    authored = sync_authored(base, seen_skills)
    counts["skill"] += authored

    assert counts["skill"] == len(seen_skills), "skill count/name-set mismatch"
    print(f"synced -> {base}  (target={args.target})")
    print(
        f"  skills={counts['skill']} (harvested={counts['skill'] - authored} "
        f"authored={authored}) commands={counts['command']} "
        f"agents={counts['agent']} skipped(archive)={counts['skipped']}"
    )
    if missing:
        print(f"  WARNING: {len(missing)} manifest source(s) absent from this checkout, skipped:")
        for m in missing:
            print(f"    - {m}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
