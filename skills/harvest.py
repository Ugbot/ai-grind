#!/usr/bin/env python3
"""Copy every upstream skill/command/agent listed in sources.toml into the
hierarchical catalog/ tree. Copy-only: upstream files are never modified.

Re-running refreshes catalog/ from upstream (idempotent). catalog/ is owned by
this script and wiped on each run; sync.py reads it but never writes it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CATALOG = ROOT / "catalog"
SOURCES = ROOT / "sources.toml"
MANIFEST = ROOT / "MANIFEST.json"

MAX_ITEMS = 200  # bound: the work-list is small and explicit
MAX_FILE_BYTES = 1 << 20  # bound: no skill file should exceed 1 MiB
TYPE_DIR = {"skill": "skills", "command": "commands", "agent": "agents", "archive": "skills"}


def source_path(source: str) -> Path:
    """Resolve the explicit source; ~ keeps installed skills portable per user."""
    assert source and isinstance(source, str)
    result = Path(source).expanduser()
    assert not str(result).startswith("~")
    return result


def load_items(path: Path, selected: set[str] | None = None) -> list[dict]:
    """Parse sources.toml into a bounded, validated list of item rows."""
    assert path.is_file(), f"missing sources file: {path}"
    if selected is None:
        with path.open("rb") as fh:
            data = tomllib.load(fh)
    else:
        from additive import read_bytes

        data = tomllib.loads(read_bytes(path).decode("utf-8"))
    items = data.get("item", [])
    assert 0 < len(items) <= MAX_ITEMS, f"item count out of bounds: {len(items)}"
    if selected is not None:
        items = [it for it in items if source_path(it["src"]).stem in selected]
        assert len(items) == len(selected), "selected source names missing or ambiguous"
    for it in items:
        assert it["type"] in TYPE_DIR, f"bad type: {it.get('type')!r}"
        assert source_path(it["src"]).exists(), f"source not found: {it['src']}"
    return items


def sha256_of(path: Path) -> str:
    """Hash a single file; bounded read guards against runaway inputs."""
    assert path.is_file(), f"not a file: {path}"
    raw = path.read_bytes()
    assert len(raw) <= MAX_FILE_BYTES, f"file too large: {path} ({len(raw)} B)"
    return hashlib.sha256(raw).hexdigest()


def read_skill_name(md_path: Path) -> str:
    """Extract the YAML frontmatter `name:` from a skill .md file."""
    assert md_path.is_file(), f"no skill markdown: {md_path}"
    lines = md_path.read_text(encoding="utf-8").splitlines()
    assert lines and lines[0].strip() == "---", f"no frontmatter: {md_path}"
    name = ""
    for i in range(1, min(len(lines), 60)):  # bound the header scan
        if lines[i].strip() == "---":
            break
        if lines[i].startswith("name:"):
            name = lines[i].split(":", 1)[1].strip().strip("'\"")
            break
    assert name, f"no `name:` in frontmatter: {md_path}"
    return name


def copy_skill(item: dict, seen: set[str]) -> dict:
    """Copy a folder- or single-file skill into catalog/skills/<category>/<name>/."""
    src = source_path(item["src"])
    dest_root = CATALOG / "skills" / item["category"]
    if src.is_dir():
        md = src / "SKILL.md"
        name = read_skill_name(md)
        dest = dest_root / name
        shutil.copytree(src, dest)
        defining = dest / "SKILL.md"
    else:
        name = read_skill_name(src)
        dest = dest_root / name
        dest.mkdir(parents=True, exist_ok=False)
        defining = dest / "SKILL.md"
        shutil.copyfile(src, defining)
    assert name not in seen, f"duplicate skill name: {name} (from {src})"
    seen.add(name)
    assert defining.is_file(), f"copy failed: {defining}"
    return {
        "name": name,
        "type": "skill",
        "category": item["category"],
        "origin": str(src),
        "dest": str(defining.relative_to(ROOT)),
        "sha256": sha256_of(defining),
    }


def copy_flat(item: dict) -> dict:
    """Copy a command/agent .md verbatim into catalog/<type>s/<category>/."""
    src = source_path(item["src"])
    assert src.is_file() and src.suffix == ".md", f"not a .md: {src}"
    dest_dir = CATALOG / TYPE_DIR[item["type"]] / item["category"]
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.copyfile(src, dest)
    assert dest.is_file(), f"copy failed: {dest}"
    return {
        "name": src.stem,
        "type": item["type"],
        "category": item["category"],
        "origin": str(src),
        "dest": str(dest.relative_to(ROOT)),
        "sha256": sha256_of(dest),
    }


def copy_archive(item: dict) -> dict:
    """Copy an archived folder verbatim; excluded from sync and name checks."""
    src = source_path(item["src"])
    assert src.is_dir(), f"archive must be a dir: {src}"
    dest = CATALOG / "skills" / item["category"]
    shutil.copytree(src, dest)
    assert dest.is_dir(), f"archive copy failed: {dest}"
    return {
        "name": src.name,
        "type": "archive",
        "category": item["category"],
        "origin": str(src),
        "dest": str(dest.relative_to(ROOT)),
        "sha256": "",
    }


def selected_harvest(names: list[str]) -> int:
    # Selected mode alone imports publication support; legacy full harvest keeps
    # its existing behavior and source requirements.
    from additive import (
        metadata_bytes,
        publish,
        read_bytes,
        read_document,
        reject_output_links,
        safe_destination,
        skill_name,
        write_metadata,
    )

    if not names or len(names) != len(set(names)) or len(names) > MAX_ITEMS:
        raise ValueError("selected names must be explicit, unique and bounded")
    items = load_items(SOURCES, set(names))
    reject_output_links(MANIFEST)  # metadata is an output too; preflight before reading or publication
    original = read_bytes(MANIFEST) if MANIFEST.exists() else None
    document = read_document(MANIFEST) if original is not None else {"items": [], "counts": {}}
    existing = document.get("items")
    if not isinstance(existing, list) or len(existing) > MAX_ITEMS:
        raise ValueError("invalid existing manifest")
    merged = list(existing)
    known = {entry["name"].casefold(): entry for entry in existing}
    if len(known) != len(existing):
        raise ValueError("ambiguous existing manifest names")
    plans = []
    for item in items:
        if item["type"] != "skill":
            raise ValueError("selected harvest currently accepts skills only")
        source = source_path(item["src"])
        if not source.is_dir():
            raise ValueError("selected harvest requires folder skills")
        name = skill_name(source / "SKILL.md")
        if name not in names or name != source.name:
            raise ValueError("selected source folder/name mismatch")
        destination = safe_destination(CATALOG, f"skills/{item['category']}/{name}")
        record = {
            "name": name,
            "type": "skill",
            "category": item["category"],
            "origin": item["src"] if item["src"].startswith(("~/", "~\\")) else str(source),
            "dest": str((destination / "SKILL.md").relative_to(ROOT)),
            "sha256": hashlib.sha256(read_bytes(source / "SKILL.md")).hexdigest(),
        }
        previous = known.get(name.casefold())
        if previous is not None:
            normalized = dict(previous)
            normalized["dest"] = str(safe_destination(ROOT, previous["dest"]).relative_to(ROOT))
            if previous["origin"] != record["origin"]:
                if source_path(previous["origin"]) != source:
                    raise ValueError(f"selected manifest origin conflicts: {name}")
                normalized["origin"] = record["origin"]
            if normalized != record:
                raise ValueError(f"selected manifest conflicts: {name}")
        else:
            merged.append(record)
            known[name.casefold()] = record
        plans.append((source, destination))
    if len(merged) > MAX_ITEMS:
        raise ValueError("selected manifest count limit")
    result = dict(document)
    result["items"] = merged
    result["counts"] = dict(document.get("counts", {}))
    result["counts"].update({kind: sum(entry["type"] == kind for entry in merged) for kind in TYPE_DIR})
    metadata_bytes(result)  # shape/serialized budget errors must precede outputs
    publish(plans)
    write_metadata(MANIFEST, result, original)  # outputs first; retry fills only missing identical files
    print(f"additive harvest: {len(plans)} selected skills; unrelated catalog and manifest entries preserved")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--additive", action="store_true")
    parser.add_argument("--select", action="append", default=[])
    args = parser.parse_args()
    if args.additive != bool(args.select):
        parser.error("--additive and one or more explicit --select names are required together")
    if args.additive:
        return selected_harvest(args.select)
    items = load_items(SOURCES)
    if CATALOG.exists():
        shutil.rmtree(CATALOG)
    CATALOG.mkdir(parents=True, exist_ok=False)
    assert CATALOG.is_dir(), "catalog/ not created"

    seen_skills: set[str] = set()
    manifest: list[dict] = []
    for it in items:
        if it["type"] == "skill":
            manifest.append(copy_skill(it, seen_skills))
        elif it["type"] == "archive":
            manifest.append(copy_archive(it))
        else:
            manifest.append(copy_flat(it))

    counts = {t: sum(1 for m in manifest if m["type"] == t) for t in ("skill", "command", "agent", "archive")}
    assert len(manifest) == len(items), "manifest/item count mismatch"
    MANIFEST.write_text(json.dumps({"counts": counts, "items": manifest}, indent=2), encoding="utf-8")

    print(f"harvested {len(manifest)} items -> {CATALOG}")
    print(
        f"  skills={counts['skill']} commands={counts['command']} "
        f"agents={counts['agent']} archive={counts['archive']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
