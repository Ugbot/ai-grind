"""Discover unharvested skills/commands/agents across the machine and adopt them.

MCP-free and testable; the skills_sync tool wraps this. Discovery SCANS and
REPORTS — sources.toml stays the single explicit source of truth (harvest.py
never crawls). Adoption appends a validated [[item]] block to sources.toml.

Skill folder anatomy (the forms that exist in the wild):
  folder-form  <name>/SKILL.md  — frontmatter `name:` must equal the folder
               name; any other files in the folder (references/, scripts) are
               bundled assets, copied whole.
  single-file  <name>.md        — frontmatter `name:`; harvest.py wraps it
               into <name>/SKILL.md.
  command      .claude/commands/<name>.md — flat file, name = stem.
  agent        .claude/agents/<name>.md   — flat file with frontmatter.
Clients load flat skills/<name>/SKILL.md only — never category subtrees.
"""

from __future__ import annotations

import json
import os
import pathlib
from dataclasses import dataclass

MAX_SCAN_PROJECTS = 256
MAX_ENTRIES_PER_DIR = 200
MAX_CANDIDATES = 100
_FRONTMATTER_SCAN_LINES = 60

_KIND_SUBDIRS = {"skill": "skills", "command": "commands", "agent": "agents"}


@dataclass(frozen=True)
class Candidate:
    """One discovered asset not yet in the library."""

    kind: str  # "skill" | "command" | "agent"
    form: str  # "folder" | "file"
    name: str
    src: str  # absolute path, forward slashes
    issue: str = ""  # non-empty = malformed, listed but not adoptable


def find_skills_root() -> pathlib.Path | None:
    """Locate the skills library: $DEVTOOLS_MCP_SKILLS_ROOT -> repo checkout.

    The library lives in the ai-grind checkout (skills/ beside src/), not in the
    installed package, so an installed server needs the env override. MCP-free so
    router.py and the skills_sync tool can both call it.
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


def read_frontmatter_name(md_path: pathlib.Path) -> str | None:
    """Tolerant frontmatter `name:` reader; None if absent/invalid."""
    try:
        lines = md_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    if not lines or lines[0].strip() != "---":
        return None
    for line in lines[1:_FRONTMATTER_SCAN_LINES]:
        if line.strip() == "---":
            break
        if line.startswith("name:"):
            return line.split(":", 1)[1].strip().strip("'\"") or None
    return None


def library_names(root: pathlib.Path) -> set[str]:
    """Every name already in the library: manifest items + authored skills."""
    assert root.is_dir(), f"skills root missing: {root}"
    names: set[str] = set()
    manifest = root / "MANIFEST.json"
    if manifest.is_file():
        items = json.loads(manifest.read_text(encoding="utf-8")).get("items", [])
        names.update(str(item.get("name", "")) for item in items)
    authored = root / "authored" / "skills"
    if authored.is_dir():
        for md in sorted(authored.rglob("SKILL.md"))[:MAX_ENTRIES_PER_DIR]:
            names.add(md.parent.name)
    names.discard("")
    return names


def _raw_srcs(root: pathlib.Path) -> list[str]:
    """Original-case `src` strings from sources.toml (forward-slash normalized)."""
    sources = root / "sources.toml"
    if not sources.is_file():
        return []
    import tomllib

    with sources.open("rb") as fh:
        data = tomllib.load(fh)
    return [str(item["src"]).replace("\\", "/") for item in data.get("item", []) if "src" in item]


def source_paths(root: pathlib.Path) -> set[str]:
    """`src` paths already listed in sources.toml, normalized for dedup comparison."""
    return {_norm(src) for src in _raw_srcs(root)}


def _norm(path: str | pathlib.Path) -> str:
    """Case-insensitive comparison key — for DEDUP only, never for real Paths.

    (Lowercasing a filesystem path corrupts it on case-sensitive systems and
    folds macOS temp dirs like /T/ to /t/; derive real Paths from originals.)
    """
    return str(pathlib.Path(path)).replace("\\", "/").rstrip("/").lower()


def scan_roots(root: pathlib.Path) -> list[pathlib.Path]:
    """Where to look: parents of known project origins + ~/.claude + env.

    Derived from sources.toml srcs (e.g. C:/code/llm-station/.claude/... makes
    C:/code a scan root), so the scan follows wherever the library already
    harvests from. $DEVTOOLS_MCP_SKILL_SCAN_ROOTS (os.pathsep-separated) adds
    more. The home directory itself is never a project scan root.
    """
    home = pathlib.Path.home()
    marker = "/.claude/"
    roots: set[pathlib.Path] = set()
    for src in _raw_srcs(root):  # original case — the derived Path must stay real
        lower = src.lower()
        if marker not in lower:
            continue
        project = pathlib.Path(src[: lower.index(marker)])
        parent = project.parent
        if parent != home and parent != parent.parent:  # skip home and drive roots
            roots.add(parent)
    env = os.environ.get("DEVTOOLS_MCP_SKILL_SCAN_ROOTS", "")
    for extra in env.split(os.pathsep):
        if extra.strip():
            roots.add(pathlib.Path(extra.strip()))
    ordered = sorted(r for r in roots if r.is_dir())
    assert len(ordered) <= 32, f"implausible scan-root count: {len(ordered)}"
    return ordered


def _client_dirs(root: pathlib.Path) -> list[pathlib.Path]:
    """All .claude dirs to inspect: per-project under scan roots + global."""
    library_repo = root.parent  # ai-grind's own .claude is a sync target, skip
    dirs: list[pathlib.Path] = []
    for scan_root in scan_roots(root):
        children = sorted(p for p in scan_root.iterdir() if p.is_dir())[:MAX_SCAN_PROJECTS]
        for project in children:
            if project == library_repo:
                continue
            claude = project / ".claude"
            if claude.is_dir():
                dirs.append(claude)
    global_claude = pathlib.Path.home() / ".claude"
    if global_claude.is_dir():
        dirs.append(global_claude)
    assert len(dirs) <= MAX_SCAN_PROJECTS + 1, "client dir scan out of bounds"
    return dirs


def _skill_candidates(skills_dir: pathlib.Path) -> list[Candidate]:
    """Classify entries of one .claude/skills dir (folder + single-file forms)."""
    out: list[Candidate] = []
    entries = sorted(skills_dir.iterdir())[:MAX_ENTRIES_PER_DIR]
    for entry in entries:
        if entry.is_dir():
            md = entry / "SKILL.md"
            if not md.is_file():
                out.append(Candidate("skill", "folder", entry.name, _norm(entry), issue="no SKILL.md in folder"))
                continue
            name = read_frontmatter_name(md)
            if name is None:
                out.append(Candidate("skill", "folder", entry.name, _norm(entry), issue="no frontmatter name:"))
            elif name != entry.name:
                issue = f"frontmatter name {name!r} != folder name"
                out.append(Candidate("skill", "folder", name, _norm(entry), issue=issue))
            else:
                out.append(Candidate("skill", "folder", name, _norm(entry)))
        elif entry.suffix == ".md" and entry.name not in ("README.md",):
            name = read_frontmatter_name(entry) or entry.stem
            out.append(Candidate("skill", "file", name, _norm(entry)))
    return out


def discover(root: pathlib.Path) -> list[Candidate]:
    """Scan for assets not yet in the library. Bounded, dedup by name + src."""
    known_names = library_names(root)
    known_srcs = source_paths(root)
    found: list[Candidate] = []
    seen: set[str] = set()
    for claude_dir in _client_dirs(root):
        candidates: list[Candidate] = []
        skills_dir = claude_dir / "skills"
        if skills_dir.is_dir():
            candidates.extend(_skill_candidates(skills_dir))
        for kind in ("command", "agent"):
            flat_dir = claude_dir / _KIND_SUBDIRS[kind]
            if flat_dir.is_dir():
                for md in sorted(flat_dir.glob("*.md"))[:MAX_ENTRIES_PER_DIR]:
                    candidates.append(Candidate(kind, "file", md.stem, _norm(md)))
        for cand in candidates:
            if cand.name in known_names or cand.src in known_srcs or cand.src in seen:
                continue
            seen.add(cand.src)
            found.append(cand)
            if len(found) >= MAX_CANDIDATES:
                return found
    return found


def format_candidates(candidates: list[Candidate], root: pathlib.Path) -> str:
    """Bounded report with copy-pasteable adopt calls."""
    if not candidates:
        return "No unharvested skills/commands/agents found — the library covers every scanned client dir."
    assert len(candidates) <= MAX_CANDIDATES, "candidate list out of bounds"
    clean = [c for c in candidates if not c.issue]
    broken = [c for c in candidates if c.issue]
    parts = [f"**{len(candidates)} candidate(s) not in the library** (scanned via `{root / 'sources.toml'}`):", ""]
    for cand in clean[:40]:
        parts.append(f"- {cand.kind} `{cand.name}` ({cand.form}) — `{cand.src}`")
    if len(clean) > 40:
        parts.append(f"... {len(clean) - 40} more")
    if clean:
        example = clean[0]
        parts.append("")
        parts.append(
            f'Adopt one: skills_sync(action="adopt", src="{example.src}", category="<category>", '
            'note="...") — appends to sources.toml and re-harvests.'
        )
    if broken:
        parts.append("")
        parts.append("**Malformed (fix before adopting):**")
        parts.extend(f"- `{c.src}` — {c.issue}" for c in broken[:15])
    return "\n".join(parts)


def adopt(root: pathlib.Path, src: str, category: str, note: str = "") -> str | None:
    """Validate and append one [[item]] to sources.toml. Returns error or None."""
    assert root.is_dir(), f"skills root missing: {root}"
    if not category.strip():
        return "adopt needs category (the catalog sub-folder, e.g. 'profiling')"
    path = pathlib.Path(src)
    if not path.exists():
        return f"src not found: {src}"

    kind = "skill"
    normalized = _norm(path)
    if "/.claude/commands/" in normalized:
        kind = "command"
    elif "/.claude/agents/" in normalized:
        kind = "agent"

    if kind == "skill":
        md = path / "SKILL.md" if path.is_dir() else path
        name = read_frontmatter_name(md)
        if name is None:
            return f"not adoptable: {md} has no frontmatter `name:`"
        if path.is_dir() and name != path.name:
            return f"not adoptable: frontmatter name {name!r} != folder name {path.name!r}"
    else:
        if path.suffix != ".md":
            return f"{kind} must be a .md file: {src}"
        name = path.stem

    if name in library_names(root):
        return f"{kind} {name!r} is already in the library"
    if _norm(path) in source_paths(root):
        return f"src already listed in sources.toml: {src}"

    src_value = str(path.resolve()).replace("\\", "/")
    block_lines = ["", "[[item]]", f'src = "{src_value}"', f'type = "{kind}"', f'category = "{category.strip()}"']
    if note.strip():
        escaped = note.strip().replace('"', "'")
        block_lines.append(f'note = "{escaped}"')
    sources = root / "sources.toml"
    with sources.open("a", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(block_lines) + "\n")
    assert _norm(path) in source_paths(root), "adopt failed to register in sources.toml"
    return None
