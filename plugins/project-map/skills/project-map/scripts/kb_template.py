"""kb_template.py: template a project in one command (`kb.py template`).

Detects what it can (name, kind, languages, build/test commands, layout,
areas, entry points, the README's opening paragraph), renders the templates
with those facts filled in, stubs an area CLAUDE.md per area, vendors the
tools, and builds the index. It never overwrites an existing file.

What it cannot know (the WHY behind each area, the hard rules, the
invariants) stays as visible `{{…}}` markers that `kb.py check` counts until
an agent (or a human) fills them from the code.
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

LANG_BY_EXT = {
    ".py": "Python", ".ts": "TypeScript", ".tsx": "TypeScript", ".js": "JavaScript",
    ".jsx": "JavaScript", ".mjs": "JavaScript", ".go": "Go", ".rs": "Rust",
    ".c": "C", ".h": "C/C++", ".cc": "C++", ".cpp": "C++", ".cxx": "C++",
    ".hpp": "C++", ".java": "Java", ".kt": "Kotlin", ".cs": "C#",
    ".swift": "Swift", ".rb": "Ruby", ".php": "PHP", ".scala": "Scala",
    ".lua": "Lua", ".dart": "Dart", ".sql": "SQL", ".sh": "Shell",
    ".ps1": "PowerShell", ".md": "Markdown", ".mdx": "MDX", ".rst": "reStructuredText",
    ".adoc": "AsciiDoc", ".ipynb": "Jupyter",
}
DOC_EXT = {".md", ".mdx", ".rst", ".adoc", ".txt"}
ENTRY_NAMES = ("main.py", "__main__.py", "main.go", "main.rs", "main.cpp", "main.c",
               "main.ts", "main.tsx", "index.ts", "index.js", "App.tsx", "app.py",
               "server.py", "cli.py", "index.md", "SUMMARY.md", "mkdocs.yml",
               "docusaurus.config.js")
MAX_TREE_LINES = 40
MARK = re.compile(r"\{\{[^{}\n]{1,120}\}\}")


# ------------------------------------------------------------ detection ---

def detect_languages(files: list[str]) -> list[str]:
    counts = Counter(LANG_BY_EXT[Path(f).suffix.lower()] for f in files
                     if Path(f).suffix.lower() in LANG_BY_EXT)
    prose = ("Markdown", "MDX", "reStructuredText", "AsciiDoc")
    ranked = [lang for lang, _ in counts.most_common()]
    return ([lang for lang in ranked if lang not in prose] or ranked)[:4]


def detect_kind(files: list[str]) -> str:
    code = sum(1 for f in files if Path(f).suffix.lower() in LANG_BY_EXT
               and Path(f).suffix.lower() not in DOC_EXT)
    docs = sum(1 for f in files if Path(f).suffix.lower() in DOC_EXT)
    if code == 0 and docs == 0:
        return "mixed"
    if docs > 3 * max(code, 1):
        return "docs"
    return "code" if code > docs else "mixed"


def detect_name(root: Path) -> str:
    pj = root / "package.json"
    if pj.is_file():
        try:
            name = json.loads(pj.read_text(encoding="utf-8")).get("name")
            if name:
                return str(name)
        except (json.JSONDecodeError, OSError):
            pass
    for manifest, rx in (("pyproject.toml", r'^name\s*=\s*"([^"]+)"'),
                         ("Cargo.toml", r'^name\s*=\s*"([^"]+)"'),
                         ("go.mod", r"^module\s+(\S+)")):
        p = root / manifest
        if p.is_file():
            m = re.search(rx, p.read_text(encoding="utf-8", errors="replace"), re.M)
            if m:
                return m.group(1).split("/")[-1]
    return root.name


def _npm_scripts(root: Path) -> dict:
    try:
        return json.loads((root / "package.json").read_text(encoding="utf-8")).get("scripts", {})
    except (json.JSONDecodeError, OSError):
        return {}


def detect_commands(root: Path) -> dict[str, str]:
    """Best-effort build/test/lint commands, keyed build|test|lint."""
    cmds: dict[str, str] = {}
    def put(k, v):
        cmds.setdefault(k, v)
    if (root / "package.json").is_file():
        runner = "pnpm" if (root / "pnpm-lock.yaml").is_file() else \
                 "yarn" if (root / "yarn.lock").is_file() else "npm run"
        scripts = _npm_scripts(root)
        for k in ("build", "test", "lint"):
            if k in scripts:
                put(k, f"{runner} {k}")
    if (root / "pyproject.toml").is_file():
        uv = (root / "uv.lock").is_file()
        put("test", "uv run pytest" if uv else "python -m pytest")
        text = (root / "pyproject.toml").read_text(encoding="utf-8", errors="replace")
        if "ruff" in text:
            put("lint", "uv run ruff check ." if uv else "ruff check .")
    if (root / "Cargo.toml").is_file():
        put("build", "cargo build"), put("test", "cargo test"), put("lint", "cargo clippy")
    if (root / "go.mod").is_file():
        put("build", "go build ./..."), put("test", "go test ./..."), put("lint", "go vet ./...")
    if (root / "CMakePresets.json").is_file():
        put("build", "cmake --preset <preset> && cmake --build build/<preset>")
        put("test", "ctest --preset <preset>")
    elif (root / "CMakeLists.txt").is_file():
        put("build", "cmake -S . -B build && cmake --build build")
        put("test", "ctest --test-dir build")
    if (root / "gradlew").is_file() or (root / "build.gradle").is_file() or (root / "build.gradle.kts").is_file():
        g = "./gradlew" if (root / "gradlew").is_file() else "gradle"
        put("build", f"{g} build"), put("test", f"{g} test")
    if (root / "pom.xml").is_file():
        m = "./mvnw" if (root / "mvnw").is_file() else "mvn"
        put("build", f"{m} package"), put("test", f"{m} test")
    if (root / "Makefile").is_file():
        put("build", "make"), put("test", "make test")
    if (root / "mkdocs.yml").is_file():
        put("build", "mkdocs build"), put("lint", "mkdocs build --strict")
    return cmds


def readme_paragraph(root: Path) -> str | None:
    for name in ("README.md", "README.rst", "README.txt", "README"):
        p = root / name
        if not p.is_file():
            continue
        para: list[str] = []
        for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
            s = line.strip()
            skip = not s or s.startswith(("#", "!", "[!", "<", "=", "-", "*", "|", "```", ">"))
            if skip and para:
                break
            if not skip:
                para.append(s)
        text = " ".join(para)
        if len(text) > 40:
            return text[:700] + ("…" if len(text) > 700 else "")
    return None


def entry_points(files: list[str]) -> list[str]:
    hits = [f for f in files if Path(f).name in ENTRY_NAMES and f.count("/") <= 3]
    return sorted(hits, key=lambda f: (f.count("/"), f))[:8]


def layout_tree(files: list[str], area_roots: list[str]) -> str:
    """Top-level entries (+ one level under area roots) with file counts."""
    top: Counter = Counter()
    sub: dict[str, Counter] = {}
    for f in files:
        parts = f.split("/")
        if len(parts) == 1:
            continue
        top[parts[0]] += 1
        if parts[0] in area_roots and len(parts) > 2:
            sub.setdefault(parts[0], Counter())[parts[1]] += 1
    lines = []
    for d in sorted(top):
        lines.append(f"{d + '/':<28} # {{{{purpose}}}} ({top[d]} files)")
        for s in sorted(sub.get(d, {})):
            lines.append(f"  {s + '/':<26} # ({sub[d][s]} files)")
        if len(lines) >= MAX_TREE_LINES:
            lines.append("  … (truncated)")
            break
    roots = [f for f in files if "/" not in f][:12]
    if roots:
        lines.append(" ".join(roots))
    return "\n".join(lines)


# ------------------------------------------------------------ rendering ---

def render_root(tpl: str, facts: dict) -> str:
    out = tpl.replace("{{PROJECT_NAME}}", facts["name"])
    if facts["readme"]:
        out = out.replace("{{ONE_PARAGRAPH_WHAT_THIS_IS}}",
                          facts["readme"] + "\n\n<!-- from README.md: verify and sharpen -->")
    out = out.replace("{{code | docs | mixed}}", facts["kind"])
    out = out.replace("{{LANGS}}", ", ".join(facts["langs"]) or "{{LANGS}}")
    if facts["kind"] == "docs":
        out = out.replace("**docs/ENGINEERING.md**: the full principles manual",
                          "**docs/STYLE.md**: the full structure & style manual")
        out = out.replace("see docs/ENGINEERING.md", "see docs/STYLE.md")
    out = out.replace(" {{or docs/STYLE.md for docs projects}}", "")
    cmds = facts["commands"]
    for key, mark in (("build", "{{BUILD_COMMAND}}"), ("test", "{{TEST_COMMAND}}"),
                      ("lint", "{{LINT_COMMAND}}")):
        out = out.replace(mark, cmds.get(key, f"# {{{{{key} command}}}}"))
    return out


def render_map(tpl: str, facts: dict) -> str:
    out = tpl.replace("{{PROJECT_NAME}}", facts["name"])
    out = out.replace("{{TREE — top two levels only, one-line comment per entry}}", facts["tree"])
    rows = [f"| {Path(a).name} | [`{a}/`]({a}/CLAUDE.md) | {{{{one line}}}} | "
            f"{{{{why it is shaped this way}}}} | stub |" for a in facts["areas"]]
    row_tpl = ("| {{name}} | [`{{path}}/`]({{path}}/CLAUDE.md) | {{one line}} | "
               "{{the design reason / constraint}} | done |")
    if rows:
        out = out.replace(row_tpl, "\n".join(rows))
    eps = facts["entry_points"]
    if eps:
        ep_lines = "\n".join(f"- `{e}`: {{{{what starts here}}}}" for e in eps)
        out = out.replace("- {{binary/CLI/site root}}: `{{path}}`", ep_lines)
    if facts["commands"].get("test"):
        out = out.replace("- Tests: `{{path}}` (`{{command}}`)",
                          f"- Tests: `{{{{path}}}}` (`{facts['commands']['test']}`)")
    return out


def unfilled(text: str) -> int:
    """Count `{{…}}` markers outside HTML comments."""
    return len(MARK.findall(re.sub(r"(?s)<!--.*?-->", "", text)))


def plan(root: Path, tdir: Path, facts: dict) -> list[tuple[str, str]]:
    """[(dest_rel, content)] for every file the template would create."""
    def read(n: str) -> str:
        return (tdir / n).read_text(encoding="utf-8")

    name = facts["name"]
    items = [("CLAUDE.md", render_root(read("CLAUDE.root.md"), facts)),
             ("AGENTS.md", read("AGENTS.md")),
             ("PROJECT_MAP.md", render_map(read("PROJECT_MAP.md"), facts)),
             ("docs/JOURNAL.md", read("JOURNAL.md").replace("{{PROJECT_NAME}}", name))]
    if facts["kind"] == "docs":
        items.append(("docs/STYLE.md", read("STYLE.docs.md").replace("{{PROJECT_NAME}}", name)))
    else:
        items.append(("docs/ENGINEERING.md", read("ENGINEERING.md").replace("{{PROJECT_NAME}}", name)))
    return items
