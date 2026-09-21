#!/usr/bin/env python3
"""Flag AI writing tells in the prose this repo owns.

Encodes the `unslop` skill's rules as a check, so the pass is repeatable instead
of a one-time sweep. Prose only: fenced code blocks, indented code, and Python
code lines are skipped, because a shell transcript full of em dashes is data, not
writing.

Scope: markdown and Python tracked by git, minus vendored and generated trees
(`skills/catalog/`, `skills/loadable/`, `plugin/`, `.agents/`). Those are copies;
fix their upstream instead.

    python scripts/unslop_check.py            # report, exit 1 if anything found
    python scripts/unslop_check.py --summary  # counts per rule, no line detail
    python scripts/unslop_check.py README.md  # only these paths
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKIP_PREFIXES = ("skills/catalog/", "skills/loadable/", "plugin/", ".agents/", ".venv/")
MAX_FILES = 2000  # bound: the tracked prose set is a few hundred files

# (rule, pattern, hint). Patterns run against prose lines only.
RULES: list[tuple[str, re.Pattern[str], str]] = [
    ("em-dash", re.compile(r"[—–]"), "end the sentence or use a comma"),
    ("dash-substitute", re.compile(r"(?<=\w) -- (?=\w)"), "same fix as an em dash"),
    ("curly-quote", re.compile(r"[“”‘’]"), "use straight quotes"),
    (
        "ai-vocab",
        re.compile(
            r"\b(additionally|crucial|delve|enduring|enhances?|enhanced|enhancing|"
            r"fostering|garner\w*|interplay|intricate|pivotal|showcas\w+|tapestry|"
            r"testament|underscor\w+|vibrant|utiliz(?:e|es|ed|ing)|leverag\w+|facilitat\w+|"
            r"numerous|seamless\w*|holistic|paradigm|substrate)\b",
            re.I,
        ),
        "use the plain word",
    ),
    ("fancy-is", re.compile(r"\b(serves as|stands as|boasts)\b", re.I), 'say "is" or "has"'),
    ("not-just", re.compile(r"\bnot (just|only)\b[^.]{0,60}?\bbut\b", re.I), "state the point directly"),
    (
        "filler",
        re.compile(
            r"\b(in order to|due to the fact that|it is important to note|"
            r"it'?s worth noting|needless to say|at the end of the day)\b",
            re.I,
        ),
        "delete it or shorten",
    ),
    ("hedge-stack", re.compile(r"\b(could potentially|may possibly|might perhaps|can potentially)\b", re.I), "pick one hedge"),
    (
        "chatbot",
        re.compile(r"(I hope this helps|Let me know if|Feel free to reach out|Happy coding)", re.I),
        "cut it",
    ),
    ("emoji", re.compile(r"[\U0001F300-\U0001FAFF✅❌⚠⭐✨]"), "remove decoration"),
    (
        "inline-header",
        re.compile(r"^\s*[-*]\s+\*\*[^*]+:\*\*"),
        "bold label then colon: write it as prose, or end the label with a period",
    ),
]

_FENCE = re.compile(r"^\s*(```|~~~)")
_MD_TABLE = re.compile(r"^\s*\|")
_INLINE_CODE = re.compile(r"`[^`]*`")
_URL = re.compile(r"<?https?://\S+>?")
_PY_DOC = re.compile(r'^\s*(r?"""|r?\'\'\')')


def prose_lines(path: Path) -> list[tuple[int, str]]:
    """(line_no, text) for lines that are prose, with code spans blanked out."""
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []
    out: list[tuple[int, str]] = []
    in_fence = in_doc = False
    is_py = path.suffix == ".py"
    for n, line in enumerate(raw.splitlines(), 1):
        if _FENCE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        if is_py:
            # Python: only comments and docstring bodies are prose.
            stripped = line.strip()
            if _PY_DOC.match(line):
                in_doc = not in_doc or stripped.count('"""') == 2 or stripped.count("'''") == 2
                if stripped.count('"""') == 2 or stripped.count("'''") == 2:
                    in_doc = False
                out.append((n, line))
                continue
            if not in_doc and not stripped.startswith("#"):
                continue
        else:
            if line.startswith(("    ", "\t")) or _MD_TABLE.match(line):
                # indented block or table row: usually a command or data
                if line.startswith(("    ", "\t")):
                    continue
        text = _URL.sub(" ", _INLINE_CODE.sub(" ", line))
        out.append((n, text))
    return out


def tracked_files(paths: list[str]) -> list[Path]:
    if paths:
        return [Path(p) for p in paths]
    listing = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "*.md", "*.py"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    assert len(listing) <= MAX_FILES, f"tracked file count out of bounds: {len(listing)}"
    return [ROOT / p for p in listing if not p.startswith(SKIP_PREFIXES)]


def scan(files: list[Path]) -> tuple[list[str], dict[str, int]]:
    findings: list[str] = []
    per_rule: dict[str, int] = {}
    for f in files:
        for n, text in prose_lines(f):
            for rule, pattern, hint in RULES:
                for m in pattern.finditer(text):
                    rel = f.relative_to(ROOT) if f.is_absolute() else f
                    findings.append(f"{rel}:{n}: {rule}: {m.group(0)!r} — {hint}".replace(" — ", ": "))
                    per_rule[rule] = per_rule.get(rule, 0) + 1
    return findings, per_rule


def main() -> int:
    ap = argparse.ArgumentParser(description="Flag AI writing tells in this repo's prose.")
    ap.add_argument("paths", nargs="*", help="limit to these files (default: all tracked prose)")
    ap.add_argument("--summary", action="store_true", help="counts per rule only")
    args = ap.parse_args()

    files = tracked_files(args.paths)
    findings, per_rule = scan(files)
    if args.summary:
        for rule, count in sorted(per_rule.items(), key=lambda kv: -kv[1]):
            print(f"{count:6}  {rule}")
        print(f"{sum(per_rule.values()):6}  TOTAL across {len(files)} files")
    else:
        for line in findings:
            print(line)
        print(f"\n{len(findings)} finding(s) in {len(files)} file(s)")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
