"""Dynamic-skill variant rendering: low/high power modes inside one SKILL.md.

A skill body may carry power-gated blocks. Only the block matching the active
mode survives materialization; the rest are stripped. Text outside any block is
always kept, so a skill with no markers renders byte-identical (pure passthrough).

    <!-- power:high -->
    Full discipline: written plan, adversarial verify, subagents.
    <!-- /power -->
    <!-- power:low -->
    Fast path: minimal steps.
    <!-- /power -->

Pure functions, no I/O. The CRDT doc holds every variant (source of truth, fully
synced); rendering is applied only when projecting the file the loader reads.
"""

from __future__ import annotations

import re

DEFAULT_MODE: str = "high"
KNOWN_MODES: tuple[str, ...] = ("low", "high")
VARIANT_BLOCKS_MAX: int = 64  # power blocks per skill document

# Markers live on their own line; label is kebab-case like a mode name.
_OPEN_RE = re.compile(r"^\s*<!--\s*power:([a-z][a-z0-9-]{0,31})\s*-->\s*$")
_CLOSE_RE = re.compile(r"^\s*<!--\s*/power\s*-->\s*$")


def has_variants(content: str) -> bool:
    """True if the document contains at least one power block (open marker)."""
    assert isinstance(content, str), "content must be str"
    return any(_OPEN_RE.match(line) for line in content.splitlines())


def detect_modes(content: str) -> list[str]:
    """Sorted power labels present in the document (for index annotation)."""
    assert isinstance(content, str), "content must be str"
    labels: set[str] = set()
    for line in content.splitlines():  # bounded by document size
        match = _OPEN_RE.match(line)
        if match:
            labels.add(match.group(1))
            assert len(labels) <= VARIANT_BLOCKS_MAX, "implausible variant-label count"
    return sorted(labels)


def render(content: str, mode: str = DEFAULT_MODE) -> str:
    """Strip power blocks that don't match `mode`; keep everything else.

    Line-oriented so frontmatter (which never contains markers) and shared prose
    pass through untouched. Nested or unbalanced markers are programmer errors.
    """
    assert isinstance(content, str), "content must be str"
    assert isinstance(mode, str) and mode, "mode must be a non-empty str"
    out: list[str] = []
    active: str | None = None  # label of the block currently open, else None
    blocks = 0
    for line in content.splitlines(keepends=True):
        open_match = _OPEN_RE.match(line)
        if open_match:
            assert active is None, "nested power blocks are not allowed"
            active = open_match.group(1)
            blocks += 1
            assert blocks <= VARIANT_BLOCKS_MAX, f"too many power blocks: {blocks}"
            continue  # drop the marker line itself
        if _CLOSE_RE.match(line):
            assert active is not None, "closing power marker without an open block"
            active = None
            continue  # drop the marker line itself
        if active is None or active == mode:
            out.append(line)
    assert active is None, "unclosed power block at end of document"
    rendered = "".join(out)
    assert len(rendered) <= len(content), "render must not grow the document"
    return rendered
