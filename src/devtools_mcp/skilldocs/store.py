"""CRDT-backed store for live skills.

One pycrdt Doc per skill, holding the full SKILL.md markdown in a Text named
"content". Every mutation is persisted as an incremental update blob (with
periodic compaction to one snapshot), so a doc rebuilds by replaying its log
and any two replicas converge by exchanging update diffs. After every local
change or remote merge the current text is materialized to
`<publish_root>/<name>/SKILL.md` — the real file skill loaders read.

Edits should be *surgical* (append / patch with Edit-tool find-replace
semantics), not whole-document replaces: concurrent full replaces both survive
a CRDT merge and duplicate the document. The tool layer enforces this shape.
"""

from __future__ import annotations

import os
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from pycrdt import Doc, Text

NAME_RE = re.compile(r"^[a-z][a-z0-9-]{1,63}$")
CONTENT_MAX: int = 256_000  # one skill document, characters
UPDATES_COMPACT_AT: int = 200  # log rows per skill before snapshot compaction
SKILLS_MAX: int = 500
FRONTMATTER_SCAN_LINES: int = 60  # bound mirrors skills/sync.py
CONTROL_DOC: str = "skill-control"  # reserved: control state, never a loadable skill

ENV_DB_PATH: str = "DEVTOOLS_MCP_SKILLDOCS_DB"  # dedicated env override for the skilldocs DB
BUSY_TIMEOUT_MS: int = 5000


def resolve_db_path(root: Path | None = None) -> Path:
    """Resolve the skilldocs DB path.

    An explicit ``root`` base dir wins (test isolation, mirrors open_tracker's
    explicit-path override); otherwise the per-store env override
    (DEVTOOLS_MCP_SKILLDOCS_DB) wins; otherwise it lives under the shared data
    root (honoring DEVTOOLS_MCP_DATA). Mirrors tracker/db.py:20,40-46.
    """
    if root is not None:
        path = root / "skilldocs.db"
    else:
        override = os.environ.get(ENV_DB_PATH, "").strip()
        if override:
            path = Path(override)
        else:
            from devtools_mcp.store.paths import data_root

            path = data_root() / "skilldocs.db"
    assert path.name, f"db path has no filename: {path!r}"
    assert not path.is_dir(), f"db path is a directory: {path}"
    return path


def connect(path: Path) -> sqlite3.Connection:
    """Open a skilldocs connection with the standard pragmas + migrations.

    WAL + foreign_keys=ON (with the same fk assert as tracker/db.py:61,63-64) +
    busy_timeout, then apply the versioned migrations from schema.py. Shared by
    SkillDocStore and standalone SkillControl so both get an identical,
    fully-migrated connection instead of divergent ad-hoc setups.
    """
    from devtools_mcp.skilldocs.schema import apply_migrations

    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    fk_on = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    assert fk_on == 1, "foreign_keys pragma did not take"
    apply_migrations(conn)
    return conn


class SkillDocError(Exception):
    """Expected/reportable condition (bad input, unknown skill) — not a bug."""


def _utc_now_iso() -> str:
    stamp = datetime.now(UTC).isoformat()
    assert stamp.endswith("+00:00"), f"expected UTC timestamp, got {stamp!r}"
    return stamp


def publish_root() -> Path:
    """Where live skills materialize. Default: the global Claude skills dir,
    so a synced change is live for every project's next skill load."""
    override = os.environ.get("DEVTOOLS_MCP_LIVE_SKILLS_DIR", "").strip()
    root = Path(override).expanduser() if override else Path.home() / ".claude" / "skills"
    assert root.name, f"publish root has no name: {root!r}"
    return root


def frontmatter_name(content: str) -> str | None:
    """The frontmatter `name:` if the content is a valid skill doc, else None.

    Valid means: starts with a `---` fence, has `name:` and `description:`
    before the closing fence (same contract skills/sync.py enforces).
    """
    assert isinstance(content, str), "content must be str"
    lines = content.splitlines()
    if not lines or lines[0].strip() != "---":
        return None
    name = ""
    has_description = False
    for line in lines[1 : min(len(lines), FRONTMATTER_SCAN_LINES)]:  # bounded
        if line.strip() == "---":
            return name if (name and has_description) else None
        if line.startswith("name:"):
            name = line.split(":", 1)[1].strip().strip("'\"")
        if line.startswith("description:"):
            has_description = True
    return None


class SkillDocStore:
    """SQLite-persisted collection of live skill docs. One store per data root."""

    def __init__(self, root: Path | None = None) -> None:
        self.path = resolve_db_path(root)
        self.conn = connect(self.path)
        assert self.conn is not None, "store failed to open"

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None  # type: ignore[assignment]

    # -- doc plumbing ----------------------------------------------------------

    def _load_doc(self, name: str) -> Doc:
        """Rebuild a doc by replaying its update log."""
        assert NAME_RE.match(name), f"unvalidated name reached _load_doc: {name!r}"
        rows = self.conn.execute(
            "SELECT update_blob FROM skill_updates WHERE name = ? ORDER BY id LIMIT ?",
            (name, UPDATES_COMPACT_AT + 50),
        ).fetchall()
        doc: Doc = Doc()
        doc["content"] = Text()
        for row in rows:  # bounded by compaction
            doc.apply_update(bytes(row[0]))
        return doc

    def _persist(self, name: str, update: bytes) -> None:
        """Append one update blob; compact to a snapshot when the log grows."""
        assert update, "empty update blob"
        now = _utc_now_iso()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(
                "INSERT INTO skill_docs (name, created_at, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET updated_at = excluded.updated_at",
                (name, now, now),
            )
            self.conn.execute(
                "INSERT INTO skill_updates (name, update_blob, ts) VALUES (?, ?, ?)",
                (name, update, now),
            )
            count = self.conn.execute("SELECT COUNT(*) FROM skill_updates WHERE name = ?", (name,)).fetchone()[0]
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        if count > UPDATES_COMPACT_AT:
            self._compact(name)

    def _compact(self, name: str) -> None:
        """Replace a skill's update log with one full snapshot update."""
        doc = self._load_doc(name)
        snapshot = doc.get_update()
        assert snapshot, "compaction produced empty snapshot"
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute("DELETE FROM skill_updates WHERE name = ?", (name,))
            self.conn.execute(
                "INSERT INTO skill_updates (name, update_blob, ts) VALUES (?, ?, ?)",
                (name, snapshot, _utc_now_iso()),
            )
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    @staticmethod
    def _validate_name(name: str) -> str:
        name = (name or "").strip().lower()
        if not NAME_RE.match(name):
            raise SkillDocError(f"Bad skill name {name!r}: kebab-case, 2-64 chars, [a-z0-9-]")
        return name

    def _require(self, name: str) -> str:
        name = self._validate_name(name)
        row = self.conn.execute("SELECT name FROM skill_docs WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise SkillDocError(f"No live skill {name!r} — create it or sync from a peer")
        return name

    # -- queries ---------------------------------------------------------------

    def exists(self, name: str) -> bool:
        name = self._validate_name(name)
        return self.conn.execute("SELECT 1 FROM skill_docs WHERE name = ?", (name,)).fetchone() is not None

    def list_skills(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT name, created_at, updated_at FROM skill_docs ORDER BY name LIMIT ?",
            (SKILLS_MAX,),
        ).fetchall()
        out = [dict(row) for row in rows]
        assert len(out) <= SKILLS_MAX, "skill list exceeded bound"
        return out

    def get_text(self, name: str) -> str:
        name = self._require(name)
        doc = self._load_doc(name)
        text = str(doc["content"])
        assert len(text) <= CONTENT_MAX, f"stored content over bound: {len(text)}"
        return text

    def state(self, name: str) -> bytes:
        """State vector — what this replica has seen for the skill."""
        name = self._require(name)
        return self._load_doc(name).get_state()

    def diff(self, name: str, since_state: bytes | None) -> bytes:
        """Update containing everything the given state vector is missing."""
        name = self._require(name)
        doc = self._load_doc(name)
        return doc.get_update(since_state) if since_state else doc.get_update()

    # -- mutations -------------------------------------------------------------

    def create(self, name: str, content: str) -> Path | None:
        """Create a live skill from full initial content (frontmatter required)."""
        name = self._validate_name(name)
        if self.exists(name):
            raise SkillDocError(f"Live skill {name!r} already exists — use append/patch")
        if len(content) > CONTENT_MAX:
            raise SkillDocError(f"content too large: {len(content)} > {CONTENT_MAX}")
        fm = frontmatter_name(content)
        if fm != name:
            raise SkillDocError(f"content frontmatter must declare name: {name} (and a description:); got {fm!r}")
        doc: Doc = Doc()
        doc["content"] = text = Text()
        before = doc.get_state()
        text += content
        self._persist(name, doc.get_update(before))
        return self.materialize(name)

    def append(self, name: str, suffix: str) -> Path | None:
        """Append text to the end of the skill document."""
        name = self._require(name)
        if not suffix:
            raise SkillDocError("append needs non-empty text")
        doc = self._load_doc(name)
        text = doc["content"]
        if len(text) + len(suffix) > CONTENT_MAX:
            raise SkillDocError(f"append would exceed {CONTENT_MAX} chars")
        before = doc.get_state()
        text += suffix
        self._persist(name, doc.get_update(before))
        return self.materialize(name)

    def patch(self, name: str, old: str, new: str) -> Path | None:
        """Find/replace with Edit-tool semantics: `old` must match exactly once.

        Deletes just the matched span and inserts the replacement, keeping the
        CRDT operation minimal so concurrent edits elsewhere merge cleanly.
        """
        name = self._require(name)
        if not old:
            raise SkillDocError("patch needs a non-empty old string")
        doc = self._load_doc(name)
        text = doc["content"]
        current = str(text)
        count = current.count(old)
        if count == 0:
            raise SkillDocError("old string not found in the skill document")
        if count > 1:
            raise SkillDocError(f"old string matches {count} times — add context to make it unique")
        if len(current) - len(old) + len(new) > CONTENT_MAX:
            raise SkillDocError(f"patch would exceed {CONTENT_MAX} chars")
        # pycrdt Text is indexed by UTF-8 byte offset, not Python code points, so
        # translate — otherwise any non-ASCII before the match corrupts the doc.
        char_start = current.index(old)
        byte_start = len(current[:char_start].encode("utf-8"))
        byte_len = len(old.encode("utf-8"))
        before = doc.get_state()
        del text[byte_start : byte_start + byte_len]
        if new:
            text.insert(byte_start, new)
        assert str(text) == current[:char_start] + new + current[char_start + len(old) :], "patch produced wrong text"
        self._persist(name, doc.get_update(before))
        return self.materialize(name)

    def apply(self, name: str, update: bytes) -> Path | None:
        """Merge a remote update (creating the skill locally if unknown)."""
        name = self._validate_name(name)
        if not update:
            raise SkillDocError("empty update")
        doc: Doc
        if self.exists(name):
            doc = self._load_doc(name)
        else:
            doc = Doc()
            doc["content"] = Text()
        doc.apply_update(update)
        if len(str(doc["content"])) > CONTENT_MAX:
            raise SkillDocError("merged content exceeds size bound")
        self._persist(name, update)
        return self.materialize(name)

    def delete(self, name: str) -> bool:
        """Remove a live skill locally: its update log and its materialized file.

        Local-only — peers that already synced the doc keep their copy (and a
        later sync from such a peer recreates it here; true tombstone deletion
        is the team collab server's job).
        """
        name = self._require(name)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute("DELETE FROM skill_updates WHERE name = ?", (name,))
            removed = self.conn.execute("DELETE FROM skill_docs WHERE name = ?", (name,)).rowcount
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        target = publish_root() / name / "SKILL.md"
        if target.is_file():
            target.unlink()
            with_dir = target.parent
            if not any(with_dir.iterdir()):
                with_dir.rmdir()
        assert removed in (0, 1), f"deleted {removed} skill rows"
        return removed == 1

    # -- materialization -------------------------------------------------------

    def _apply_control(self, name: str, content: str) -> tuple[str, bool]:
        """Render the active power variant and report whether the skill is
        disabled. Passthrough (zero cost) when the doc has no variant markers."""
        from devtools_mcp.skilldocs.control import SkillControl
        from devtools_mcp.skilldocs.variants import has_variants, render

        control = SkillControl(conn=self.conn)  # shares this store's connection
        if control.is_disabled(name):
            return content, True
        if has_variants(content):
            content = render(content, control.effective_mode(name))
        return content, False

    def materialize(self, name: str) -> Path | None:
        """Write the current merged text to `<publish_root>/<name>/SKILL.md`.

        Skipped (returns None) when the document isn't a valid skill yet — e.g. a
        partially-synced doc whose frontmatter doesn't parse — when it's the
        reserved control doc, or when the skill is disabled (existing file is then
        removed). Power variants are rendered to the active mode. The DB copy is
        always authoritative; the file is a projection.
        """
        name = self._require(name)
        target = publish_root() / name / "SKILL.md"
        if name == CONTROL_DOC:
            return None
        content = self.get_text(name)
        if frontmatter_name(content) != name:
            return None
        content, disabled = self._apply_control(name, content)
        if disabled:
            self._remove_file(target)
            return None
        if frontmatter_name(content) != name:  # variant rendering must not break frontmatter
            return None
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8", newline="\n")
        assert target.is_file(), f"materialize failed: {target}"
        return target

    @staticmethod
    def _remove_file(target: Path) -> None:
        """Delete a materialized SKILL.md and its now-empty parent dir."""
        if target.is_file():
            target.unlink()
            parent = target.parent
            if parent.is_dir() and not any(parent.iterdir()):
                parent.rmdir()

    def materialize_all(self) -> int:
        """Re-project every live skill to disk. Returns count written."""
        written = 0
        for entry in self.list_skills():  # bounded by SKILLS_MAX
            if self.materialize(entry["name"]) is not None:
                written += 1
        assert written <= SKILLS_MAX, "materialize count exceeded bound"
        return written
