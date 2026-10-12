"""Tests for skills_discovery: frontmatter reading, scanning forms, dedup, adopt.

Uses a synthetic library root + fake project tree in tmp_path, the real
library is only touched by the one integration test at the end.
"""

from __future__ import annotations

from pathlib import Path

from devtools_mcp.skills_discovery import (
    Candidate,
    adopt,
    discover,
    format_candidates,
    library_names,
    read_frontmatter_name,
    scan_roots,
    source_paths,
)

REPO_SKILLS = Path(__file__).resolve().parent.parent / "skills"


def _skill_md(name: str, body: str = "content") -> str:
    return f"---\nname: {name}\ndescription: test skill\n---\n\n# {name}\n{body}\n"


def _make_library(tmp_path: Path, code_root: Path) -> Path:
    """Synthetic skills root: sources.toml pointing into code_root, no manifest."""
    root = tmp_path / "library" / "skills"
    (root / "authored" / "skills" / "cat" / "existing-authored").mkdir(parents=True)
    (root / "authored" / "skills" / "cat" / "existing-authored" / "SKILL.md").write_text(
        _skill_md("existing-authored"), encoding="utf-8"
    )
    known_project = code_root / "known-project"
    known_skill = known_project / ".claude" / "skills" / "known-skill"
    known_skill.mkdir(parents=True)
    (known_skill / "SKILL.md").write_text(_skill_md("known-skill"), encoding="utf-8")
    src_value = str(known_skill).replace("\\", "/")
    (root / "sources.toml").write_text(
        f'[[item]]\nsrc = "{src_value}"\ntype = "skill"\ncategory = "cat"\n',
        encoding="utf-8",
    )
    (root / "sync.py").write_text("# placeholder\n", encoding="utf-8")
    (root / "harvest.py").write_text("# placeholder\n", encoding="utf-8")
    return root


class TestFrontmatter:
    def test_reads_name(self, tmp_path):
        md = tmp_path / "SKILL.md"
        md.write_text(_skill_md("my-skill"), encoding="utf-8")
        assert read_frontmatter_name(md) == "my-skill"

    def test_no_frontmatter_returns_none(self, tmp_path):
        md = tmp_path / "SKILL.md"
        md.write_text("# just markdown\n", encoding="utf-8")
        assert read_frontmatter_name(md) is None

    def test_missing_file_returns_none(self, tmp_path):
        assert read_frontmatter_name(tmp_path / "nope.md") is None


class TestScanAndDiscover:
    def test_discovers_all_forms_and_dedups(self, tmp_path, monkeypatch):
        code = tmp_path / "code"
        root = _make_library(tmp_path, code)
        monkeypatch.setenv("DEVTOOLS_MCP_SKILL_SCAN_ROOTS", "")
        # New project with: folder skill, single-file skill, malformed folder,
        # a command, and a duplicate of a library name (must be skipped).
        proj = code / "new-project" / ".claude"
        folder = proj / "skills" / "fresh-skill"
        folder.mkdir(parents=True)
        (folder / "SKILL.md").write_text(_skill_md("fresh-skill"), encoding="utf-8")
        (proj / "skills" / "loose-skill.md").write_text(_skill_md("loose-skill"), encoding="utf-8")
        broken = proj / "skills" / "broken-skill"
        broken.mkdir()
        dup = proj / "skills" / "existing-authored"
        dup.mkdir()
        (dup / "SKILL.md").write_text(_skill_md("existing-authored"), encoding="utf-8")
        commands = proj / "commands"
        commands.mkdir()
        (commands / "run-thing.md").write_text("# run thing\n", encoding="utf-8")

        assert code in scan_roots(root)
        found = discover(root)
        by_name = {c.name: c for c in found}
        assert by_name["fresh-skill"].form == "folder"
        assert by_name["loose-skill"].form == "file"
        assert by_name["run-thing"].kind == "command"
        assert "existing-authored" not in by_name  # library name dedup
        assert "known-skill" not in by_name  # already in sources.toml
        assert by_name["broken-skill"].issue == "no SKILL.md in folder"

    def test_name_mismatch_flagged(self, tmp_path, monkeypatch):
        code = tmp_path / "code"
        root = _make_library(tmp_path, code)
        monkeypatch.setenv("DEVTOOLS_MCP_SKILL_SCAN_ROOTS", "")
        folder = code / "p2" / ".claude" / "skills" / "folder-name"
        folder.mkdir(parents=True)
        (folder / "SKILL.md").write_text(_skill_md("other-name"), encoding="utf-8")
        found = discover(root)
        flagged = [c for c in found if "!=" in c.issue]
        assert flagged and flagged[0].name == "other-name"

    def test_format_is_bounded_with_adopt_hint(self, tmp_path):
        cands = [Candidate("skill", "folder", f"s{i}", f"c:/x/s{i}") for i in range(60)]
        text = format_candidates(cands, tmp_path)
        assert text.count("\n") < 60
        assert 'action="adopt"' in text

    def test_format_empty(self, tmp_path):
        assert "No unharvested" in format_candidates([], tmp_path)


class TestAdopt:
    def test_adopt_appends_item(self, tmp_path, monkeypatch):
        code = tmp_path / "code"
        root = _make_library(tmp_path, code)
        folder = code / "p3" / ".claude" / "skills" / "adopt-me"
        folder.mkdir(parents=True)
        (folder / "SKILL.md").write_text(_skill_md("adopt-me"), encoding="utf-8")
        error = adopt(root, str(folder), "profiling", note='has "quotes"')
        assert error is None
        srcs = source_paths(root)
        assert any("adopt-me" in s for s in srcs)
        text = (root / "sources.toml").read_text(encoding="utf-8")
        assert 'type = "skill"' in text
        assert '"quotes"' not in text  # escaped to single quotes

    def test_adopt_rejects_duplicate_name(self, tmp_path):
        code = tmp_path / "code"
        root = _make_library(tmp_path, code)
        dup = code / "p4" / ".claude" / "skills" / "existing-authored"
        dup.mkdir(parents=True)
        (dup / "SKILL.md").write_text(_skill_md("existing-authored"), encoding="utf-8")
        error = adopt(root, str(dup), "cat")
        assert error is not None and "already in the library" in error

    def test_adopt_rejects_missing_and_malformed(self, tmp_path):
        code = tmp_path / "code"
        root = _make_library(tmp_path, code)
        assert "not found" in adopt(root, str(code / "nope"), "cat")
        broken = code / "p5" / ".claude" / "skills" / "no-front"
        broken.mkdir(parents=True)
        (broken / "SKILL.md").write_text("# no frontmatter\n", encoding="utf-8")
        assert "no frontmatter" in adopt(root, str(broken), "cat")
        assert "needs category" in adopt(root, str(broken), " ")

    def test_adopt_infers_command_kind(self, tmp_path):
        code = tmp_path / "code"
        root = _make_library(tmp_path, code)
        cmd = code / "p6" / ".claude" / "commands" / "do-it.md"
        cmd.parent.mkdir(parents=True)
        cmd.write_text("# do it\n", encoding="utf-8")
        assert adopt(root, str(cmd), "build") is None
        assert 'type = "command"' in (root / "sources.toml").read_text(encoding="utf-8")


class TestRealLibrary:
    def test_discover_runs_against_real_library(self):
        """Integration smoke: real sources.toml derives roots; no crash, bounded."""
        found = discover(REPO_SKILLS)
        assert len(found) <= 100
        names = library_names(REPO_SKILLS)
        assert "renderdoc-frame-analysis" in names
        assert all(c.name not in names for c in found if not c.issue)
