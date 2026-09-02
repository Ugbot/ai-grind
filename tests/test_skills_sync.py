"""Tests for the skills_sync tool helpers (root resolution, status, script runs)."""

from __future__ import annotations

from pathlib import Path

from devtools_mcp.tools.skills_sync_tools import (
    OWNED_TARGETS,
    SYNC_TARGETS,
    find_skills_root,
    library_status,
    run_script,
)

REPO_SKILLS = Path(__file__).resolve().parent.parent / "skills"


class TestFindSkillsRoot:
    def test_finds_checkout_root(self):
        root = find_skills_root()
        assert root is not None
        assert (root / "sync.py").is_file()
        assert (root / "harvest.py").is_file()

    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv("DEVTOOLS_MCP_SKILLS_ROOT", str(REPO_SKILLS))
        assert find_skills_root() == REPO_SKILLS

    def test_bad_env_override_returns_none(self, monkeypatch):
        monkeypatch.setenv("DEVTOOLS_MCP_SKILLS_ROOT", str(REPO_SKILLS / "nope"))
        assert find_skills_root() is None


class TestLibraryStatus:
    def test_status_is_bounded_and_informative(self):
        text = library_status(REPO_SKILLS)
        assert text.count("\n") < 40
        assert "Harvested" in text
        assert "Authored" in text
        assert "plugin" in text

    def test_targets_are_consistent(self):
        assert set(OWNED_TARGETS) <= set(SYNC_TARGETS)
        assert "global" not in OWNED_TARGETS  # never wiped implicitly
        assert "project" not in OWNED_TARGETS


class TestRunScript:
    async def test_sync_local_runs(self):
        # target=local rewrites skills/loadable, derived, gitignored output.
        code, tail = await run_script(REPO_SKILLS, "sync.py", ["--target", "local"])
        assert code == 0
        assert "synced ->" in tail
        assert "skills=" in tail

    async def test_bad_target_fails_cleanly(self):
        code, tail = await run_script(REPO_SKILLS, "sync.py", ["--target", "bogus"])
        assert code != 0
        assert tail  # argparse error text captured
