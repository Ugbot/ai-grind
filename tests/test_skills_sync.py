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


# ---------------------------------------------------------------- platform / prune

import importlib.util  # noqa: E402
import json  # noqa: E402
import sys  # noqa: E402

import pytest  # noqa: E402


def _skill(root: Path, rel: str, name: str) -> None:
    folder = root / rel / name
    folder.mkdir(parents=True)
    (folder / "SKILL.md").write_text(f"---\nname: {name}\ndescription: t\n---\nbody\n", encoding="utf-8")


@pytest.fixture
def sync_mod(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(REPO_SKILLS))  # selected_sync imports additive
    spec = importlib.util.spec_from_file_location("sync_platform_test", REPO_SKILLS / "sync.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["sync_platform_test"] = module
    spec.loader.exec_module(module)
    root = tmp_path / "skills"
    (root / "authored" / "skills").mkdir(parents=True)
    (root / "MANIFEST.json").write_text('{"items": [{"type": "archive", "category": "_disabled"}]}', encoding="utf-8")
    (root / "sources.toml").write_text(
        '[[item]]\nsrc = "C:/x/retired-skill"\ntype = "skill"\ncategory = "_disabled"\n'
        '[[item]]\nsrc = "C:/x/old-cmd.md"\ntype = "command"\ncategory = "_disabled"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", root)
    monkeypatch.setattr(module, "MANIFEST", root / "MANIFEST.json")
    monkeypatch.setattr(module, "AUTHORED", root / "authored")
    _skill(root / "authored" / "skills", "tools", "portable")
    _skill(root / "authored" / "skills", "windows/powershell", "pwsh-x")
    monkeypatch.setattr(module.Path, "home", classmethod(lambda cls: tmp_path / "home"))
    return module


def _run(mod, monkeypatch, *argv):
    monkeypatch.setattr(sys, "argv", ["sync.py", *argv])
    return mod.main()


class TestWindowsSegment:
    def test_segment_rule(self, sync_mod):
        assert sync_mod.windows_only("windows/powershell")
        assert sync_mod.windows_only("a/windows")
        assert not sync_mod.windows_only("tools")
        assert not sync_mod.windows_only("windowsish/x")

    def test_cross_platform_targets_exclude_windows(self, sync_mod, monkeypatch, tmp_path):
        for target in ("plugin", "local", "agents", "agents-global"):
            base = tmp_path / target
            monkeypatch.setattr(sync_mod, "target_base", lambda _n, b=base: (b, True))
            assert _run(sync_mod, monkeypatch, "--target", target) == 0
            names = {p.name for p in (base / "skills").iterdir()}
            assert names == {"portable"}, target

    def test_windows_target_includes_only_windows(self, sync_mod, monkeypatch, tmp_path):
        base = tmp_path / "plugin-windows"
        monkeypatch.setattr(sync_mod, "target_base", lambda _n: (base, True))
        assert _run(sync_mod, monkeypatch, "--target", "plugin-windows") == 0
        assert {p.name for p in (base / "skills").iterdir()} == {"pwsh-x"}

    def test_plugin_wipe_keeps_manifest(self, sync_mod, monkeypatch, tmp_path):
        base = tmp_path / "plugin"
        (base / ".claude-plugin").mkdir(parents=True)
        (base / ".claude-plugin" / "plugin.json").write_text("{}", encoding="utf-8")
        _skill(base, "skills", "stale")
        monkeypatch.setattr(sync_mod, "target_base", lambda _n: (base, True))
        assert _run(sync_mod, monkeypatch, "--target", "plugin") == 0
        assert (base / ".claude-plugin" / "plugin.json").is_file()
        assert not (base / "skills" / "stale").exists()

    def test_selected_sync_rejects_wrong_platform(self, sync_mod):
        with pytest.raises(ValueError):
            sync_mod.selected_sync("plugin", ["pwsh-x"])
        with pytest.raises(ValueError):
            sync_mod.selected_sync("plugin-windows", ["portable"])


class TestAgentsGlobal:
    def test_base_is_home_agents_and_owned(self, sync_mod, tmp_path):
        base, owned = sync_mod.target_base("agents-global")
        assert base == tmp_path / "home" / ".agents"
        assert owned

    def test_wipes_skills_only_and_mirrors(self, sync_mod, monkeypatch, tmp_path):
        base = tmp_path / "home" / ".agents"
        _skill(base, "skills", "retired-skill")
        (base / "other.txt").parent.mkdir(parents=True, exist_ok=True)
        (base / "other.txt").write_text("keep", encoding="utf-8")
        assert _run(sync_mod, monkeypatch, "--target", "agents-global") == 0
        assert {p.name for p in (base / "skills").iterdir()} == {"portable"}  # retired skill gone
        assert (base / "other.txt").read_text(encoding="utf-8") == "keep"
        assert not (base / "commands").exists()


class TestPrune:
    def _home(self, tmp_path: Path) -> Path:
        home = tmp_path / "home" / ".claude"
        _skill(home, "skills", "retired-skill")  # listed in sources.toml (_disabled)
        _skill(home, "skills", "pwsh-x")  # authored, windows-only: not published globally
        _skill(home, "skills", "users-own-skill")  # unrelated
        (home / "commands").mkdir()
        (home / "commands" / "old-cmd.md").write_text("x", encoding="utf-8")
        (home / "commands" / "mine.md").write_text("x", encoding="utf-8")
        (home / "agents").mkdir()
        (home / "agents" / "mine-agent.md").write_text("x", encoding="utf-8")
        return home

    def test_global_without_prune_keeps_everything(self, sync_mod, monkeypatch, tmp_path):
        home = self._home(tmp_path)
        assert _run(sync_mod, monkeypatch, "--target", "global") == 0
        assert (home / "skills" / "retired-skill").is_dir()
        assert (home / "skills" / "portable" / "SKILL.md").is_file()

    def test_prune_removes_only_library_listed_names(self, sync_mod, monkeypatch, tmp_path):
        home = self._home(tmp_path)
        assert _run(sync_mod, monkeypatch, "--target", "global", "--prune") == 0
        assert not (home / "skills" / "retired-skill").exists()
        assert not (home / "skills" / "pwsh-x").exists()
        assert not (home / "commands" / "old-cmd.md").exists()
        assert (home / "skills" / "users-own-skill").is_dir()  # unrelated survives
        assert (home / "commands" / "mine.md").is_file()
        assert (home / "agents" / "mine-agent.md").is_file()
        assert (home / "skills" / "portable").is_dir()  # still published

    def test_prune_never_removes_published_names(self, sync_mod, tmp_path):
        home = tmp_path / "h"
        _skill(home, "skills", "portable")
        removed = sync_mod.prune_global(
            home, {"skills": {"portable"}, "commands": set(), "agents": set()}, {"portable"}
        )
        assert removed == []

    def test_prune_flag_rejected_off_global(self, sync_mod, monkeypatch):
        with pytest.raises(SystemExit):
            _run(sync_mod, monkeypatch, "--target", "plugin", "--prune")

    def test_ever_listed_includes_sources_and_authored(self, sync_mod):
        names = sync_mod.ever_listed_names()
        assert {"retired-skill", "old-cmd", "portable", "pwsh-x"} <= names
        assert "users-own-skill" not in names


def test_manifest_json_survives_real_bundle_layout():
    for rel, name in (("plugin", "ai-grind-skills"), ("plugin-windows", "ai-grind-windows")):
        manifest = REPO_SKILLS.parent / rel / ".claude-plugin" / "plugin.json"
        data = json.loads(manifest.read_text(encoding="utf-8"))
        assert data["name"] == name
        assert "mcpServers" not in data
