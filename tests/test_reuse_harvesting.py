"""Portable contract caller and authored-agent sync checks using synthetic files."""

import importlib.util
import json
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CALLER = ROOT / "skills/authored/skills/devtools/reuse-harvesting/scripts/check_artifacts.py"


def load_script(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def sync_module(monkeypatch, tmp_path):
    module = load_script(ROOT / "skills/sync.py", "reuse_test_sync")
    skills = tmp_path / "skills"
    skills.mkdir()
    manifest = skills / "MANIFEST.json"
    manifest.write_text('{"items": [{"type": "archive", "category": "_disabled"}]}', encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", skills)
    monkeypatch.setattr(module, "MANIFEST", manifest)
    monkeypatch.setattr(module, "AUTHORED", skills / "authored")
    return module


def agent(module, name="reuse-harvester", category="integration"):
    path = module.AUTHORED / "agents" / category / f"{name}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"---\nname: {name}\ndescription: Test agent\n---\nInstructions.\n", encoding="utf-8")
    return path


def test_sync_publishes_authored_agent(sync_module, monkeypatch, tmp_path):
    source = agent(sync_module)
    monkeypatch.setattr(sys, "argv", ["sync.py", "--target", "plugin"])
    assert sync_module.main() == 0
    assert (tmp_path / "plugin/agents/reuse-harvester.md").read_bytes() == source.read_bytes()


def test_skills_only_target_excludes_agents(sync_module, monkeypatch, tmp_path):
    agent(sync_module)
    monkeypatch.setattr(sys, "argv", ["sync.py", "--target", "agents"])
    assert sync_module.main() == 0
    assert not (tmp_path / ".agents/agents").exists()


@pytest.mark.parametrize("catalog_name", ["reuse-harvester", "Reuse-Harvester"])
def test_authored_agent_collision_preserves_catalog(sync_module, monkeypatch, tmp_path, catalog_name):
    agent(sync_module)
    catalog = sync_module.ROOT / f"catalog/agents/integration/{catalog_name}.md"
    catalog.parent.mkdir(parents=True)
    catalog.write_text("catalog", encoding="utf-8")
    sync_module.MANIFEST.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "type": "agent",
                        "name": catalog_name,
                        "category": "integration",
                        "dest": str(catalog.relative_to(sync_module.ROOT)),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "argv", ["sync.py", "--target", "plugin"])
    with pytest.raises(AssertionError, match="duplicate agent"):
        sync_module.main()
    assert (tmp_path / f"plugin/agents/{catalog_name}.md").read_text() == "catalog"


def test_authored_agent_case_collision_preserves_first(sync_module, tmp_path):
    first = agent(sync_module, "Reuse-Harvester", "first")
    agent(sync_module, "reuse-harvester", "second")
    base = tmp_path / "out"
    with pytest.raises(AssertionError, match="duplicate agent"):
        sync_module.sync_authored_agents(base, set())
    assert (base / "agents/Reuse-Harvester.md").read_bytes() == first.read_bytes()


def test_authored_agents_require_matching_name_and_bounded_count(sync_module, tmp_path, monkeypatch):
    source = agent(sync_module, "wrong")
    source.write_text("---\nname: other\n---\n", encoding="utf-8")
    with pytest.raises(AssertionError, match="filename/name mismatch"):
        sync_module.sync_authored_agents(tmp_path / "out", set())
    source.unlink()
    agent(sync_module, "first")
    agent(sync_module, "second")
    monkeypatch.setattr(sync_module, "MAX_AUTHORED", 1)
    with pytest.raises(AssertionError, match="out of bounds"):
        sync_module.sync_authored_agents(tmp_path / "out", set())


def test_authored_agent_sidelined_and_shared_target_preservation(sync_module, tmp_path):
    agent(sync_module, "disabled", "_disabled")
    agent(sync_module, "experimental", "experimental")
    source = agent(sync_module)
    base = tmp_path / "shared"
    (base / "agents").mkdir(parents=True)
    unrelated = base / "agents/unrelated.md"
    unrelated.write_text("owned elsewhere", encoding="utf-8")
    assert sync_module.sync_authored_agents(base, set()) == 1
    assert (base / "agents/reuse-harvester.md").read_bytes() == source.read_bytes()
    assert unrelated.read_text() == "owned elsewhere"
    assert not (base / "agents/disabled.md").exists()


def test_artifact_caller_roundtrip(tmp_path, capsys):
    caller = load_script(CALLER, "reuse_test_caller")
    assert caller.main(["--ai-grind", str(ROOT)]) == 0
    record = json.loads(capsys.readouterr().out)
    assert len(record["checks"]) >= 11
    assert record["module_sha256"] and record["platform"]


def test_artifact_caller_missing_checkout(tmp_path, capsys):
    caller = load_script(CALLER, "reuse_test_caller_missing")
    assert caller.main(["--ai-grind", str(tmp_path)]) == 1
    assert "missing" in capsys.readouterr().err


def test_artifact_caller_missing_argument():
    caller = load_script(CALLER, "reuse_test_caller_argument")
    with pytest.raises(SystemExit) as error:
        caller.main([])
    assert error.value.code == 2


def test_caller_wrong_failure_type_is_not_accepted():
    caller = load_script(CALLER, "reuse_test_caller_error")
    with pytest.raises(PermissionError):
        caller.expect_error(FileExistsError, lambda: (_ for _ in ()).throw(PermissionError("wrong failure")))
    with pytest.raises(RuntimeError, match="unexpectedly succeeded"):
        caller.expect_error(ValueError, lambda: None)


def test_caller_rejects_malformed_helper(tmp_path, capsys):
    caller = load_script(CALLER, "reuse_test_caller_malformed")
    source = tmp_path / "src/devtools_mcp/evidence/artifacts.py"
    source.parent.mkdir(parents=True)
    source.write_text("this is not python code!", encoding="utf-8")
    assert caller.main(["--ai-grind", str(tmp_path)]) == 1
    assert "Artifact contract check failed" in capsys.readouterr().err


def test_native_codex_role_and_plugin_assets_are_preserved():
    source = CALLER.parents[1]
    mirror = ROOT / "plugin/skills/reuse-harvesting"
    role = tomllib.loads((source / "agents/reuse-harvester.toml").read_text(encoding="utf-8"))
    assert role["name"] == "reuse-harvester"
    assert "Resume every pending discovery cursor" in role["developer_instructions"]
    for item in source.rglob("*"):
        if item.is_file() and "__pycache__" not in item.parts:
            assert (mirror / item.relative_to(source)).read_bytes() == item.read_bytes()
    agent_source = ROOT / "skills/authored/agents/integration/reuse-harvester.md"
    assert (ROOT / "plugin/agents/reuse-harvester.md").read_bytes() == agent_source.read_bytes()
