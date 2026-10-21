"""Selected harvest/sync contracts use synthetic files, never real mirrors."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load_script(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def library(monkeypatch, tmp_path):
    additive = load_script(ROOT / "skills/additive.py", "additive")
    monkeypatch.setitem(sys.modules, "additive", additive)
    harvest = load_script(ROOT / "skills/harvest.py", "selected_harvest_test")
    sync = load_script(ROOT / "skills/sync.py", "selected_sync_test")
    skills = tmp_path / "repo with spaces/skills"
    skills.mkdir(parents=True)
    for module in (harvest, sync):
        monkeypatch.setattr(module, "ROOT", skills)
        monkeypatch.setattr(module, "MANIFEST", skills / "MANIFEST.json")
    monkeypatch.setattr(harvest, "CATALOG", skills / "catalog")
    monkeypatch.setattr(harvest, "SOURCES", skills / "sources.toml")
    monkeypatch.setattr(sync, "AUTHORED", skills / "authored")
    legacy = {
        "name": "legacy",
        "type": "skill",
        "category": "existing",
        "origin": "C:/unavailable/legacy",
        "dest": "catalog/skills/existing/legacy/SKILL.md",
        "sha256": "0" * 64,
    }
    manifest = {
        "counts": {"skill": 1, "agent": 0, "command": 0, "archive": 0},
        "items": [legacy],
        "preserve_extension": {"owner": "other-agent"},
    }
    harvest.MANIFEST.write_text(json.dumps(manifest), encoding="utf-8")
    sentinel = skills / "catalog/other-agent/keep.txt"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_bytes(b"unrelated catalog evidence")
    return additive, harvest, sync


def source_skill(harvest, name, extra=True):
    path = harvest.ROOT.parent / "official source" / name
    path.mkdir(parents=True)
    (path / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: Fixture\n---\nOfficial fixture.\n", encoding="utf-8"
    )
    if extra:
        (path / "references").mkdir()
        (path / "references/contract.md").write_bytes(b"bundled reference")
    return path


def register(harvest, *paths):
    text = '[[item]]\nsrc="C:/unavailable/legacy"\ntype="skill"\ncategory="existing"\n'
    for path in paths:
        text += f'\n[[item]]\nsrc={json.dumps(str(path))}\ntype="skill"\ncategory="cloudflare"\n'
    harvest.SOURCES.write_text(text, encoding="utf-8")


def test_source_expanduser_is_per_user_and_does_not_read_credentials(library):
    _, harvest, _ = library
    assert harvest.source_path("~/.agents/skills/cloudflare") == Path.home() / ".agents/skills/cloudflare"
    assert harvest.source_path("relative/skill") == Path("relative/skill")


def test_selected_harvest_preserves_unavailable_legacy_and_is_idempotent(library):
    additive, harvest, _ = library
    source = source_skill(harvest, "cloudflare")
    register(harvest, source)
    previous = json.loads(harvest.MANIFEST.read_bytes())
    assert harvest.selected_harvest(["cloudflare"]) == 0
    result = json.loads(harvest.MANIFEST.read_bytes())
    assert result["items"][0] == previous["items"][0]
    assert result["preserve_extension"] == previous["preserve_extension"]
    assert result["counts"]["skill"] == 2
    target = harvest.CATALOG / "skills/cloudflare/cloudflare"
    assert additive.inventory(source) == additive.inventory(target)
    assert (harvest.CATALOG / "other-agent/keep.txt").read_bytes() == b"unrelated catalog evidence"
    manifest_bytes = harvest.MANIFEST.read_bytes()
    assert harvest.selected_harvest(["cloudflare"]) == 0
    assert harvest.MANIFEST.read_bytes() == manifest_bytes


def test_conflict_preflights_all_selected_before_mutations(library):
    _, harvest, _ = library
    first, second = source_skill(harvest, "first"), source_skill(harvest, "second")
    register(harvest, first, second)
    conflict = harvest.CATALOG / "skills/cloudflare/second"
    conflict.mkdir(parents=True)
    (conflict / "SKILL.md").write_bytes(b"another agent owns this")
    original = harvest.MANIFEST.read_bytes()
    with pytest.raises(ValueError, match="conflicts"):
        harvest.selected_harvest(["first", "second"])
    assert not (harvest.CATALOG / "skills/cloudflare/first").exists()
    assert (conflict / "SKILL.md").read_bytes() == b"another agent owns this"
    assert harvest.MANIFEST.read_bytes() == original


def test_partial_publication_resumes_without_metadata_loss(library, monkeypatch):
    additive, harvest, _ = library
    source = source_skill(harvest, "cloudflare")
    register(harvest, source)
    previous = harvest.MANIFEST.read_bytes()
    original_publish = additive.publish_file
    calls = 0

    def fail_second(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("synthetic publication failure")
        return original_publish(*args)

    monkeypatch.setattr(additive, "publish_file", fail_second)
    with pytest.raises(OSError, match="synthetic"):
        harvest.selected_harvest(["cloudflare"])
    assert harvest.MANIFEST.read_bytes() == previous
    target = harvest.CATALOG / "skills/cloudflare/cloudflare"
    assert target.exists()
    monkeypatch.setattr(additive, "publish_file", original_publish)
    assert harvest.selected_harvest(["cloudflare"]) == 0
    assert additive.inventory(target) == additive.inventory(source)


def test_failed_metadata_publish_resumes_after_complete_outputs(library, monkeypatch):
    additive, harvest, _ = library
    source = source_skill(harvest, "cloudflare")
    register(harvest, source)
    previous = harvest.MANIFEST.read_bytes()
    original = additive.os.replace

    def fail(*_):
        raise OSError("synthetic metadata failure")

    monkeypatch.setattr(additive.os, "replace", fail)
    with pytest.raises(OSError, match="metadata"):
        harvest.selected_harvest(["cloudflare"])
    assert harvest.MANIFEST.read_bytes() == previous
    assert additive.inventory(source) == additive.inventory(harvest.CATALOG / "skills/cloudflare/cloudflare")
    monkeypatch.setattr(additive.os, "replace", original)
    assert harvest.selected_harvest(["cloudflare"]) == 0


def test_unknown_or_duplicate_selection_never_creates_output(library):
    _, harvest, _ = library
    source = source_skill(harvest, "cloudflare")
    register(harvest, source)
    original = harvest.MANIFEST.read_bytes()
    for names in (["missing"], ["cloudflare", "cloudflare"]):
        with pytest.raises((ValueError, AssertionError)):
            harvest.selected_harvest(names)
    assert harvest.MANIFEST.read_bytes() == original
    assert not (harvest.CATALOG / "skills/cloudflare").exists()


def authored(sync, kind, name):
    if kind == "skill":
        path = sync.AUTHORED / "skills/devtools" / name / "SKILL.md"
    else:
        path = sync.AUTHORED / "agents/integration" / f"{name}.md"
    path.parent.mkdir(parents=True)
    path.write_text(f"---\nname: {name}\ndescription: Fixture\n---\nInstructions.\n", encoding="utf-8")
    return path


def test_selected_sync_publishes_only_requested_catalog_authored_and_agent(library):
    additive, harvest, sync = library
    source = source_skill(harvest, "cloudflare")
    register(harvest, source)
    harvest.selected_harvest(["cloudflare"])
    control = authored(sync, "skill", "cloudflare-control")
    agent = authored(sync, "agent", "cloudflare-controller")
    unrelated = sync.ROOT.parent / "plugin/skills/unrelated/keep.txt"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_bytes(b"preserve plugin output")
    before = harvest.MANIFEST.read_bytes()
    assert sync.selected_sync("plugin", ["cloudflare", "cloudflare-control", "cloudflare-controller"]) == 0
    assert (sync.ROOT.parent / "plugin/skills/cloudflare-control/SKILL.md").read_bytes() == control.read_bytes()
    assert (sync.ROOT.parent / "plugin/agents/cloudflare-controller.md").read_bytes() == agent.read_bytes()
    assert unrelated.read_bytes() == b"preserve plugin output"
    assert harvest.MANIFEST.read_bytes() == before
    snapshots = additive.inventory(sync.ROOT.parent / "plugin")
    sync.selected_sync("plugin", ["cloudflare", "cloudflare-control", "cloudflare-controller"])
    assert additive.inventory(sync.ROOT.parent / "plugin") == snapshots


def test_selected_sync_conflict_unknown_and_global_refuse_before_outputs(library):
    _, _, sync = library
    authored(sync, "skill", "cloudflare-control")
    authored(sync, "agent", "cloudflare-controller")
    conflicting = sync.ROOT.parent / "plugin/agents/cloudflare-controller.md"
    conflicting.parent.mkdir(parents=True)
    conflicting.write_bytes(b"other controller")
    with pytest.raises(ValueError, match="conflicts"):
        sync.selected_sync("plugin", ["cloudflare-control", "cloudflare-controller"])
    assert not (sync.ROOT.parent / "plugin/skills/cloudflare-control").exists()
    for target, names in (
        ("global", ["cloudflare-control"]),
        ("project", ["cloudflare-control"]),
        ("agents", ["cloudflare-controller"]),
        ("plugin", ["unknown"]),
    ):
        with pytest.raises(ValueError):
            sync.selected_sync(target, names)
    assert conflicting.read_bytes() == b"other controller"


def test_publication_budget_and_unsafe_destinations_refuse(library, monkeypatch):
    additive, harvest, _ = library
    source = source_skill(harvest, "cloudflare")
    register(harvest, source)
    monkeypatch.setattr(additive, "MAX_TOTAL_BYTES", 1)
    with pytest.raises(ValueError, match="byte limit"):
        harvest.selected_harvest(["cloudflare"])
    assert not (harvest.CATALOG / "skills/cloudflare").exists()
    for relative in ("../escape", "/absolute", "C:/escape", "\\\\server\\share", "ok/../escape"):
        with pytest.raises(ValueError):
            additive.safe_destination(harvest.ROOT, relative)


def test_selected_flags_are_opt_in_and_defaults_keep_existing_route(library, monkeypatch):
    _, harvest, sync = library
    for module, command in ((harvest, "harvest.py"), (sync, "sync.py")):
        for arguments in (["--select", "cloudflare"], ["--additive"]):
            monkeypatch.setattr(sys, "argv", [command, *arguments])
            with pytest.raises(SystemExit) as error:
                module.main()
            assert error.value.code == 2


@pytest.mark.parametrize("target", ["plugin", "local", "agents"])
def test_selected_sync_rejects_linked_output_root_before_writes(library, monkeypatch, target):
    _, _, sync = library
    authored(sync, "skill", "cloudflare-control")
    output, _ = sync.target_base(target)
    original = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda path: path == output or original(path))
    with pytest.raises(ValueError, match="root/ancestor is a link"):
        sync.selected_sync(target, ["cloudflare-control"])
    assert not output.exists()


def test_harvest_rejects_linked_catalog_and_parent(library, monkeypatch):
    _, harvest, _ = library
    source = source_skill(harvest, "cloudflare")
    register(harvest, source)
    original = Path.is_symlink
    for linked in (harvest.CATALOG, harvest.ROOT.parent):
        with monkeypatch.context() as context:
            context.setattr(Path, "is_symlink", lambda path, linked=linked: path == linked or original(path))
            with pytest.raises(ValueError, match="root/ancestor is a link"):
                harvest.selected_harvest(["cloudflare"])
    assert not (harvest.CATALOG / "skills/cloudflare").exists()


def test_real_linked_output_cannot_redirect_publication(library, tmp_path):
    _, _, sync = library
    authored(sync, "skill", "cloudflare-control")
    outside = tmp_path / "outside-owned-tree"
    outside.mkdir()
    output, _ = sync.target_base("plugin")
    output.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="root/ancestor is a link"):
        sync.selected_sync("plugin", ["cloudflare-control"])
    assert list(outside.iterdir()) == []
    assert output.is_symlink()


def test_merged_metadata_budget_refuses_before_catalog_writes(library, monkeypatch):
    additive, harvest, _ = library
    source = source_skill(harvest, "cloudflare")
    register(harvest, source)
    original = harvest.MANIFEST.read_bytes()
    size = len(additive.metadata_bytes(json.loads(original)))
    monkeypatch.setattr(additive, "MAX_METADATA_BYTES", size + 32)
    with pytest.raises(ValueError, match="metadata byte limit"):
        harvest.selected_harvest(["cloudflare"])
    assert not (harvest.CATALOG / "skills/cloudflare").exists()
    assert harvest.MANIFEST.read_bytes() == original


def test_selected_modes_never_call_unbounded_legacy_readers(library, monkeypatch):
    _, harvest, sync = library
    source = source_skill(harvest, "cloudflare")
    register(harvest, source)

    def forbidden(*_):
        raise AssertionError("unbounded legacy reader called")

    monkeypatch.setattr(harvest, "read_skill_name", forbidden)
    monkeypatch.setattr(harvest, "sha256_of", forbidden)
    monkeypatch.setattr(sync, "load_manifest", forbidden)
    monkeypatch.setattr(sync, "read_skill_name", forbidden)
    assert harvest.selected_harvest(["cloudflare"]) == 0
    assert sync.selected_sync("plugin", ["cloudflare"]) == 0


def test_selected_skill_and_source_list_have_preparse_byte_limits(library):
    additive, harvest, _ = library
    source = source_skill(harvest, "cloudflare")
    register(harvest, source)
    (source / "SKILL.md").write_bytes(b"x" * (additive.MAX_FILE_BYTES + 1))
    with pytest.raises(ValueError, match="byte limit"):
        harvest.selected_harvest(["cloudflare"])
    assert not (harvest.CATALOG / "skills/cloudflare").exists()
    harvest.SOURCES.write_bytes(b"x" * (additive.MAX_FILE_BYTES + 1))
    with pytest.raises(ValueError, match="byte limit"):
        harvest.selected_harvest(["cloudflare"])


def test_selected_sync_manifest_has_preparse_byte_limit(library):
    additive, _, sync = library
    authored(sync, "skill", "cloudflare-control")
    sync.MANIFEST.write_bytes(b" " * (additive.MAX_METADATA_BYTES + 1))
    with pytest.raises(ValueError, match="selected metadata"):
        sync.selected_sync("plugin", ["cloudflare-control"])
    assert not (sync.ROOT.parent / "plugin").exists()


def test_portable_origin_is_idempotent_across_different_homes(library, monkeypatch, tmp_path):
    _, harvest, _ = library
    first = source_skill(harvest, "cloudflare")
    second = tmp_path / "another home/.agents/skills/cloudflare"
    second.mkdir(parents=True)
    (second / "SKILL.md").write_bytes((first / "SKILL.md").read_bytes())
    (second / "references").mkdir()
    (second / "references/contract.md").write_bytes((first / "references/contract.md").read_bytes())
    configured = "~/.agents/skills/cloudflare"
    harvest.SOURCES.write_text(
        '[[item]]\nsrc="~/.agents/skills/cloudflare"\ntype="skill"\ncategory="cloudflare"\n', encoding="utf-8"
    )
    original_resolver = harvest.source_path
    current_home_source = first
    monkeypatch.setattr(
        harvest, "source_path", lambda value: current_home_source if value == configured else original_resolver(value)
    )
    assert harvest.selected_harvest(["cloudflare"]) == 0
    original = harvest.MANIFEST.read_bytes()
    document = json.loads(original)
    assert document["items"][-1]["origin"] == configured
    assert document["items"][0]["origin"] == "C:/unavailable/legacy"
    current_home_source = second
    assert harvest.selected_harvest(["cloudflare"]) == 0
    assert harvest.MANIFEST.read_bytes() == original


@pytest.mark.parametrize(
    "invalid", [{"items": "wrong"}, {"items": [], "counts": []}, {"items": [{"name": [], "type": "skill"}]}]
)
def test_invalid_existing_metadata_shape_refuses_before_outputs(library, invalid):
    _, harvest, _ = library
    source = source_skill(harvest, "cloudflare")
    register(harvest, source)
    original = json.dumps(invalid).encode()
    harvest.MANIFEST.write_bytes(original)
    with pytest.raises(ValueError, match="selected metadata"):
        harvest.selected_harvest(["cloudflare"])
    assert harvest.MANIFEST.read_bytes() == original
    assert not (harvest.CATALOG / "skills/cloudflare").exists()


def test_linked_manifest_refuses_before_read_or_catalog_output(library, tmp_path):
    _, harvest, _ = library
    source = source_skill(harvest, "cloudflare")
    register(harvest, source)
    original = harvest.MANIFEST.read_bytes()
    external = tmp_path / "external-manifest.json"
    external.write_bytes(original)
    harvest.MANIFEST.unlink()
    harvest.MANIFEST.symlink_to(external)
    with pytest.raises(ValueError, match="root/ancestor is a link"):
        harvest.selected_harvest(["cloudflare"])
    assert harvest.MANIFEST.is_symlink()
    assert external.read_bytes() == original
    assert not (harvest.CATALOG / "skills/cloudflare").exists()
