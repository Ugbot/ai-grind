"""The public bundle must match its sources and exclude project-only assets."""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_public_bundle_matches_regeneration(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("bundle_sync", ROOT / "skills/sync.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    generated = tmp_path / "plugin"
    monkeypatch.setattr(module, "target_base", lambda name: (generated, True))
    monkeypatch.setattr(sys, "argv", ["sync.py", "--target", "plugin"])
    assert module.main() == 0

    def snapshot(root):
        return {path.relative_to(root): path.read_bytes() for path in root.rglob("*") if path.is_file()}

    assert snapshot(generated) == snapshot(ROOT / "plugin")
    names = {path.name for path in (generated / "skills").iterdir()}
    assert not any(name.startswith("venus-") for name in names)
    assert not names.intersection({"chukonu-dev", "bench-rdtsc-profile", "debug-linux-lldb", "debug-windows-msvc"})
    assert {p.name for p in (generated / "commands").iterdir()} == {"clean-test-data.md"}
    assert not {p.stem for p in (generated / "agents").iterdir()}.intersection(
        {"living-docs-writer", "test-bench-runner"}
    )
