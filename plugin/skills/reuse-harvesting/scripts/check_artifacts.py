#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Check a trusted ai-grind artifact helper without importing the MCP package."""

from __future__ import annotations

import argparse
import importlib.util
import json
import platform
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from types import ModuleType


def load_artifacts(checkout: Path) -> tuple[ModuleType, Path]:
    source = checkout.resolve() / "src/devtools_mcp/evidence/artifacts.py"
    if not source.is_file():
        raise FileNotFoundError(f"ai-grind artifact helper is missing: {source}")
    spec = importlib.util.spec_from_file_location("reuse_harvest_artifacts", source)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load artifact helper: {source}")
    assert spec.name == "reuse_harvest_artifacts"
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    assert module.__file__ == str(source)
    return module, source


def expect_error(expected: type[Exception], operation: Callable[[], object]) -> None:
    assert issubclass(expected, Exception)
    assert callable(operation)
    try:
        operation()
    except expected:
        return
    raise RuntimeError(f"operation unexpectedly succeeded; expected {expected.__name__}")


def check_bindings(module: ModuleType, root: Path) -> list[str]:
    assert root.is_dir()
    assert not any(root.iterdir())
    target = root / "source with spaces.txt"
    target.write_bytes(b"first\n")
    binding = module.fingerprint_file(target)
    module.verify_binding(binding)
    target.write_bytes(b"other\n")
    expect_error(module.ArtifactError, lambda: module.verify_binding(binding))
    expect_error(FileNotFoundError, lambda: module.fingerprint_file(root / "absent"))
    expect_error(module.ArtifactError, lambda: module.fingerprint_file(target, max_bytes=2))
    expect_error(module.ArtifactError, lambda: module.fingerprint_file(root))
    link = root / "link"
    link.symlink_to(target)
    expect_error(module.ArtifactError, lambda: module.fingerprint_file(link))
    inventory = module.inventory_artifacts(root, limits=module.ScanLimits(max_entries=1))
    if inventory.complete or inventory.entries_seen > 1:
        raise RuntimeError("inventory cap must report incomplete discovery")
    return [
        "unchanged binding and spaces",
        "same-size drift rejected",
        "missing file rejected",
        "oversized file rejected",
        "directory rejected",
        "leaf symlink rejected",
        "partial inventory explicit",
    ]


def check_publication(module: ModuleType, root: Path) -> list[str]:
    assert root.is_dir()
    record = root / "binding.json"
    assert not record.exists()
    module.write_json_record(record, {"result": "first"})
    original = record.read_bytes()
    if json.loads(original) != {"result": "first"}:
        raise RuntimeError("JSON roundtrip mismatch")
    expect_error(FileExistsError, lambda: module.write_json_record(record, {}))
    if record.read_bytes() != original:
        raise RuntimeError("existing record was altered")
    expect_error(FileNotFoundError, lambda: module.write_json_record(root / "absent/x", {}))
    expect_error(ValueError, lambda: module.write_json_record(root / "nan", {"v": float("nan")}))
    expect_error(module.ArtifactError, lambda: module.write_json_record(root / "oversized", {"v": "x"}, max_bytes=1))
    module.write_json_record(record, {"result": "replacement"}, replace=True)
    if json.loads(record.read_bytes()) != {"result": "replacement"}:
        raise RuntimeError("explicit replacement failed")
    if (root / "nan").exists() or (root / "oversized").exists() or any(root.glob(".evidence-*.tmp")):
        raise RuntimeError("failed publication left files behind")
    return [
        "atomic record roundtrip",
        "existing record preserved",
        "missing destination parent rejected",
        "nonfinite JSON rejected",
        "oversized JSON rejected",
        "explicit replacement",
        "temporary cleanup",
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ai-grind", type=Path, required=True, help="trusted local ai-grind checkout")
    args = parser.parse_args(argv)
    assert isinstance(args.ai_grind, Path)
    try:
        module, source = load_artifacts(args.ai_grind)
        binding = module.fingerprint_file(source, max_bytes=1 << 20)
        with tempfile.TemporaryDirectory(prefix="artifact contracts ") as directory:
            root = Path(directory)
            checks = check_bindings(module, root) + check_publication(module, root)
        module.verify_binding(binding, max_bytes=1 << 20)
        assert len(checks) == 14
        print(
            json.dumps(
                {
                    "python": sys.version,
                    "platform": platform.platform(),
                    "module_path": str(source),
                    "module_sha256": binding.sha256,
                    "checks": checks,
                },
                indent=2,
            )
        )
        return 0
    except (OSError, ValueError, RuntimeError, ImportError, SyntaxError) as error:
        print(f"Artifact contract check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
