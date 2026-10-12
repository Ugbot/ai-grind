"""Synthetic fixtures for file churn, caps, exclusions and atomic publication."""

import hashlib
import json
import os
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest

from devtools_mcp.evidence import (
    ArtifactError,
    ScanLimits,
    fingerprint_file,
    inventory_artifacts,
    verify_binding,
    write_json_record,
)


def test_fingerprint_and_verify(tmp_path: Path):
    file = tmp_path / "binary"
    content = b"abc" * (1 << 20)
    file.write_bytes(content)
    binding = fingerprint_file(file)
    assert binding.sha256 == hashlib.sha256(content).hexdigest()
    assert binding.size_bytes == len(content)
    verify_binding(binding)
    with pytest.raises(ArtifactError, match="mismatch"):
        verify_binding(replace(binding, sha256="0" * 64))


def test_zero_size_and_exact_cap(tmp_path: Path):
    file = tmp_path / "empty"
    file.touch()
    assert fingerprint_file(file, max_bytes=1).size_bytes == 0
    file.write_bytes(b"abcd")
    assert fingerprint_file(file, max_bytes=4).size_bytes == 4
    with pytest.raises(ArtifactError, match="limit"):
        fingerprint_file(file, max_bytes=3)


def test_replaced_and_same_size_modified_bindings_fail(tmp_path: Path):
    file = tmp_path / "source"
    file.write_bytes(b"abc")
    binding = fingerprint_file(file)
    file.write_bytes(b"xyz")
    os.utime(file, ns=(binding.mtime_ns, binding.mtime_ns))
    with pytest.raises(ArtifactError, match="mismatch"):
        verify_binding(binding)
    file.unlink()
    file.write_bytes(b"abc")
    with pytest.raises(ArtifactError, match="mismatch"):
        verify_binding(binding)


def test_symlinks_and_nonregular_files_rejected(tmp_path: Path):
    file = tmp_path / "file"
    file.write_bytes(b"data")
    link = tmp_path / "link"
    link.symlink_to(file)
    for target in (link, tmp_path):
        with pytest.raises(ArtifactError, match="regular"):
            fingerprint_file(target)
    with pytest.raises(ArtifactError, match="directory"):
        inventory_artifacts(link)


def test_churn_during_hash_rejected(tmp_path: Path, monkeypatch):
    file = tmp_path / "file"
    file.write_bytes(b"start")
    original = os.fstat
    calls = 0

    def changed(fd):
        nonlocal calls
        calls += 1
        if calls == 2:
            file.write_bytes(b"longer")
        return original(fd)

    monkeypatch.setattr(os, "fstat", changed)
    with pytest.raises(ArtifactError, match="changed"):
        fingerprint_file(file)


def test_inventory_exclusions_and_totals(tmp_path: Path):
    (tmp_path / "a").write_bytes(b"aa")
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "b").write_bytes(b"bbb")
    (tmp_path / ".venv").mkdir()
    (tmp_path / ".venv" / "excluded").write_bytes(b"hidden")
    (tmp_path / "link").symlink_to(tmp_path / "nested", target_is_directory=True)
    result = inventory_artifacts(tmp_path)
    assert result.complete and result.total_bytes == 5
    assert [f.relative_path for f in result.files] == ["a", str(Path("nested") / "b")]
    assert {issue.reason for issue in result.exclusions} == {"symlink", "excluded_directory"}


@pytest.mark.parametrize(
    "limits,reason",
    [
        (ScanLimits(max_entries=1), "entry_limit"),
        (ScanLimits(max_depth=0), "depth_limit"),
        (ScanLimits(max_total_bytes=1), "byte_limit"),
    ],
)
def test_inventory_never_claims_complete_at_limit(tmp_path: Path, limits: ScanLimits, reason: str):
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "large").write_bytes(b"data")
    result = inventory_artifacts(tmp_path, limits=limits)
    assert not result.complete
    assert reason in {issue.reason for issue in result.issues}
    assert result.entries_seen <= limits.max_entries
    assert result.total_bytes <= limits.max_total_bytes


def test_inventory_exact_entry_limit_can_be_complete(tmp_path: Path):
    (tmp_path / "only").touch()
    result = inventory_artifacts(tmp_path, limits=ScanLimits(max_entries=1))
    assert result.complete
    assert result.entries_seen == 1


def test_inventory_io_failure_is_explicit(tmp_path: Path, monkeypatch):
    def denied(_):
        raise PermissionError(13, "denied")

    monkeypatch.setattr(os, "scandir", denied)
    result = inventory_artifacts(tmp_path)
    assert not result.complete
    assert result.issues[0].reason == "io_error:13"


@pytest.mark.parametrize("kwargs", [{"max_entries": 0}, {"max_depth": -1}, {"max_total_bytes": -1}])
def test_bad_scan_limits(kwargs):
    with pytest.raises(ValueError):
        ScanLimits(**kwargs)


def test_atomic_json_preserves_previous_evidence(tmp_path: Path):
    destination = tmp_path / "result.json"
    binding = write_json_record(destination, {"status": "failed", "returncode": 2})
    verify_binding(binding)
    assert json.loads(destination.read_text()) == {"status": "failed", "returncode": 2}
    with pytest.raises(FileExistsError):
        write_json_record(destination, {"status": "passed"})
    assert json.loads(destination.read_text())["status"] == "failed"
    assert not list(tmp_path.glob(".evidence-*"))
    write_json_record(destination, {"status": "passed"}, replace=True)
    assert json.loads(destination.read_text())["status"] == "passed"


def test_atomic_json_limits_and_nonfinite_do_not_publish(tmp_path: Path):
    destination = tmp_path / "result.json"
    with pytest.raises(ArtifactError, match="limit"):
        write_json_record(destination, {"payload": "large"}, max_bytes=1)
    with pytest.raises(ValueError):
        write_json_record(destination, {"number": float("nan")})
    assert not destination.exists()
    assert not list(tmp_path.iterdir())


def test_atomic_json_publish_failure_cleans_temporary(tmp_path: Path, monkeypatch):
    destination = tmp_path / "result.json"

    def denied(*_):
        raise OSError(5, "publication failed")

    monkeypatch.setattr(os, "link", denied)
    with pytest.raises(OSError, match="publication failed"):
        write_json_record(destination, {"status": "failed"})
    assert not destination.exists()
    assert not list(tmp_path.iterdir())


def test_json_explicit_larger_cap_and_hard_cap(tmp_path: Path):
    destination = tmp_path / "result.json"
    payload = {"body": "x" * (1 << 20)}
    with pytest.raises(ArtifactError):
        write_json_record(destination, payload)
    assert write_json_record(destination, payload, max_bytes=2 << 20).size_bytes > 1 << 20
    with pytest.raises(ValueError):
        write_json_record(destination, {}, replace=True, max_bytes=33 << 20)


def test_inventory_iteration_error_keeps_partial_counters(tmp_path: Path, monkeypatch):
    (tmp_path / "pending").mkdir()
    (tmp_path / "first").write_bytes(b"1234")
    (tmp_path / "pending" / "second").write_bytes(b"567")
    original = os.scandir

    @contextmanager
    def partial(directory):
        with original(directory) as entries:
            if Path(directory) == tmp_path:
                by_name = {entry.name: entry for entry in entries}

                def fail_after_entries():
                    yield by_name["pending"]
                    yield by_name["first"]
                    raise OSError(5, "directory iteration failed")

                yield fail_after_entries()
            else:
                yield entries

    monkeypatch.setattr(os, "scandir", partial)
    result = inventory_artifacts(tmp_path, limits=ScanLimits(max_total_bytes=5))
    assert not result.complete
    assert result.entries_seen == 3
    assert result.total_bytes == sum(file.size_bytes for file in result.files) == 4
    assert {issue.reason for issue in result.issues} == {"io_error:5", "byte_limit"}


def test_atomic_json_sync_failure_preserves_previous_record(tmp_path: Path, monkeypatch):
    destination = tmp_path / "result.json"
    destination.write_bytes(b"previous evidence")

    def failed_sync(_):
        raise OSError(5, "file sync failed")

    monkeypatch.setattr(os, "fsync", failed_sync)
    with pytest.raises(OSError, match="file sync failed"):
        write_json_record(destination, {"status": "failed"}, replace=True)
    assert destination.read_bytes() == b"previous evidence"
    assert not list(tmp_path.glob(".evidence-*"))
