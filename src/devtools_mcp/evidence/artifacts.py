"""Bounded artifact identity, inventory, and atomic developer evidence records.

Callers own the directories being inspected. This detects ordinary file churn;
it is not a sandbox against adversarial ancestor-directory replacement.
Source/adaptation provenance: docs/research/tmp-python-mining-20261012.md.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

HASH_CHUNK_BYTES = 1 << 20
DEFAULT_HASH_BYTES = 64 << 20
MAX_RECORD_BYTES = 32 << 20
DEFAULT_EXCLUDED_DIRS = frozenset({".git", ".venv", "venv", "node_modules", "site-packages", "__pycache__"})


class ArtifactError(ValueError):
    """An artifact is unsuitable, changed during inspection, or exceeds a cap."""


@dataclass(frozen=True)
class FileBinding:
    path: str
    size_bytes: int
    mtime_ns: int
    ctime_ns: int
    device: int
    inode: int
    sha256: str


@dataclass(frozen=True)
class ScanLimits:
    max_entries: int = 10_000
    max_depth: int = 32
    max_total_bytes: int = 1 << 30

    def __post_init__(self) -> None:
        if not (0 < self.max_entries <= 1_000_000 and 0 <= self.max_depth <= 128 and self.max_total_bytes >= 0):
            raise ValueError("invalid inventory limits")
        assert self.max_entries > 0
        assert self.max_depth >= 0


DEFAULT_SCAN_LIMITS = ScanLimits()


@dataclass(frozen=True)
class FileEntry:
    relative_path: str
    size_bytes: int
    mtime_ns: int


@dataclass(frozen=True)
class ScanIssue:
    relative_path: str
    reason: str


@dataclass(frozen=True)
class ArtifactInventory:
    root: str
    files: tuple[FileEntry, ...]
    issues: tuple[ScanIssue, ...]
    exclusions: tuple[ScanIssue, ...]
    entries_seen: int
    total_bytes: int
    complete: bool


def _identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    assert isinstance(info, os.stat_result)
    result = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    assert len(result) == 5
    return result


def _regular(path: Path) -> os.stat_result:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ArtifactError("artifact must be a regular file without a symlink")
    assert info.st_size >= 0
    assert not stat.S_ISLNK(info.st_mode)
    return info


def fingerprint_file(path: Path, *, max_bytes: int = DEFAULT_HASH_BYTES) -> FileBinding:
    """Hash a stable regular file; reject oversized, replaced, or modified files.

    OSError propagates for IO failures. The leaf is opened without following
    symlinks where supported; before/open/after identities must agree.
    """
    if not (0 < max_bytes <= 1 << 40):
        raise ValueError("max_bytes must be positive and at most 1 TiB")
    absolute = Path(os.path.abspath(path))
    before = _regular(absolute)
    if before.st_size > max_bytes:
        raise ArtifactError("artifact exceeds byte limit")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0)
    fd = os.open(absolute, flags)
    with os.fdopen(fd, "rb") as stream:
        if _identity(before) != _identity(os.fstat(stream.fileno())):
            raise ArtifactError("artifact changed before hashing")
        digest, consumed = hashlib.sha256(), 0
        for _ in range(before.st_size // HASH_CHUNK_BYTES + 2):
            chunk = stream.read(min(HASH_CHUNK_BYTES, max_bytes - consumed + 1))
            if not chunk:
                break
            consumed += len(chunk)
            if consumed > max_bytes:
                raise ArtifactError("artifact grew beyond byte limit")
            digest.update(chunk)
        if consumed != before.st_size or _identity(before) != _identity(os.fstat(stream.fileno())):
            raise ArtifactError("artifact changed during hashing")
    if _identity(before) != _identity(_regular(absolute)):
        raise ArtifactError("artifact changed after hashing")
    result = FileBinding(
        str(absolute),
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
        before.st_dev,
        before.st_ino,
        digest.hexdigest(),
    )
    assert consumed <= max_bytes
    assert len(result.sha256) == 64
    return result


def verify_binding(binding: FileBinding, *, max_bytes: int = DEFAULT_HASH_BYTES) -> None:
    """Rehash and require the complete file binding to match; never repair it."""
    assert isinstance(binding, FileBinding)
    actual = fingerprint_file(Path(binding.path), max_bytes=max_bytes)
    if actual != binding:
        raise ArtifactError("artifact binding mismatch")
    assert actual.sha256 == binding.sha256


def _scan_directory(
    root: Path,
    directory: Path,
    depth: int,
    limits: ScanLimits,
    excluded_dirs: frozenset[str],
    files: list[FileEntry],
    issues: list[ScanIssue],
    exclusions: list[ScanIssue],
    pending: list[tuple[Path, int]],
    seen: int,
    total: int,
) -> tuple[int, int, bool]:
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                relative = str(Path(entry.path).relative_to(root))
                if seen == limits.max_entries:
                    issues.append(ScanIssue(relative, "entry_limit"))
                    return seen, total, True
                seen += 1
                try:
                    info = entry.stat(follow_symlinks=False)
                    if stat.S_ISLNK(info.st_mode):
                        exclusions.append(ScanIssue(relative, "symlink"))
                    elif stat.S_ISDIR(info.st_mode):
                        if entry.name in excluded_dirs:
                            exclusions.append(ScanIssue(relative, "excluded_directory"))
                        elif depth == limits.max_depth:
                            issues.append(ScanIssue(relative, "depth_limit"))
                        else:
                            pending.append((Path(entry.path), depth + 1))
                    elif stat.S_ISREG(info.st_mode):
                        if total + info.st_size > limits.max_total_bytes:
                            issues.append(ScanIssue(relative, "byte_limit"))
                            return seen, total, True
                        total += info.st_size
                        files.append(FileEntry(relative, info.st_size, info.st_mtime_ns))
                    else:
                        exclusions.append(ScanIssue(relative, "special_file"))
                except OSError as exc:
                    issues.append(ScanIssue(relative, f"io_error:{exc.errno}"))
    except OSError as exc:
        issues.append(ScanIssue(str(directory.relative_to(root)), f"io_error:{exc.errno}"))
    assert seen <= limits.max_entries
    assert total <= limits.max_total_bytes
    return seen, total, False


def inventory_artifacts(
    root: Path, *, limits: ScanLimits = DEFAULT_SCAN_LIMITS, excluded_dirs: frozenset[str] = DEFAULT_EXCLUDED_DIRS
) -> ArtifactInventory:
    """Inventory regular files without opening contents or following leaf links.

    `complete` means traversal completed within caps without IO errors, relative
    to explicit exclusions. This is an observation, not an atomic tree snapshot.
    File/entry counts and logical bytes never exceed their declared caps.
    """
    absolute = Path(os.path.abspath(root))
    if not stat.S_ISDIR(absolute.lstat().st_mode):
        raise ArtifactError("inventory root must be a directory without a symlink")
    files: list[FileEntry] = []
    issues: list[ScanIssue] = []
    exclusions: list[ScanIssue] = []
    pending = [(absolute, 0)]
    seen = total = 0
    for _ in range(limits.max_entries + 1):
        if not pending:
            break
        directory, depth = pending.pop()
        try:
            if not stat.S_ISDIR(directory.lstat().st_mode):
                issues.append(ScanIssue(str(directory.relative_to(absolute)), "directory_changed"))
                continue
            seen, total, stop = _scan_directory(
                absolute, directory, depth, limits, excluded_dirs, files, issues, exclusions, pending, seen, total
            )
            if stop:
                break
        except OSError as exc:
            issues.append(ScanIssue(str(directory.relative_to(absolute)), f"io_error:{exc.errno}"))
    result = ArtifactInventory(
        str(absolute),
        tuple(sorted(files, key=lambda f: f.relative_path)),
        tuple(issues),
        tuple(exclusions),
        seen,
        total,
        not issues and not pending,
    )
    assert result.entries_seen <= limits.max_entries
    assert result.total_bytes <= limits.max_total_bytes
    return result


def write_json_record(
    destination: Path, record: Mapping[str, object], *, replace: bool = False, max_bytes: int = 1 << 20
) -> FileBinding:
    """Publish a bounded JSON object atomically, preserving an existing file by default.

    Caller-supplied objects are serialized with NaN/Inf forbidden. The temporary
    file is flushed and synced before publication; on POSIX the parent directory
    is also synced. IO errors propagate, including errors after publication
    (which may leave the completed record at its destination). No parent dirs
    are created. `replace=True` explicitly permits replacing an existing record.
    """
    if not (0 < max_bytes <= MAX_RECORD_BYTES):
        raise ValueError("record byte limit must be positive and at most 32 MiB")
    data = (json.dumps(dict(record), allow_nan=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    if len(data) > max_bytes:
        raise ArtifactError("JSON record exceeds byte limit")
    absolute = Path(os.path.abspath(destination))
    fd, name = tempfile.mkstemp(prefix=".evidence-", suffix=".tmp", dir=absolute.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            written = stream.write(data)
            if written != len(data):
                raise OSError("incomplete JSON record write")
            stream.flush()
            os.fsync(stream.fileno())
        if replace:
            os.replace(temporary, absolute)
        else:
            os.link(temporary, absolute)
            temporary.unlink()
        if os.name != "nt":
            directory_fd = os.open(absolute.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    result = fingerprint_file(absolute, max_bytes=max_bytes)
    if result.size_bytes != len(data) or result.sha256 != hashlib.sha256(data).hexdigest():
        raise ArtifactError("published JSON record changed")
    assert result.size_bytes <= max_bytes
    assert len(result.sha256) == 64
    return result
