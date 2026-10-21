"""Selected publication: bounded staging, no replacement, metadata written last."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import tempfile
from pathlib import Path, PurePosixPath, PureWindowsPath

MAX_FILES = 8192
MAX_FILE_BYTES = 1024 * 1024
MAX_TOTAL_BYTES = 64 * 1024 * 1024
MAX_METADATA_BYTES = 1024 * 1024
MAX_ITEMS = 200


def reject_output_links(path: Path) -> None:
    absolute = path.absolute()
    ancestors = (absolute, *absolute.parents)
    if len(ancestors) > 128:
        raise ValueError("selected output path depth limit")
    for ancestor in ancestors:
        if ancestor.is_symlink() or ancestor.is_junction():
            raise ValueError(f"selected output root/ancestor is a link: {ancestor}")
    assert len(ancestors) <= 128
    assert absolute.is_absolute()


def read_bytes(path: Path, limit: int = MAX_FILE_BYTES) -> bytes:
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError(f"selected input exceeds byte limit: {path}")
    assert len(data) <= limit
    assert isinstance(data, bytes)
    return data


def safe_destination(root: Path, relative: str) -> Path:
    reject_output_links(root)
    parts = PureWindowsPath(relative).parts if "\\" in relative else PurePosixPath(relative).parts
    if (
        not parts
        or any(part in ("..", ".") or ":" in part for part in parts)
        or PureWindowsPath(relative).anchor
        or PurePosixPath(relative).anchor
    ):
        raise ValueError("unsafe selected destination")
    target = root.joinpath(*parts)
    reject_output_links(target)
    if not target.resolve().is_relative_to(root.resolve()):
        raise ValueError("selected destination escapes root")
    assert target != root
    assert target.is_relative_to(root)
    return target


def skill_name(path: Path) -> str:
    lines = read_bytes(path).decode("utf-8").splitlines()
    if not lines or lines[0].strip() != "---":
        raise ValueError("selected skill has no frontmatter")
    for line in lines[1:60]:
        if line.strip() == "---":
            break
        if line.startswith("name:"):
            name = line.split(":", 1)[1].strip().strip("'\"")
            if not name or len(name) > 128 or "/" in name or "\\" in name or name in (".", ".."):
                raise ValueError("invalid selected skill name")
            return name
    raise ValueError("selected skill has no name")


def read_document(path: Path) -> dict:
    try:
        document = json.loads(read_bytes(path, MAX_METADATA_BYTES))
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ValueError("invalid selected metadata") from error
    metadata_bytes(document)
    assert isinstance(document, dict)
    assert isinstance(document["items"], list)
    return document


def metadata_bytes(document: dict) -> bytes:
    if not isinstance(document, dict) or not isinstance(document.get("items"), list):
        raise ValueError("invalid selected metadata shape")
    items = document["items"]
    if len(items) > MAX_ITEMS or any(not isinstance(item, dict) for item in items):
        raise ValueError("invalid selected metadata item shape/count")
    if any(
        not isinstance(item.get("name"), str) or item.get("type") not in ("skill", "agent", "command", "archive")
        for item in items
    ):
        raise ValueError("invalid selected metadata identity")
    if "counts" in document and not isinstance(document["counts"], dict):
        raise ValueError("invalid selected metadata counts")
    try:
        data = json.dumps(document, indent=2, allow_nan=False).encode() + b"\n"
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ValueError("invalid selected metadata encoding") from error
    if len(data) > MAX_METADATA_BYTES:
        raise ValueError("selected metadata byte limit")
    assert len(data) <= MAX_METADATA_BYTES
    assert isinstance(data, bytes)
    return data


def inventory(root: Path) -> dict[str, str]:
    if root.is_symlink():
        raise ValueError(f"selected tree must not be a symlink: {root}")
    if root.is_file():
        return {"": hashlib.sha256(read_bytes(root)).hexdigest()}
    if not root.is_dir():
        raise ValueError(f"selected tree missing: {root}")
    result: dict[str, str] = {}
    total = 0
    pending = [root]
    visited = 0
    for _ in range(MAX_FILES + 1):
        if not pending:
            assert len(result) <= MAX_FILES and total <= MAX_TOTAL_BYTES
            return result
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                visited += 1
                if visited > MAX_FILES or entry.is_symlink():
                    raise ValueError("selected tree count/symlink limit")
                path = Path(entry.path)
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                elif entry.is_file(follow_symlinks=False):
                    data = read_bytes(path)
                    total += len(data)
                    if total > MAX_TOTAL_BYTES:
                        raise ValueError("selected tree total byte limit")
                    result[path.relative_to(root).as_posix()] = hashlib.sha256(data).hexdigest()
                else:
                    raise ValueError("selected tree contains a special file")
    raise ValueError("selected tree count limit")


def check_destination(destination: Path, expected: dict[str, str]) -> None:
    assert expected and len(expected) <= MAX_FILES
    reject_output_links(destination)
    if destination.exists() or destination.is_symlink():
        actual = inventory(destination)
        if any(expected.get(name) != digest for name, digest in actual.items()):
            raise ValueError(f"selected destination conflicts: {destination}")
    assert isinstance(destination, Path)


def publish_file(source: Path, destination: Path, expected: str, temporary_parent: Path) -> None:
    reject_output_links(destination)
    reject_output_links(temporary_parent)
    data = read_bytes(source)
    if hashlib.sha256(data).hexdigest() != expected:
        raise ValueError("selected source changed during publication")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=temporary_parent, prefix=".selected-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, source.stat().st_mode & 0o777)
        try:
            os.link(temporary, destination)  # atomic create, never replace another writer's file
        except FileExistsError:
            if hashlib.sha256(read_bytes(destination)).hexdigest() != expected:
                raise ValueError(f"selected destination changed: {destination}") from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    assert destination.is_file()
    assert hashlib.sha256(read_bytes(destination)).hexdigest() == expected


def publish(plans: list[tuple[Path, Path]]) -> None:
    if not 0 < len(plans) <= 200 or len({str(dest.resolve()) for _, dest in plans}) != len(plans):
        raise ValueError("invalid selected publication plan")
    inventories = [inventory(source) for source, _ in plans]
    if sum(len(files) for files in inventories) > MAX_FILES:
        raise ValueError("selected publication total file limit")
    for (_, destination), files in zip(plans, inventories, strict=True):
        check_destination(destination, files)
    with tempfile.TemporaryDirectory(prefix="ai-grind-selected-") as temporary:
        staged = Path(temporary)
        total = 0
        for index, ((source, _), files) in enumerate(zip(plans, inventories, strict=True)):
            for name, digest in files.items():
                data = read_bytes(source / name)
                total += len(data)
                if total > MAX_TOTAL_BYTES or hashlib.sha256(data).hexdigest() != digest:
                    raise ValueError("selected source changed or total bytes exceeded")
                target = staged / str(index) / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
                os.chmod(target, (source / name).stat().st_mode & 0o777)
        for (_, destination), files in zip(plans, inventories, strict=True):
            check_destination(destination, files)  # recheck after staging, before outputs
        for index, ((_, destination), files) in enumerate(zip(plans, inventories, strict=True)):
            for name, digest in files.items():
                target = destination / name
                if target.is_file() and hashlib.sha256(read_bytes(target)).hexdigest() == digest:
                    continue
                # Temporary files stay outside the selected tree, so a hard
                # interruption cannot turn its orphan temp into a resume conflict.
                publish_file(staged / str(index) / name, target, digest, destination.parent)
    assert all(destination.exists() for _, destination in plans)


def write_metadata(path: Path, document: dict, expected: bytes | None) -> None:
    reject_output_links(path)
    data = metadata_bytes(document)
    current = read_bytes(path, MAX_METADATA_BYTES) if path.exists() else None
    if current != expected:
        raise ValueError("selected metadata changed concurrently")
    if current is not None and json.loads(current) == document:
        return
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".selected-manifest-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        current = read_bytes(path, MAX_METADATA_BYTES) if path.exists() else None
        if current != expected:
            raise ValueError("selected metadata changed concurrently")
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()
    assert path.is_file()
    assert json.loads(read_bytes(path, MAX_METADATA_BYTES)) == document
