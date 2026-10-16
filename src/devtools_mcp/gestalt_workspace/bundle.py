"""Stable no-follow admission of the supported portable v1 asset bundle."""

from __future__ import annotations

import os
import re
import stat
from pathlib import Path
from typing import Any

from .common import _HASH, _REF, canonical, decode, digest, require


class Bundle:
    def __init__(self, directory: str, limits: dict[str, int]):
        require(type(directory) is str and Path(directory).is_absolute(), "absolute bundle_dir required")
        require(
            os.name == "posix" and hasattr(os, "O_DIRECTORY") and hasattr(os, "O_NOFOLLOW"),
            "safe no-follow bundle admission currently requires macOS/Linux POSIX descriptors",
            "unsupported_platform",
        )
        self.root = Path(directory).resolve(strict=True)
        require(self.root.is_dir(), "bundle_dir must be a directory")
        self.root_identity = (self.root.stat().st_dev, self.root.stat().st_ino)
        self.limits, self.total = limits, 0
        manifest = self.read("manifest.json")
        require(
            type(manifest) is dict and type(manifest.get("schema_version")) is int and manifest["schema_version"] == 1,
            "portable manifest v1 required",
        )
        self.namespace = manifest.get("namespace")
        self.prefix = manifest.get("name_prefix")
        require(
            self.namespace == "cme.research" and self.prefix == "CME Research /" and manifest.get("synthetic") is True,
            "only portable CME research v1 is supported",
        )
        entries = manifest.get("notebooks")
        require(
            type(entries) is list and len(entries) == 4 and len(entries) + 1 <= limits["max_resources"],
            "four portable notebooks and dashboard required",
        )
        self.notebooks: list[dict[str, Any]] = []
        keys: set[str] = set()
        names: set[str] = set()
        for entry in entries:
            require(type(entry) is dict, "notebook entry required")
            key, name = entry.get("key"), entry.get("name")
            require(
                type(key) is str and re.fullmatch(r"[a-z0-9-]{1,96}", key) is not None and key not in keys,
                "unique notebook key required",
            )
            self.name(name, names)
            document = self.read(entry.get("document"))
            require(type(document) is dict and document.get("name") == name, "notebook document name mismatch")
            body = document.get("body")
            self.body(body, "cells")
            cells = body["cells"]
            require(len(cells) <= 4096, "too many notebook cells")
            cell_ids: set[int] = set()
            for cell in cells:
                require(
                    type(cell) is dict
                    and type(cell.get("id")) is int
                    and cell["id"] > 0
                    and cell["id"] not in cell_ids
                    and cell.get("lang") in {"python", "markdown", "sql"}
                    and type(cell.get("code")) is str,
                    "invalid notebook cell",
                )
                cell_ids.add(cell["id"])
            expected = entry.get("body_sha256")
            require(
                type(expected) is str and _HASH.fullmatch(expected) is not None and digest(body) == expected,
                "notebook canonical body hash mismatch",
            )
            keys.add(key)
            self.notebooks.append({"kind": "notebook", "key": key, "name": name, "body": body})
        dashboard = manifest.get("dashboard")
        require(type(dashboard) is dict, "dashboard entry required")
        self.name(dashboard.get("name"), names)
        template = self.read(dashboard.get("template"))
        require(type(template) is dict and template.get("name") == dashboard["name"], "dashboard name mismatch")
        self.body(template.get("body"), "widgets")
        widget_ids: set[str] = set()
        require(len(template["body"]["widgets"]) <= 4096, "too many dashboard widgets")
        for widget in template["body"]["widgets"]:
            require(
                type(widget) is dict
                and type(widget.get("id")) is str
                and widget["id"]
                and widget["id"] not in widget_ids
                and widget.get("type") == "text"
                and all(type(widget.get(field)) is str for field in ("title", "width", "content")),
                "invalid dashboard text widget",
            )
            widget_ids.add(widget["id"])
        self.dashboard = {"kind": "dashboard", "name": dashboard["name"], "body": template["body"]}
        encoded = canonical(template["body"]).decode("utf-8")
        references = set(_REF.findall(encoded))
        require(
            references == keys and "{{" not in _REF.sub("", encoded) and "}}" not in _REF.sub("", encoded),
            "dashboard references must resolve exactly the declared notebook keys",
        )
        require(
            len(_REF.sub("18446744073709551615", encoded).encode("utf-8")) <= limits["max_body_bytes"],
            "resolved dashboard worst-case body byte limit exceeded",
        )
        if "body_sha256" in dashboard:
            require(dashboard["body_sha256"] == digest(template["body"]), "dashboard template hash mismatch")
        factories = manifest.get("source_factory_files")
        require(
            type(factories) is dict
            and 1 <= len(factories) <= 64
            and all(type(k) is str and type(v) is str and _HASH.fullmatch(v) for k, v in factories.items()),
            "invalid source factory digest map",
        )
        require(manifest.get("source_factory_sha256") == digest(factories), "source factory digest mismatch")

    def name(self, name: Any, names: set[str]) -> None:
        require(
            type(name) is str
            and name.startswith(self.prefix)
            and len(self.prefix) < len(name) <= 256
            and name not in names
            and all(ord(c) >= 32 for c in name),
            "unique prefixed name required",
        )
        names.add(name)

    def body(self, body: Any, field: str) -> None:
        require(type(body) is dict and type(body.get(field)) is list, f"body.{field} array required")
        require(len(canonical(body)) <= self.limits["max_body_bytes"], "body byte limit exceeded")

    def read(self, relative: Any) -> Any:
        require(
            type(relative) is str
            and bool(relative)
            and len(Path(relative).parts) == 1
            and relative not in {".", ".."}
            and not Path(relative).is_absolute(),
            "portable v1 files must be direct relative children of bundle root",
        )
        directory_fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            root_stat = os.fstat(directory_fd)
            require((root_stat.st_dev, root_stat.st_ino) == self.root_identity, "bundle directory changed")
            fd = os.open(relative, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
            with os.fdopen(fd, "rb") as handle:
                before = os.fstat(handle.fileno())
                require(
                    stat.S_ISREG(before.st_mode) and before.st_size <= self.limits["max_file_bytes"],
                    "regular bounded bundle file required",
                )
                raw = handle.read(self.limits["max_file_bytes"] + 1)
                after = os.fstat(handle.fileno())
            current = os.stat(relative, dir_fd=directory_fd, follow_symlinks=False)
        finally:
            os.close(directory_fd)

        def identity(info: os.stat_result) -> tuple[int, ...]:
            return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns

        require(
            identity(before) == identity(after) == identity(current) and len(raw) == before.st_size,
            "bundle file changed while reading",
        )
        self.total += len(raw)
        require(self.total <= self.limits["max_total_bytes"], "bundle total byte limit exceeded")
        return decode(raw)
