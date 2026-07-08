"""Capture git and environment provenance for profiling runs."""

from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path


def capture_provenance(binary: str) -> dict[str, object]:
    """Gather cwd, hostname, and optional git context from a binary path."""
    cwd = _resolve_cwd(binary)
    info: dict[str, object] = {
        "cwd": cwd,
        "hostname": socket.gethostname(),
        "git_commit": "",
        "git_branch": "",
        "git_dirty": False,
    }
    git = _git_info(cwd)
    info.update(git)
    return info


def _resolve_cwd(binary: str) -> str:
    path = Path(binary)
    if path.is_file():
        return str(path.parent.resolve())
    if path.is_dir():
        return str(path.resolve())
    parent = path.parent
    if parent != Path(".") and parent.exists():
        return str(parent.resolve())
    return os.getcwd()


def _git_info(cwd: str) -> dict[str, object]:
    if not Path(cwd).is_dir():
        return {"git_commit": "", "git_branch": "", "git_dirty": False}
    try:
        commit = _git_run(cwd, "rev-parse", "HEAD")
        branch = _git_run(cwd, "rev-parse", "--abbrev-ref", "HEAD")
        dirty = _git_run(cwd, "status", "--porcelain") != ""
        return {
            "git_commit": commit[:12] if commit else "",
            "git_branch": branch,
            "git_dirty": dirty,
        }
    except (OSError, subprocess.SubprocessError):
        return {"git_commit": "", "git_branch": "", "git_dirty": False}


def _git_run(cwd: str, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip()
