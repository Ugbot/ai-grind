"""npm execution: dependency tree (subdeps), install, audit, outdated, scripts."""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import time

from devtools_mcp.build.exec import write_raw
from devtools_mcp.build.jsdeps import (
    parse_npm_audit,
    parse_npm_ls,
    parse_npm_outdated,
    parse_package_scripts,
)
from devtools_mcp.build.jsrun import assemble, capture
from devtools_mcp.build.models import BuildResult

_ARGV = {
    "build": lambda a, e: ["run", *(a or ["build"]), *e],
    "test": lambda a, e: ["test", *e],
    "deps": lambda a, e: ["ls", "--all", "--json", *e],
    "sync": lambda a, e: ["install", *e],
    "audit": lambda a, e: ["audit", "--json", *e],
    "outdated": lambda a, e: ["outdated", "--json", *e],
}


def resolve_npm() -> str | None:
    return shutil.which("npm")


async def check_npm() -> dict[str, str]:
    npm = resolve_npm()
    version = ""
    if npm:
        try:
            proc = await asyncio.create_subprocess_exec(
                npm, "--version", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            version = "npm " + out.decode("utf-8", "replace").strip()
        except (TimeoutError, OSError):
            version = ""
    return {"path": npm or "", "version": version}


async def run_npm(
    tool: str = "deps", binary: str = "", args: list[str] | None = None,
    extra_args: list[str] | None = None, timeout: int = 1800, **kwargs: object,
) -> tuple[str | None, BuildResult | None, str]:
    """Run an npm tool in a project directory and normalize the output."""
    project = binary or os.getcwd()
    if not pathlib.Path(project).is_dir():
        return f"project dir not found: {project}", None, ""
    npm = resolve_npm()
    if not npm:
        return "npm not found. Install Node.js (nodejs.org).", None, ""

    if tool == "tasks":
        scripts = parse_package_scripts(project)
        return None, assemble("npm", "tasks", project, "npm run", 0.0, 0, "", scripts=scripts), ""
    if tool not in _ARGV:
        return f"Unknown npm tool: {tool} (build|test|deps|sync|audit|outdated|tasks)", None, ""

    argv = _ARGV[tool](args or [], extra_args or [])
    start = time.monotonic()
    rc, ptext, raw = await capture([npm, *argv], project, timeout, tool)
    raw_path = write_raw("devtools-npm-", raw)
    deps = parse_npm_ls(ptext) if tool == "deps" else (parse_npm_outdated(ptext) if tool == "outdated" else [])
    vulns = parse_npm_audit(ptext) if tool == "audit" else []
    result = assemble("npm", tool, project, "npm " + " ".join(argv),
                      time.monotonic() - start, rc, raw, deps=deps, vulns=vulns)
    return None, result, raw_path
