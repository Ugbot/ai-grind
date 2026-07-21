"""pnpm execution: dependency tree (subdeps), install, audit, outdated, scripts."""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import time

from devtools_mcp.build.exec import write_raw
from devtools_mcp.build.jsdeps import (
    parse_npm_audit,
    parse_npm_outdated,
    parse_package_scripts,
    parse_pnpm_list,
)
from devtools_mcp.build.jsrun import assemble, capture
from devtools_mcp.build.models import BuildResult

_ARGV = {
    "build": lambda a, e: ["run", *(a or ["build"]), *e],
    "test": lambda a, e: ["test", *e],
    "deps": lambda a, e: ["list", "--depth", "Infinity", "--json", *e],
    "sync": lambda a, e: ["install", *e],
    "audit": lambda a, e: ["audit", "--json", *e],
    "outdated": lambda a, e: ["outdated", *e],
}


def resolve_pnpm() -> str | None:
    return shutil.which("pnpm")


async def check_pnpm() -> dict[str, str]:
    pnpm = resolve_pnpm()
    version = ""
    if pnpm:
        try:
            proc = await asyncio.create_subprocess_exec(
                pnpm, "--version", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
            )
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            version = "pnpm " + out.decode("utf-8", "replace").strip()
        except (TimeoutError, OSError):
            version = ""
    return {"path": pnpm or "", "version": version}


async def run_pnpm(
    tool: str = "deps",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    **kwargs: object,
) -> tuple[str | None, BuildResult | None, str]:
    """Run a pnpm tool in a project directory and normalize the output."""
    project = binary or os.getcwd()
    if not pathlib.Path(project).is_dir():
        return f"project dir not found: {project}", None, ""
    pnpm = resolve_pnpm()
    if not pnpm:
        return "pnpm not found. Install it: `npm i -g pnpm` or see pnpm.io.", None, ""

    if tool == "tasks":
        scripts = parse_package_scripts(project)
        return None, assemble("pnpm", "tasks", project, "pnpm run", 0.0, 0, "", scripts=scripts), ""
    if tool not in _ARGV:
        return f"Unknown pnpm tool: {tool} (build|test|deps|sync|audit|outdated|tasks)", None, ""

    argv = _ARGV[tool](args or [], extra_args or [])
    start = time.monotonic()
    rc, ptext, raw = await capture([pnpm, *argv], project, timeout, tool)
    raw_path = write_raw("devtools-pnpm-", raw)
    deps = parse_pnpm_list(ptext) if tool == "deps" else (parse_npm_outdated(ptext) if tool == "outdated" else [])
    vulns = parse_npm_audit(ptext) if tool == "audit" else []
    success = (rc == 0 or bool(deps) or bool(vulns)) if tool in ("audit", "outdated") else None
    result = assemble(
        "pnpm",
        tool,
        project,
        "pnpm " + " ".join(argv),
        time.monotonic() - start,
        rc,
        raw,
        deps=deps,
        vulns=vulns,
        success=success,
    )
    return None, result, raw_path
