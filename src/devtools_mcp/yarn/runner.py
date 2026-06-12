"""yarn (classic 1.x) execution: dependency tree, install, audit, scripts."""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import time

from devtools_mcp.build.exec import write_raw
from devtools_mcp.build.jsdeps import parse_package_scripts, parse_yarn_audit, parse_yarn_list
from devtools_mcp.build.jsrun import assemble, capture
from devtools_mcp.build.models import BuildResult

_ARGV = {
    "build": lambda a, e: ["run", *(a or ["build"]), *e],
    "test": lambda a, e: ["test", *e],
    "deps": lambda a, e: ["list", "--json", *e],
    "sync": lambda a, e: ["install", *e],
    "audit": lambda a, e: ["audit", "--json", *e],
}


def resolve_yarn() -> str | None:
    return shutil.which("yarn")


async def check_yarn() -> dict[str, str]:
    yarn = resolve_yarn()
    version = ""
    if yarn:
        try:
            proc = await asyncio.create_subprocess_exec(
                yarn, "--version", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
            )
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            version = "yarn " + out.decode("utf-8", "replace").strip()
        except (TimeoutError, OSError):
            version = ""
    return {"path": yarn or "", "version": version}


async def run_yarn(
    tool: str = "deps",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    **kwargs: object,
) -> tuple[str | None, BuildResult | None, str]:
    """Run a yarn tool in a project directory and normalize the output."""
    project = binary or os.getcwd()
    if not pathlib.Path(project).is_dir():
        return f"project dir not found: {project}", None, ""
    yarn = resolve_yarn()
    if not yarn:
        return "yarn not found. Install it: `npm i -g yarn`.", None, ""

    if tool == "tasks":
        scripts = parse_package_scripts(project)
        return None, assemble("yarn", "tasks", project, "yarn run", 0.0, 0, "", scripts=scripts), ""
    if tool not in _ARGV:
        return f"Unknown yarn tool: {tool} (build|test|deps|sync|audit|tasks)", None, ""

    argv = _ARGV[tool](args or [], extra_args or [])
    start = time.monotonic()
    rc, ptext, raw = await capture([yarn, *argv], project, timeout, tool)
    raw_path = write_raw("devtools-yarn-", raw)
    deps = parse_yarn_list(ptext) if tool == "deps" else []
    vulns = parse_yarn_audit(ptext) if tool == "audit" else []
    result = assemble(
        "yarn", tool, project, "yarn " + " ".join(argv), time.monotonic() - start, rc, raw, deps=deps, vulns=vulns
    )
    return None, result, raw_path
