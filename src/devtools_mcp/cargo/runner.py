"""Cargo execution: build, check, test, dependency tree, fetch (sync), audit."""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import time

from devtools_mcp.build.exec import run_capture, tail, write_raw
from devtools_mcp.build.models import BuildResult
from devtools_mcp.cargo.parsers import (
    parse_cargo_audit,
    parse_cargo_build,
    parse_cargo_outdated,
    parse_cargo_test,
    parse_cargo_tree,
)
from devtools_mcp.models import create_run_base

_ARGV = {
    "build": lambda a, e: ["build", *a, *e],
    "check": lambda a, e: ["check", *a, *e],
    "test": lambda a, e: ["test", *a, *e],
    "deps": lambda a, e: ["tree", *a, *e],
    "sync": lambda a, e: ["fetch", *e],
    "audit": lambda a, e: ["audit", "--json", *e],
    "outdated": lambda a, e: ["outdated", "--format", "json", *e],
}
_INFORMATIONAL = {"deps", "audit", "outdated"}


def resolve_cargo() -> str | None:
    return shutil.which("cargo")


async def check_cargo() -> dict[str, str]:
    cargo = resolve_cargo()
    version = ""
    if cargo:
        try:
            proc = await asyncio.create_subprocess_exec(
                cargo, "--version", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
            )
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            version = out.decode("utf-8", "replace").strip()
        except (TimeoutError, OSError):
            version = ""
    return {"path": cargo or "", "version": version}


async def run_cargo(
    tool: str = "build",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    **kwargs: object,
) -> tuple[str | None, BuildResult | None, str]:
    """Run a Cargo command in a crate directory and normalize the output."""
    project = binary or os.getcwd()
    if not pathlib.Path(project).is_dir():
        return f"project dir not found: {project}", None, ""
    cargo = resolve_cargo()
    if not cargo:
        return "cargo not found. Install Rust via rustup (rustup.rs).", None, ""
    if tool not in _ARGV:
        return f"Unknown cargo tool: {tool} (build|check|test|deps|sync|audit|outdated)", None, ""

    argv = _ARGV[tool](args or [], extra_args or [])
    start = time.monotonic()
    rc, text = await run_capture([cargo, *argv], cwd=project, timeout=timeout)
    raw_path = write_raw("devtools-cargo-", text)

    deps = parse_cargo_tree(text) if tool == "deps" else []
    vulns = parse_cargo_audit(text) if tool == "audit" else []
    tests = []
    failures: list[str] = []
    if tool == "test":
        tests, parsed_ok = parse_cargo_test(text)
        success = parsed_ok and rc == 0  # a compile failure is rc != 0, not a passing test run
    elif tool in ("build", "check"):
        success, failures = parse_cargo_build(text)
        success = success and rc == 0
    elif tool in ("outdated", "audit"):
        # Separately-installed subcommands: a missing one exits non-zero with "no such
        # command" and must not read as a clean result.
        subcmd = "cargo-outdated" if tool == "outdated" else "cargo-audit"
        if tool == "outdated":
            deps = parse_cargo_outdated(text)
        if "no such command" in text:
            failures = [f"{subcmd} is not installed. Install it: cargo install {subcmd}"]
            success = False
        else:
            # Both exit non-zero when they find something, findings are not failure.
            found = bool(deps) if tool == "outdated" else bool(vulns)
            success = rc == 0 or found
    else:  # deps, sync
        success = tool in _INFORMATIONAL or rc == 0

    base = create_run_base(
        suite="cargo", tool=tool, binary=project, duration_seconds=time.monotonic() - start, exit_code=rc
    )
    result = BuildResult(
        **base.model_dump(),
        command="cargo " + " ".join(argv),
        success=success,
        dependencies=deps,
        tests=tests,
        vulnerabilities=vulns,
        failures=failures,
        raw_output=tail(text),
    )
    return None, result, raw_path
