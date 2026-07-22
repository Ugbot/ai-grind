"""pnpm execution: dependency tree (subdeps), install, audit, outdated, scripts.

Thin config over the shared JS-package-manager runner (build/jsrun).
"""

from __future__ import annotations

from devtools_mcp.build.jsdeps import parse_npm_audit, parse_npm_outdated, parse_pnpm_list
from devtools_mcp.build.jsrun import JsPackageManager, check_pm, run_pm
from devtools_mcp.build.models import BuildResult

_PNPM = JsPackageManager(
    suite="pnpm",
    version_prefix="pnpm ",
    argv={
        "build": lambda a, e: ["run", *(a or ["build"]), *e],
        "test": lambda a, e: ["test", *e],
        "deps": lambda a, e: ["list", "--depth", "Infinity", "--json", *e],
        "sync": lambda a, e: ["install", *e],
        "audit": lambda a, e: ["audit", "--json", *e],
        "outdated": lambda a, e: ["outdated", "--format", "json", *e],
    },
    not_found="pnpm not found. Install it: `npm i -g pnpm` or see pnpm.io.",
    dep_parser=parse_pnpm_list,
    outdated_parser=parse_npm_outdated,
    audit_parser=parse_npm_audit,
)


def resolve_pnpm() -> str | None:
    return _PNPM.resolve()


async def check_pnpm() -> dict[str, str]:
    return await check_pm(_PNPM)


async def run_pnpm(
    tool: str = "deps",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    **kwargs: object,
) -> tuple[str | None, BuildResult | None, str]:
    """Run a pnpm tool in a project directory and normalize the output."""
    return await run_pm(_PNPM, tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)
