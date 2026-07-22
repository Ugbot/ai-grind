"""npm execution: dependency tree (subdeps), install, audit, outdated, scripts.

Thin config over the shared JS-package-manager runner (build/jsrun); npm/pnpm/
yarn differ only in the argv map and parsers, so the flow lives once there.
"""

from __future__ import annotations

from devtools_mcp.build.jsdeps import parse_npm_audit, parse_npm_ls, parse_npm_outdated
from devtools_mcp.build.jsrun import JsPackageManager, check_pm, run_pm
from devtools_mcp.build.models import BuildResult

_NPM = JsPackageManager(
    suite="npm",
    version_prefix="npm ",
    argv={
        "build": lambda a, e: ["run", *(a or ["build"]), *e],
        "test": lambda a, e: ["test", *e],
        "deps": lambda a, e: ["ls", "--all", "--json", *e],
        "sync": lambda a, e: ["install", *e],
        "audit": lambda a, e: ["audit", "--json", *e],
        "outdated": lambda a, e: ["outdated", "--json", *e],
    },
    not_found="npm not found. Install Node.js (nodejs.org).",
    dep_parser=parse_npm_ls,
    outdated_parser=parse_npm_outdated,
    audit_parser=parse_npm_audit,
)


def resolve_npm() -> str | None:
    return _NPM.resolve()


async def check_npm() -> dict[str, str]:
    return await check_pm(_NPM)


async def run_npm(
    tool: str = "deps",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    **kwargs: object,
) -> tuple[str | None, BuildResult | None, str]:
    """Run an npm tool in a project directory and normalize the output."""
    return await run_pm(_NPM, tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)
