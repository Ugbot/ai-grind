"""yarn (classic 1.x) execution: dependency tree, install, audit, outdated, scripts.

Thin config over the shared JS-package-manager runner (build/jsrun).
"""

from __future__ import annotations

from devtools_mcp.build.jsdeps import parse_yarn_audit, parse_yarn_list, parse_yarn_outdated
from devtools_mcp.build.jsrun import JsPackageManager, check_pm, run_pm
from devtools_mcp.build.models import BuildResult

_YARN = JsPackageManager(
    suite="yarn",
    version_prefix="yarn ",
    argv={
        "build": lambda a, e: ["run", *(a or ["build"]), *e],
        "test": lambda a, e: ["test", *e],
        "deps": lambda a, e: ["list", "--json", *e],
        "sync": lambda a, e: ["install", *e],
        "audit": lambda a, e: ["audit", "--json", *e],
        "outdated": lambda a, e: ["outdated", "--json", *e],  # classic only; berry has no outdated
    },
    not_found="yarn not found. Install it: `npm i -g yarn`.",
    dep_parser=parse_yarn_list,
    outdated_parser=parse_yarn_outdated,
    audit_parser=parse_yarn_audit,
)


def resolve_yarn() -> str | None:
    return _YARN.resolve()


async def check_yarn() -> dict[str, str]:
    return await check_pm(_YARN)


async def run_yarn(
    tool: str = "deps",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    **kwargs: object,
) -> tuple[str | None, BuildResult | None, str]:
    """Run a yarn tool in a project directory and normalize the output."""
    return await run_pm(_YARN, tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)
