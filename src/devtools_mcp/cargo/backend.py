"""Cargo backend registration."""

from __future__ import annotations

from typing import Any

from devtools_mcp.build.analysis import deps_df, tests_df, vulns_df
from devtools_mcp.build.formatters import format_build_summary
from devtools_mcp.build.models import BuildResult
from devtools_mcp.cargo.runner import check_cargo, resolve_cargo, run_cargo
from devtools_mcp.models import RunBase
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend

_TOOLS = ["build", "check", "test", "deps", "sync", "audit", "outdated"]


async def detect() -> list[InstalledTool]:
    info = await check_cargo()
    available = bool(resolve_cargo())
    version = info.get("version") or "cargo"
    return [
        InstalledTool(suite="cargo", name=t, path=info.get("path") or "cargo", version=version, available=available)
        for t in _TOOLS
    ]


async def run(
    tool: str = "build",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    **kwargs: Any,
) -> tuple[str | None, RunBase | None, str]:
    """Run a Cargo command. `binary` = crate directory."""
    return await run_cargo(tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)


def format_summary(result: RunBase) -> str:
    if isinstance(result, BuildResult):
        return format_build_summary(result)
    return f"Unknown cargo result: {type(result)}"


_DF_BUILDERS = {
    "deps": deps_df,
    "sync": deps_df,
    "build": deps_df,
    "check": deps_df,
    "test": tests_df,
    "audit": vulns_df,
    "outdated": deps_df,
    "_default": deps_df,
}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="cargo",
            tools=_TOOLS,
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
        )
    )


_register()
