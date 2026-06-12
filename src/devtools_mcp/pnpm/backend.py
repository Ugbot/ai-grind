"""pnpm backend registration."""

from __future__ import annotations

from typing import Any

from devtools_mcp.build.analysis import available_tasks_df, deps_df, vulns_df
from devtools_mcp.build.formatters import format_build_summary
from devtools_mcp.build.models import BuildResult
from devtools_mcp.models import RunBase
from devtools_mcp.pnpm.runner import check_pnpm, resolve_pnpm, run_pnpm
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend

_TOOLS = ["build", "test", "deps", "sync", "audit", "outdated", "tasks"]


async def detect() -> list[InstalledTool]:
    info = await check_pnpm()
    available = bool(resolve_pnpm())
    version = info.get("version") or "pnpm"
    return [
        InstalledTool(suite="pnpm", name=t, path=info.get("path") or "pnpm", version=version, available=available)
        for t in _TOOLS
    ]


async def run(
    tool: str = "deps",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    **kwargs: Any,
) -> tuple[str | None, RunBase | None, str]:
    """Run a pnpm tool. `binary` = project directory."""
    return await run_pnpm(tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)


def format_summary(result: RunBase) -> str:
    if isinstance(result, BuildResult):
        return format_build_summary(result)
    return f"Unknown pnpm result: {type(result)}"


_DF_BUILDERS = {
    "deps": deps_df,
    "sync": deps_df,
    "outdated": deps_df,
    "build": deps_df,
    "test": deps_df,
    "audit": vulns_df,
    "tasks": available_tasks_df,
    "_default": deps_df,
}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="pnpm",
            tools=_TOOLS,
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
        )
    )


_register()
