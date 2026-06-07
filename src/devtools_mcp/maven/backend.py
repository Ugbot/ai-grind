"""Maven backend registration."""

from __future__ import annotations

from typing import Any

from devtools_mcp.build.analysis import deps_df, modules_df, tests_df
from devtools_mcp.build.formatters import format_build_summary
from devtools_mcp.build.models import BuildResult
from devtools_mcp.maven.runner import check_maven, run_maven
from devtools_mcp.models import RunBase
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend


async def detect() -> list[InstalledTool]:
    """Maven via global mvn or a project mvnw wrapper (so wrapper projects work)."""
    info = await check_maven()
    version = info.get("version") or "wrapper"
    path = info.get("path") or "mvnw"
    # Optimistic: a project may carry mvnw even when mvn isn't on PATH; the runner
    # resolves and errors precisely if neither is found.
    return [InstalledTool(suite="maven", name=t, path=path, version=version, available=True)
            for t in ("build", "test", "deps", "sync")]


async def run(
    tool: str = "build",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    **kwargs: Any,
) -> tuple[str | None, RunBase | None, str]:
    """Run a Maven goal via the runner. `binary` = project directory."""
    return await run_maven(tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)


def format_summary(result: RunBase) -> str:
    if isinstance(result, BuildResult):
        return format_build_summary(result)
    return f"Unknown maven result: {type(result)}"


_DF_BUILDERS = {
    "build": modules_df,
    "test": tests_df,
    "deps": deps_df,
    "sync": deps_df,
    "_default": deps_df,
}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="maven",
            tools=["build", "test", "deps", "sync"],
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
        )
    )


_register()
