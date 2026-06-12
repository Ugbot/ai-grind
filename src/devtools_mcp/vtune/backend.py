"""VTune backend registration."""

from __future__ import annotations

from typing import Any

from devtools_mcp.models import RunBase
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend
from devtools_mcp.vtune.analysis import vtune_functions_df, vtune_stack_samples
from devtools_mcp.vtune.formatters import format_vtune_summary
from devtools_mcp.vtune.models import VtuneResult
from devtools_mcp.vtune.runner import ANALYSES, check_vtune, run_vtune

_TOOLS = sorted(ANALYSES)


async def detect() -> list[InstalledTool]:
    """Detect the vtune CLI; all analysis verbs share one executable."""
    info = await check_vtune()
    available = info.get("installed") == "true"
    return [
        InstalledTool(
            suite="vtune",
            name=name,
            path=info.get("path", "vtune"),
            version=info.get("version", ""),
            available=available,
        )
        for name in _TOOLS
    ]


async def run(
    tool: str = "cpu",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 600,
    **kwargs: Any,
) -> tuple[str | None, RunBase | None, str]:
    """Run one VTune collection + report decode via the runner."""
    return await run_vtune(tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)


def format_summary(result: RunBase) -> str:
    if isinstance(result, VtuneResult):
        return format_vtune_summary(result)
    return f"Unknown vtune result: {type(result)}"


_DF_BUILDERS = {"_default": vtune_functions_df}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="vtune",
            tools=_TOOLS,
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
            stacks=vtune_stack_samples,
        )
    )


_register()
