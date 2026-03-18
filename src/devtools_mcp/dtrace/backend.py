"""DTrace backend registration."""

from __future__ import annotations

from typing import Any

from devtools_mcp.dtrace.analysis import (
    dtrace_aggregation_df,
    dtrace_stacks_df,
)
from devtools_mcp.dtrace.formatters import format_dtrace_summary
from devtools_mcp.dtrace.models import DTraceResult
from devtools_mcp.dtrace.runner import check_dtrace, run_dtrace
from devtools_mcp.models import RunBase
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend


async def detect() -> list[InstalledTool]:
    """Detect DTrace installation."""
    info = await check_dtrace()
    tools = ["trace", "syscall", "profile"]
    if info.get("installed") == "true":
        return [
            InstalledTool(
                suite="dtrace",
                name=t,
                path=info["path"],
                version=info["version"],
                available=True,
            )
            for t in tools
        ]
    return [InstalledTool(suite="dtrace", name="dtrace", path=info.get("path", "dtrace"), version="", available=False)]


async def run(
    tool: str = "trace",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 30,
    **kwargs: Any,
) -> tuple[str | None, DTraceResult | None, str]:
    """Run DTrace via the runner."""
    return await run_dtrace(
        tool=tool,
        binary=binary,
        args=args,
        extra_args=extra_args,
        timeout=timeout,
        **kwargs,
    )


def format_summary(result: RunBase) -> str:
    """Format a DTrace result summary."""
    if isinstance(result, DTraceResult):
        return format_dtrace_summary(result)
    return f"Unknown DTrace result type: {type(result)}"


_DF_BUILDERS = {
    "trace": dtrace_aggregation_df,
    "syscall": dtrace_aggregation_df,
    "profile": dtrace_stacks_df,
    "_default": dtrace_aggregation_df,
}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="dtrace",
            tools=["trace", "syscall", "profile"],
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
        )
    )


_register()
