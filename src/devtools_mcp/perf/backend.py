"""perf backend registration."""

from __future__ import annotations

from typing import Any

from devtools_mcp.models import RunBase
from devtools_mcp.perf.analysis import perf_annotation_df, perf_counters_df, perf_hotspots_df
from devtools_mcp.perf.formatters import format_perf_summary
from devtools_mcp.perf.models import PerfAnnotationResult, PerfRecordResult, PerfStatResult
from devtools_mcp.perf.runner import check_perf, run_perf
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend


async def detect() -> list[InstalledTool]:
    """Detect perf installation."""
    info = await check_perf()
    tools = ["stat", "record", "annotate"]
    if info.get("installed") == "true":
        return [
            InstalledTool(
                suite="perf",
                name=t,
                path=info["path"],
                version=info["version"],
                available=True,
            )
            for t in tools
        ]
    return [InstalledTool(suite="perf", name="perf", path=info.get("path", "perf"), version="", available=False)]


async def run(
    tool: str = "stat",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    **kwargs: Any,
) -> tuple[str | None, RunBase | None, str]:
    """Run perf via the runner."""
    return await run_perf(
        tool=tool,
        binary=binary,
        args=args,
        extra_args=extra_args,
        timeout=timeout,
        **kwargs,
    )


def format_summary(result: RunBase) -> str:
    if isinstance(result, (PerfStatResult, PerfRecordResult, PerfAnnotationResult)):
        return format_perf_summary(result)
    return f"Unknown perf result: {type(result)}"


_DF_BUILDERS = {
    "stat": perf_counters_df,
    "record": perf_hotspots_df,
    "annotate": perf_annotation_df,
    "_default": perf_counters_df,
}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="perf",
            tools=["stat", "record", "annotate"],
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
        )
    )


_register()
