"""ETW backend registration."""

from __future__ import annotations

from typing import Any

from devtools_mcp.etw.analysis import etw_hotspots_df, etw_stack_samples
from devtools_mcp.etw.formatters import format_etw_summary
from devtools_mcp.etw.models import EtwResult
from devtools_mcp.etw.runner import check_etw, run_etw
from devtools_mcp.models import RunBase
from devtools_mcp.registry import BackendSpec, InstalledTool, InstallSpec, InstallStep, register_backend

ETW_INSTALL = InstallSpec(
    platforms={
        "windows": [
            InstallStep(
                kind="download",
                argv=[
                    "https://github.com/microsoft/perfview/releases/latest/download/PerfView.exe",
                    "C:/code/PerfView.exe",
                ],
                description="PerfView single-exe download (to the path find_perfview probes)",
            ),
        ],
    },
    note="ETW collection needs an elevated (Administrator) shell at run time.",
    url="https://github.com/microsoft/perfview",
)


async def detect() -> list[InstalledTool]:
    """Detect PerfView for ETW profiling."""
    info = await check_etw()
    if info.get("installed") == "true":
        return [InstalledTool(suite="etw", name="cpu", path=info["path"], version=info["version"], available=True)]
    return [InstalledTool(suite="etw", name="cpu", path=info.get("path", "PerfView.exe"), version="", available=False)]


async def run(
    tool: str = "cpu",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    **kwargs: Any,
) -> tuple[str | None, RunBase | None, str]:
    """Run an ETW capture/decode via the runner."""
    return await run_etw(tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)


def format_summary(result: RunBase) -> str:
    if isinstance(result, EtwResult):
        return format_etw_summary(result)
    return f"Unknown etw result: {type(result)}"


_DF_BUILDERS = {"cpu": etw_hotspots_df, "_default": etw_hotspots_df}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="etw",
            tools=["cpu"],
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
            stacks=etw_stack_samples,
            install=ETW_INSTALL,
        )
    )


_register()
