"""CDB backend registration."""

from __future__ import annotations

from typing import Any

from devtools_mcp.cdb.analysis import cdb_frames_df, cdb_stack_samples
from devtools_mcp.cdb.formatters import format_cdb_summary
from devtools_mcp.cdb.models import CdbSnapshot
from devtools_mcp.cdb.runner import check_cdb, run_cdb
from devtools_mcp.models import RunBase
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend


async def detect() -> list[InstalledTool]:
    """Detect cdb.exe for Windows debugging."""
    info = await check_cdb()
    tools = ["stacks", "analyze", "inspect"]
    if info.get("installed") == "true":
        return [InstalledTool(suite="cdb", name=t, path=info["path"], version=info["version"], available=True)
                for t in tools]
    return [InstalledTool(suite="cdb", name="cdb", path=info.get("path", "cdb.exe"), version="", available=False)]


async def run(
    tool: str = "stacks",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 120,
    **kwargs: Any,
) -> tuple[str | None, RunBase | None, str]:
    """Run a batch CDB script via the runner."""
    return await run_cdb(tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)


def format_summary(result: RunBase) -> str:
    if isinstance(result, CdbSnapshot):
        return format_cdb_summary(result)
    return f"Unknown cdb result: {type(result)}"


_DF_BUILDERS = {
    "stacks": cdb_frames_df,
    "analyze": cdb_frames_df,
    "inspect": cdb_frames_df,
    "_default": cdb_frames_df,
}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="cdb",
            tools=["stacks", "analyze", "inspect"],
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
            stacks=cdb_stack_samples,
        )
    )


_register()
