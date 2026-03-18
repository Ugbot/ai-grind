"""LLDB backend registration for the tool registry."""

from __future__ import annotations

from devtools_mcp.lldb.analysis import lldb_breakpoints_df, lldb_frames_df, lldb_threads_df, lldb_variables_df
from devtools_mcp.lldb.formatters import format_snapshot_summary
from devtools_mcp.lldb.models import LldbSnapshot
from devtools_mcp.lldb.session import check_lldb
from devtools_mcp.models import RunBase
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend


async def detect() -> list[InstalledTool]:
    """Detect LLDB installation."""
    info = await check_lldb()
    return [
        InstalledTool(
            suite="lldb",
            name="lldb",
            path=info.get("path", "lldb"),
            version=info.get("version", ""),
            available=info.get("installed") == "true",
        )
    ]


async def run(**kwargs: object) -> tuple[str, None, str]:
    """LLDB doesn't use batch run — it's session-based. Use debug_start instead."""
    return "LLDB is session-based. Use debug_start() to create a session.", None, ""


def format_summary(result: RunBase) -> str:
    """Format an LLDB snapshot summary."""
    if isinstance(result, LldbSnapshot):
        return format_snapshot_summary(result)
    return f"Unknown LLDB result type: {type(result)}"


_DF_BUILDERS = {
    "backtrace": lldb_frames_df,
    "threads": lldb_threads_df,
    "variables": lldb_variables_df,
    "breakpoints": lldb_breakpoints_df,
    "_default": lldb_frames_df,
}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="lldb",
            tools=["lldb"],
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
        )
    )


_register()
