"""LLDB backend registration for the tool registry.

The interactive PTY LLDB stack is gone, native debugging goes through the
unified debug suite's lldb-dap adapter. This backend survives for two jobs:
hydrating historical LldbSnapshot runs from disk (store/hydrate.py resolves
them by _module/_class) and serving their df_builders/summaries.
"""

from __future__ import annotations

from devtools_mcp.lldb.analysis import (
    lldb_breakpoints_df,
    lldb_frames_df,
    lldb_raw_lines_df,
    lldb_registers_df,
    lldb_threads_df,
    lldb_variables_df,
)
from devtools_mcp.lldb.formatters import format_snapshot_summary
from devtools_mcp.lldb.models import LldbSnapshot
from devtools_mcp.models import RunBase
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend


async def detect() -> list[InstalledTool]:
    """Probe lldb-dap, the implementation behind native debug sessions."""
    from devtools_mcp.debug.adapters.lldb_dap import detect as detect_lldb_dap

    tool = await detect_lldb_dap()
    return [
        InstalledTool(
            suite="lldb",
            name="lldb-dap",
            path=tool.path,
            version=tool.version,
            available=tool.available,
        )
    ]


async def run(**kwargs: object) -> tuple[str, None, str]:
    """LLDB doesn't use batch run. It's session-based. Use debug_start instead."""
    return "Native debugging is session-based. Use debug_start() (lldb-dap adapter).", None, ""


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
    "registers": lldb_registers_df,
    "memory": lldb_raw_lines_df,
    "expression": lldb_raw_lines_df,
    "disassemble": lldb_raw_lines_df,
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
