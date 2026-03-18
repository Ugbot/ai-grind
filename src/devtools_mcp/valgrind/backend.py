"""Valgrind backend registration for the tool registry."""

from __future__ import annotations

from typing import Any

from devtools_mcp.models import RunBase, create_run_base
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend
from devtools_mcp.valgrind import analysis
from devtools_mcp.valgrind.formatters import (
    format_cachegrind_summary,
    format_callgrind_summary,
    format_massif_summary,
    format_memcheck_summary,
    format_threadcheck_summary,
)
from devtools_mcp.valgrind.models import (
    CachegrindResult,
    CallgrindResult,
    MassifResult,
    MemcheckResult,
    ThreadCheckResult,
)
from devtools_mcp.valgrind.parsers import (
    parse_cachegrind,
    parse_callgrind,
    parse_massif,
    parse_memcheck_xml,
    parse_threadcheck_xml,
)
from devtools_mcp.valgrind.runner import check_valgrind, run_valgrind


async def detect() -> list[InstalledTool]:
    """Detect valgrind installation."""
    info = await check_valgrind()
    if info.get("installed") == "true":
        tools = ["memcheck", "helgrind", "drd", "callgrind", "cachegrind", "massif"]
        return [
            InstalledTool(
                suite="valgrind",
                name=t,
                path=info["path"],
                version=info["version"],
                available=True,
            )
            for t in tools
        ]
    return [
        InstalledTool(
            suite="valgrind",
            name="valgrind",
            path=info.get("path", "valgrind"),
            version="",
            available=False,
        )
    ]


async def run(
    tool: str,
    binary: str,
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    **kwargs: Any,
) -> tuple[str | None, RunBase | None, str]:
    """Run a valgrind tool. Returns (error_msg, parsed_result, raw_output_path)."""
    result = await run_valgrind(
        tool=tool,
        binary=binary,
        binary_args=args,
        valgrind_args=extra_args,
        timeout=timeout,
    )

    if result.exit_code == -1:
        return f"{tool.capitalize()} failed: {result.stderr}", None, ""

    run_base = create_run_base(
        suite="valgrind",
        tool=tool,
        binary=binary,
        args=args,
        duration_seconds=result.duration_seconds,
        exit_code=result.exit_code,
    )

    if tool == "memcheck":
        parsed = parse_memcheck_xml(result.output_path, run_base)
    elif tool in ("helgrind", "drd"):
        parsed = parse_threadcheck_xml(result.output_path, run_base, tool=tool)
    elif tool == "callgrind":
        parsed = parse_callgrind(result.output_path, run_base)
    elif tool == "cachegrind":
        parsed = parse_cachegrind(result.output_path, run_base)
    elif tool == "massif":
        parsed = parse_massif(result.output_path, run_base)
    else:
        return f"Unknown valgrind tool: {tool}", None, ""

    return None, parsed, result.output_path


def format_summary(result: RunBase) -> str:
    """Format a valgrind result summary."""
    if isinstance(result, MemcheckResult):
        return format_memcheck_summary(result)
    if isinstance(result, ThreadCheckResult):
        return format_threadcheck_summary(result)
    if isinstance(result, CallgrindResult):
        return format_callgrind_summary(result)
    if isinstance(result, CachegrindResult):
        return format_cachegrind_summary(result)
    if isinstance(result, MassifResult):
        return format_massif_summary(result)
    return f"Unknown valgrind result type: {type(result)}"


# DataFrame builders keyed by tool name
_DF_BUILDERS = {
    "memcheck": analysis.memcheck_errors_df,
    "helgrind": analysis.threadcheck_errors_df,
    "drd": analysis.threadcheck_errors_df,
    "callgrind": analysis.callgrind_df,
    "cachegrind": analysis.cachegrind_df,
    "massif": analysis.massif_timeline_df,
    "_default": analysis.memcheck_errors_df,
}


def _register() -> None:
    """Register the valgrind backend with the tool registry."""
    register_backend(
        BackendSpec(
            suite="valgrind",
            tools=["memcheck", "helgrind", "drd", "callgrind", "cachegrind", "massif"],
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
        )
    )


# Auto-register on import
_register()
