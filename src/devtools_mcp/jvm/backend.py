"""JVM backend registration."""

from __future__ import annotations

from typing import Any

from devtools_mcp.jvm.analysis import jvm_heap_df, jvm_hotspots_df, jvm_stack_samples, jvm_threads_df
from devtools_mcp.jvm.formatters import format_jvm_summary
from devtools_mcp.jvm.models import JvmResult
from devtools_mcp.jvm.runner import check_jvm, run_jvm
from devtools_mcp.models import RunBase
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend


async def detect() -> list[InstalledTool]:
    """Detect JVM tooling — jcmd drives cpu(jfr)/threads/heap; alloc uses async-profiler."""
    info = await check_jvm()
    jdk = bool(info.get("jcmd"))
    tools = {
        "cpu": jdk and bool(info.get("jfr")),  # JFR CPU profile
        "threads": jdk or bool(info.get("jstack")),
        "heap": jdk or bool(info.get("jmap")),
        "alloc": bool(info.get("asprof")),  # async-profiler allocation profile
    }
    path_for = {
        "cpu": info.get("jfr") or "jfr",
        "threads": info.get("jcmd") or "jstack",
        "heap": info.get("jcmd") or "jmap",
        "alloc": info.get("asprof") or "asprof",
    }
    return [
        InstalledTool(
            suite="jvm",
            name=t,
            path=path_for[t] or t,
            version="JDK" if t != "alloc" else "async-profiler",
            available=ok,
        )
        for t, ok in tools.items()
    ]


async def run(
    tool: str = "cpu",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    **kwargs: Any,
) -> tuple[str | None, RunBase | None, str]:
    """Run a JVM tool via the runner."""
    return await run_jvm(tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)


def format_summary(result: RunBase) -> str:
    if isinstance(result, JvmResult):
        return format_jvm_summary(result)
    return f"Unknown jvm result: {type(result)}"


_DF_BUILDERS = {
    "cpu": jvm_hotspots_df,
    "alloc": jvm_hotspots_df,
    "threads": jvm_threads_df,
    "heap": jvm_heap_df,
    "_default": jvm_hotspots_df,
}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="jvm",
            tools=["cpu", "alloc", "threads", "heap"],
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
            stacks=jvm_stack_samples,
        )
    )


_register()
