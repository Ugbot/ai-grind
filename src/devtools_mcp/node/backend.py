"""Node.js backend registration."""

from __future__ import annotations

from typing import Any

from devtools_mcp.models import RunBase
from devtools_mcp.node.analysis import node_hotspots_df, node_stack_samples
from devtools_mcp.node.formatters import format_node_summary
from devtools_mcp.node.models import NodeResult
from devtools_mcp.node.runner import check_node, run_node
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend


async def detect() -> list[InstalledTool]:
    """Detect Node.js for V8 CPU/heap profiling."""
    info = await check_node()
    avail = info.get("installed") == "true"
    return [
        InstalledTool(suite="node", name=t, path=info["path"], version="node", available=avail)
        for t in ("cpu", "heap")
    ]


async def run(
    tool: str = "cpu",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    **kwargs: Any,
) -> tuple[str | None, RunBase | None, str]:
    """Run a Node profile via the runner."""
    return await run_node(tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)


def format_summary(result: RunBase) -> str:
    if isinstance(result, NodeResult):
        return format_node_summary(result)
    return f"Unknown node result: {type(result)}"


_DF_BUILDERS = {"cpu": node_hotspots_df, "heap": node_hotspots_df, "_default": node_hotspots_df}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="node",
            tools=["cpu", "heap"],
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
            stacks=node_stack_samples,
        )
    )


_register()
