"""Debug suite registration: one BackendSpec fronting all debug adapters.

devtools_check shows per-adapter availability; devtools_install(suite=
"debug", tool="<adapter>") shows that adapter's install story. Actual
debugging goes through the session tools (debug_start/debug/debug_inspect/
debug_stop), not devtools_run.
"""

from __future__ import annotations

from devtools_mcp.debug.adapters import list_adapters
from devtools_mcp.debug.analysis import DF_BUILDERS
from devtools_mcp.debug.formatters import format_stop_summary
from devtools_mcp.debug.models import DebugSnapshot
from devtools_mcp.models import RunBase, StackSample
from devtools_mcp.registry import BackendSpec, InstalledTool, register_backend


async def detect() -> list[InstalledTool]:
    """Fan out over every registered adapter's detect()."""
    tools: list[InstalledTool] = []
    for spec in list_adapters():
        try:
            tools.append(await spec.detect())
        except Exception:  # noqa: BLE001  # one broken adapter must not hide the rest
            tools.append(InstalledTool(suite="debug", name=spec.name, path="", version="", available=False))
    return tools


async def run(**kwargs: object) -> tuple[str, None, str]:
    """The debug suite is session-based, point at the session tools."""
    return "The debug suite is session-based. Use debug_start() to launch or attach.", None, ""


def format_summary(result: RunBase) -> str:
    if isinstance(result, DebugSnapshot):
        return format_stop_summary(result)
    return f"Unknown debug result type: {type(result)}"


def stacks(result: RunBase) -> list[StackSample]:
    """Flame-graph input: a session-summary run carries one stack per stop;
    a single stop snapshot yields its stopped thread's stack."""
    if not isinstance(result, DebugSnapshot):
        return []
    if result.session_stacks:
        return list(result.session_stacks)
    if result.threads and result.threads[0].frames:
        frames = [f.function or f.file or f"frame#{f.index}" for f in reversed(result.threads[0].frames)]
        return [StackSample(frames=frames, weight=1)]
    return []


def _register() -> None:
    adapters = list_adapters()
    register_backend(
        BackendSpec(
            suite="debug",
            tools=[spec.name for spec in adapters],
            detect=detect,
            run=run,
            df_builders=dict(DF_BUILDERS),
            format_summary=format_summary,
            stacks=stacks,
            tool_installs={spec.name: spec.install for spec in adapters if spec.install is not None},
            description="Unified cross-language debugger (DAP adapters + more): sessions, watches, snapshots, plans",
        )
    )


_register()
