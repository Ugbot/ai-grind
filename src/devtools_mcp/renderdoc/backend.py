"""RenderDoc backend registration."""

from __future__ import annotations

from typing import Any

from devtools_mcp.models import RunBase
from devtools_mcp.registry import (
    BackendSpec,
    InstalledTool,
    InstallSpec,
    InstallStep,
    register_backend,
)
from devtools_mcp.renderdoc.analysis import (
    rdoc_actions_df,
    rdoc_capture_df,
    rdoc_counters_df,
    rdoc_resources_df,
    rdoc_stack_samples,
    rdoc_thumb_df,
)
from devtools_mcp.renderdoc.formatters import format_renderdoc_summary
from devtools_mcp.renderdoc.runner import (
    REPLAY_TOOLS,
    TOOLS,
    check_renderdoc,
    run_renderdoc,
)

RENDERDOC_INSTALL = InstallSpec(
    platforms={
        "windows": [
            InstallStep(
                kind="winget",
                argv=[
                    "winget",
                    "install",
                    "--id",
                    "BaldurKarlsson.RenderDoc",
                    "-e",
                    "--accept-source-agreements",
                    "--accept-package-agreements",
                ],
                description="RenderDoc (renderdoccmd + qrenderdoc) via winget",
            ),
        ],
        "linux": [
            InstallStep(
                kind="apt",
                argv=["apt-get", "install", "-y", "renderdoc"],
                description="RenderDoc via apt (Debian/Ubuntu)",
                elevation=True,
            ),
        ],
    },
    note="Replay analysis (analyze/counters/resources) needs a GPU + interactive session.",
    url="https://renderdoc.org/builds",
)


async def detect() -> list[InstalledTool]:
    """Per-verb availability: capture/thumb need renderdoccmd, replay verbs qrenderdoc."""
    info = await check_renderdoc()
    installed = info.get("installed") == "true"
    has_cmd = bool(info.get("renderdoccmd"))
    has_qrd = bool(info.get("qrenderdoc"))
    tools: list[InstalledTool] = []
    for name in TOOLS:
        if name in REPLAY_TOOLS:
            available = installed and has_qrd
            path = info.get("qrenderdoc", "")
        elif name == "capture":
            available = installed and has_qrd  # targetcontrol default needs the bridge
            path = info.get("qrenderdoc", "") or info.get("renderdoccmd", "")
        else:  # thumb
            available = installed and has_cmd
            path = info.get("renderdoccmd", "")
        tools.append(
            InstalledTool(
                suite="renderdoc",
                name=name,
                path=path or info.get("path", "renderdoc"),
                version=info.get("version", ""),
                available=available,
            )
        )
    assert len(tools) == len(TOOLS), "detect must report every verb"
    return tools


async def run(
    tool: str = "analyze",
    binary: str = "",
    args: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 300,
    **kwargs: Any,
) -> tuple[str | None, RunBase | None, str]:
    """Run one renderdoc verb via the runner."""
    return await run_renderdoc(tool=tool, binary=binary, args=args, extra_args=extra_args, timeout=timeout, **kwargs)


def format_summary(result: RunBase) -> str:
    return format_renderdoc_summary(result)


_DF_BUILDERS = {
    "capture": rdoc_capture_df,
    "analyze": rdoc_actions_df,
    "counters": rdoc_counters_df,
    "resources": rdoc_resources_df,
    "thumb": rdoc_thumb_df,
    "_default": rdoc_actions_df,
}


def _register() -> None:
    register_backend(
        BackendSpec(
            suite="renderdoc",
            tools=list(TOOLS),
            detect=detect,
            run=run,
            df_builders=_DF_BUILDERS,
            format_summary=format_summary,
            stacks=rdoc_stack_samples,
            install=RENDERDOC_INSTALL,
            description="GPU frame capture + replay analysis (RenderDoc)",
        )
    )


_register()
